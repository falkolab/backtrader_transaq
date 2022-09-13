import collections
import inspect
import logging
import math
import threading
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional, Literal, Union, List, Dict, Tuple, Callable

import itertools
import pytz
import time
from tzlocal.windows_tz import win_tz

from backtrader import TimeFrame, Position
from backtrader.metabase import MetaParams
from backtrader.utils.py3 import with_metaclass, queue
from backtrader_transaq.connection import Connection
from transaqpy.commands import Ticker, SubscriptionEntity
from transaqpy.structures import ClientAccount, ServerStatus, CandleKindPacket, CandleKind, HistoryCandlePacket, \
    HistoryCandleStatus, SecurityPacket, TradePacket, Trade, TimeDiffResult, HistoryCandle, Union as TransaqUnion, \
    MarketPacket, MultiPortfolio, PositionPacket, SecurityPosition, ClientOrderPacket, Order, StopOrder, \
    Security, Market, TextMessagePacket

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class HandlerFuncProps:
    message_type: Optional[str]


def transaq_handler(message_type=None):
    def decorator(func):
        func._transaq_handler_props = HandlerFuncProps(message_type)
        return func

    return decorator


@dataclass(frozen=True)
class TickerQueue:
    ticker: Ticker
    queue: queue.Queue


class MetaSingleton(MetaParams):
    """Metaclass to make a metaclassed class a singleton"""

    def __init__(cls, name, bases, dct):
        super(MetaSingleton, cls).__init__(name, bases, dct)
        cls._singleton = None

    def __call__(cls, *args, **kwargs):
        if cls._singleton is None:
            cls._singleton = (
                super(MetaSingleton, cls).__call__(*args, **kwargs))

        return cls._singleton


CandlePeriod = tuple[TimeFrame, int]


class TransaqStore(with_metaclass(MetaSingleton, object)):
    BrokerCls = None  # broker class will autoregister
    DataCls = None  # data class will auto register
    params = (
        ('transaq_connector', None),
        ('login', None),
        ('password', None),
        ('host', '127.0.0.1'),
        ('port', 50051),
        ('reconnect', 3),  # -1 forever, 0 No, > 0 number of retries
        ('reconnect_timeout', 3.0),  # timeout between reconnections
        ('debug_', False),
        ('notifyall', False),
        ('client_id', None),
        ('base_currency', ''),
        ('market', 'ММВБ'),
        ('union', None),
        ('push_pos_equity', 15),
        ('time_diff_adjustment', 0)
    )

    _recovering_from_time_utc = None
    _initial_data_loading: bool = True
    _candle_kinds: List[CandleKind] = None
    _server_timezone = None
    _timediff = None
    _unions: list = None
    _markets: Dict[int, str] = None
    _client_id: Optional[str] = None
    _accounts: dict = None
    _portfolio: Optional[MultiPortfolio] = None
    _account_updates_timer = None
    _history_buffer = None
    _history_params: dict = None
    _securities: Dict[str, Security] = None
    _securities_bysecid: Dict[str, Security] = None
    _current_market: Market = None
    _on_connect_callback: Callable = None

    def __init__(self, **kwargs):
        super().__init__()
        self.broker = None  # broker instance
        self._env = None  # reference to cerebro for general notifications
        self.datas = list()  # datas that have registered over start
        self.dont_reconnect = False  # for non-recoverable connect errors
        self.conn = Connection.create(self.p.transaq_connector,
                                      self.p.login,
                                      self.p.password,
                                      host=self.p.host,
                                      port=self.p.port
                                      )
        if self.p.debug_ or self.p.notifyall:
            self.conn.register_all(self.watcher)

        self._unions = []
        self._markets = {}
        self._candle_kinds = []
        self._securities = {}
        self._securities_bysecid = {}
        self._positions = collections.defaultdict(Position)  # actual positions

        self._lock_account_update = threading.Lock()  # sync account updates
        self._lock_queue = threading.Lock()  # sync access to _tickerId/Queues
        self._lock_connect = threading.Lock()
        self._lock_positions = threading.Lock()
        self._lock_server_status = threading.Lock()
        self._lock_portfolio_update = threading.Lock()
        self._lock_orders = threading.Lock()
        self._lock_securities = threading.Lock()
        self._lock_time_offset = threading.Lock()
        self._time_offset = timedelta()  # to control time difference with server

        self._ticker_id = itertools.count(1)  # unique tickerIds

        self.notifs = queue.Queue()  # store notifications for cerebro
        self._accounts = dict()
        self._client_id = self.p.client_id

        # Structures to hold datas requests
        self._queues = collections.OrderedDict()  # key: tickerId -> queues
        self._tickers = collections.OrderedDict()  # key: queue -> tickerId
        self._history_params = dict()

        # Account list received
        self._event_managed_accounts = threading.Event()
        self._event_accdownload = threading.Event()

        self._event_portfolio = threading.Event()

        # Register decorated methods with the conn
        self._register_handlers()

    @property
    def server_timezone(self):
        return self._server_timezone

    _PERIODS_MAP = {
        (TimeFrame.Minutes, 1): 60,
        (TimeFrame.Minutes, 5): 300,
        (TimeFrame.Minutes, 15): 900,
        (TimeFrame.Minutes, 60): 3600,
        (TimeFrame.Days, 1): 86400,
        (TimeFrame.Weeks, 1): 604800,
    }

    _PERIODS_SECS = {
        TimeFrame.Minutes: 60,
        TimeFrame.Days: 86400,
        TimeFrame.Weeks: 604800
    }

    def get_most_suitable_period(self, timeframe: TimeFrame, compression: int) -> Optional[CandlePeriod]:
        if self._PERIODS_MAP.get((timeframe, compression), None) is not None:
            return timeframe, compression

        tfsecs = self._PERIODS_SECS.get(timeframe)
        if not tfsecs:
            return None
        insecs = tfsecs * compression
        pf = list(filter(lambda p: p[1] < insecs, self._PERIODS_MAP.items()))
        if len(pf) > 0:
            return pf[-1][0]
        return None

    def get_candle_kind(self, timeframe: TimeFrame, compression: int, fallback=False) -> Optional[CandleKind]:
        # candle_kinds:
        # 1: 1 минута - 60 sec
        # 2: 5 минут - 300 sec
        # 3: 15 минут - 900 sec
        # 4: 1 час - 3600 sec
        # 5: 1 сутки - 86400 sec
        # 6: 1 неделя - 604800 sec

        second_to_kind = {ck.period: ck for ck in self._candle_kinds}

        if timeframe < TimeFrame.Minutes:
            return None

        if timeframe > TimeFrame.Weeks:
            if fallback:
                return second_to_kind.get((TimeFrame.Weeks, 1), None)
            else:
                return None

        candlesec = self._PERIODS_SECS[timeframe] * compression
        kind = second_to_kind.get(candlesec, None)
        if kind is not None:
            return kind

        if fallback:
            secf = list(filter(lambda s: min(s, candlesec), self._PERIODS_MAP.values()))
            if len(secf):
                return second_to_kind.get(secf[-1], None)
        return None

    def _register_handlers(self):
        for method, message_type in self._get_subscription_registrations():
            self.conn.register(method, message_type)

    def _unregister_handlers(self):
        for method, message in self._get_subscription_registrations():
            self.conn.unregister(method, message)

    def _get_subscription_registrations(self):
        methods = inspect.getmembers(self, inspect.ismethod)
        registrations = []
        for name, method in methods:
            handler_opts: Union[HandlerFuncProps, Literal[False]] = getattr(method, '_transaq_handler_props', False)
            if not handler_opts:
                continue
            registrations.append((method, handler_opts.message_type or name,))
        return registrations

    @classmethod
    def getdata(cls, *args, **kwargs):
        """Returns ``DataCls`` with args, kwargs"""
        return cls.DataCls(*args, **kwargs)

    @classmethod
    def getbroker(cls, *args, **kwargs):
        """Returns broker with *args, **kwargs from registered ``BrokerCls``"""
        return cls.BrokerCls(*args, **kwargs)

    @property
    def is_online(self):
        try:
            return self.conn.is_online
        except AttributeError:
            # It looks like the Client not yet instantiated
            return False

    @property
    def is_connected(self):
        try:
            return self.conn.is_connected
        except AttributeError:
            # It looks like the Client not yet instantiated
            return False

    @property
    def is_recovering(self):
        try:
            return self.conn.is_recovering
        except AttributeError:
            # It looks like the Client not yet instantiated
            return False

    def watcher(self, message):
        # will be registered to see all messages if debug is requested
        if self.p.debug_:
            logger.debug('Получено сообщение: %s', message.__repr__())
        if self.p.notifyall:
            fields = dict(message.get_fields())
            self.notifs.put((message, tuple(fields.values()), dict(fields.items())))

    def start(self, data=None, broker=None):
        self.reconnect(from_start=True)  # reconnect should be an invariant

        # Datas require some processing to kickstart data reception
        if data is not None:
            self._env = data._env
            # For datas simulate a queue with None to kickstart co
            self.datas.append(data)

            # if connection fails, get a fake registration that will force the
            # datas to try to reconnect or else bail out
            return self.make_initial_ticker_queue()

        elif broker is not None:
            self.broker = broker

    def stop(self):
        try:
            self.conn.disconnect()  # disconnect should be an invariant
        except AttributeError:
            pass  # conn may have never been connected and lack "disconnect"
        self._unregister_handlers()

        # Unblock any calls set on these events
        self._event_managed_accounts.set()
        self._event_accdownload.set()
        self._event_portfolio.set()

    def _get_connection_params(self):
        return {
            'push_pos_equity': self.p.push_pos_equity,
        }

    def reconnect(self, from_start=False, resubscribe_datas=False):
        """
        # This method must be an invariant in that it can be called several
        # times from the same source and must be consistent.
        :param from_start:
        :param resubscribe_datas:
        :return:
        """
        with self._lock_connect:
            if self.is_connected:
                if resubscribe_datas:
                    self.start_datas()
                return True

            if self.dont_reconnect:
                return self.is_connected

            is_connect_command_success = False
            retries = self.p.reconnect
            retries = retries + 1 if retries >= 0 else retries
            last_time = None

            def interval():
                sec = (datetime.now() - last_time).total_seconds()
                return max(0.0, sec)

            logging.info('Connecting...')
            while (retries < 0 or retries) and not self.is_connected:
                if last_time is None or interval() >= self.p.reconnect_timeout:
                    if retries > 0:
                        retries = max(0, retries - 1)
                    is_connect_command_success = self.conn.connect(
                        **self._get_connection_params()
                    )
                    if is_connect_command_success:
                        # to prevent continuously send connect command
                        self.dont_reconnect = True
                        # It's ok, we've sent the command successfully.
                        # And we need to wait for the appropriate status message
                        break
                    logging.info('Connection failed!')

                    if retries > 0:
                        logging.warning('Retry to connect: %d/%d', self.p.reconnect - retries, self.p.reconnect)
                last_time = datetime.now()
                time.sleep(self.p.reconnect_timeout - interval())

            try:
                # Waiting for server_status
                while is_connect_command_success:
                    # todo: need to find better solution
                    # ожидаем обработку сообщения статуса об успешном соединении в клиенте
                    if self.is_connected:
                        if not from_start or resubscribe_datas:
                            self.start_datas()
                        logging.info('Connected at: %s', datetime.now())
                        return True
                    elif self.conn.is_connection_error:
                        self.dont_reconnect = True
                        logging.warning('Unable to connect: %s', self.conn.get_connection_error())
                        return False
                    logging.debug('Waiting for connected status message ...')
                    time.sleep(1)
            except KeyboardInterrupt:
                if is_connect_command_success:
                    self.conn.disconnect()
                self.dont_reconnect = True
                raise

            self.dont_reconnect = not is_connect_command_success
            return False

    def start_datas(self):
        """
        kickstrat datas, not returning until all of them have been done
        :return:
        """
        threads = list()
        for data in self.datas:
            thread = threading.Thread(target=data.request_data)
            thread.start()
            threads.append(thread)

        for thread in threads:
            thread.join()

    def stop_datas(self):
        # stop subs and force datas out of the loop (in LIFO order)
        qs = list(self.qs.values())
        ts = list()
        for data in self.datas:
            t = threading.Thread(target=data.canceldata)
            t.start()
            ts.append(t)

        for t in ts:
            t.join()

        for q in reversed(qs):  # datamaster the last one to get a None
            q.put(None)

    def get_notifications(self):
        """Return the pending "store" notifications"""
        # The background thread could keep on adding notifications. The None
        # mark allows to identify which is the last notification to deliver
        self.notifs.put(None)  # put a mark
        notifs = list()
        while True:
            notif = self.notifs.get()
            if notif is None:  # mark is reached
                break
            notifs.append(notif)

        return notifs

    def request_profile_updates(self, subscribe=True, account=None):
        # Мы не можем подписаться на обновление этих данных.
        # Поэтому когда надо перезапрашиваем сбросив текущее значение или делаем это периодически
        if account is None:
            account = self.get_account()  # list(self._accounts.values())[0]

        if subscribe:
            if self._account_updates_timer:
                self._account_updates_timer.cancel()
            self._account_updates_timer = threading.Timer(
                60,
                self.request_profile_updates, args=None,
                kwargs=dict(subscribe=True, account=None)
            )
            self._account_updates_timer.start()
        else:
            with self._lock_portfolio_update:
                self._event_portfolio.clear()
                self._portfolio = None
        self.conn.get_mc_portfolio(account.id)

    @transaq_handler(message_type=MultiPortfolio.ROOT_NAME)
    def update_portfolio(self, portfolio: MultiPortfolio):
        with self._lock_portfolio_update:
            self._portfolio = portfolio
            self._event_portfolio.set()

    def get_portfolio(self):
        self._event_managed_accounts.wait()
        if self._portfolio is None:
            self._event_portfolio.wait()
        return self._portfolio

    def _on_account_updated(self, client, removed=False):
        pass

    def get_account_value(self):
        portfolio = self.get_portfolio()
        if portfolio is not None:
            return portfolio.equity

    def get_account_cash(self):
        portfolio = self.get_portfolio()
        if self.p.base_currency is None:
            return portfolio.moneys[0].balance

        for m in portfolio.moneys:
            if m.currency == self.p.base_currency:
                return m.balance
        return None

    @transaq_handler(message_type=ClientAccount.ROOT_NAME)
    def account_update(self, account: ClientAccount):
        with self._lock_account_update:
            if account.active:
                self._accounts[account.id] = account
                self._on_account_updated(self._accounts[account.id])
            elif account.id in self._accounts:
                del self._accounts[account.id]
                self._on_account_updated(self._accounts[account.id], removed=True)
            if self._client_id is None:
                self._client_id = account.id
                if self._current_market and self._current_market.id != account.market \
                        or self.p.union and account.union != self.p.union:
                    self._client_id = None

        self._event_managed_accounts.set()

    def get_account(self) -> ClientAccount:
        if len(self._accounts) == 0:
            # API doc ch. 4.3
            logger.debug('Waiting for client Accounts from server ...')
        self._event_managed_accounts.wait()
        return self._accounts[self._client_id]

    @transaq_handler()
    def server_status(self, status_message: ServerStatus):
        self._initial_data_loading = not status_message.is_connected
        if hasattr(status_message, 'timezone'):
            self._server_timezone = win_tz.get(status_message.timezone, None)
            logging.info('Server timezone: %s', self._server_timezone)
        if status_message.is_connected:
            if self._on_connect_callback:
                self._on_connect_callback(status_message)
            if status_message.is_recover:
                self._recovering_from_time_utc = datetime.utcnow()
            elif self._recovering_from_time_utc:
                self._recover()
                self._recovering_from_time_utc = None
        elif not status_message.is_connected and not status_message.is_error and not status_message.is_recover:
            # diconected
            logging.warning('Store has been disconnected at: %s', datetime.now())
        if status_message.is_connected and not status_message.is_recover:
            self.synchronize_time()

    @transaq_handler(message_type=CandleKindPacket.ROOT_NAME)
    def candle_kinds(self, cc: CandleKindPacket):
        """
        1: 1 минута - 60 sec
        2: 5 минут - 300 sec
        3: 15 минут - 900 sec
        4: 1 час - 3600 sec
        5: 1 сутки - 86400 sec
        6: 1 неделя - 604800 sec
        """
        self._candle_kinds = [i for i in cc.items]

    @transaq_handler()
    def securities(self, securities: SecurityPacket):
        with self._lock_securities:
            for s in securities.items:
                s: Security
                ticker = Ticker(s.board, s.seccode)
                if s.active:
                    self._securities[ticker.id] = s
                    self._securities_bysecid[s.id] = s
                else:
                    self._securities.pop(ticker.id, None)
                    self._securities_bysecid.pop(s.id, None)

    def _recover(self):
        raise NotImplemented

    def request_market_data(self, ticker: Ticker):
        _, q = self.make_ticker_queue(ticker)
        self.conn.subscribe(SubscriptionEntity.ALL_TRADES, [ticker])
        return q

    def cancel_market_data(self, q: queue.Queue):
        """Cancels an existing MarketData subscription

        Params:
          - q: the Queue returned by reqMktData
        """
        with self._lock_queue:
            ticker = self._tickers.get(q, None)
            if ticker is not None:
                self.conn.unsubscribe(SubscriptionEntity.ALL_TRADES, [ticker])

            self.cancelQueue(q, True)

    def cancel_queue(self, q: queue.Queue, send_none=False):
        """Cancels a Queue for data delivery"""
        # pop ts (tickers) and with the result qs (queues)
        ticker = self._tickers.pop(q, None)
        self._queues.pop(ticker, None)
        if send_none:
            q.put(None)

    @staticmethod
    def make_initial_ticker_queue() -> queue.Queue:
        """Creates ticker/Queue for data delivery to a data feed"""
        q = queue.Queue()
        q.put(None)
        return q

    def make_ticker_queue(self, ticker: Ticker, period: int = None):
        """Creates ticker/Queue for data delivery to a data feed"""
        q = queue.Queue()
        with self._lock_queue:
            _id = "{}-{}".format(ticker.id, str(period))
            self._queues[_id] = q  # can be managed from other thread
            self._tickers[q] = _id
            # self.iscash[ticker_id] = False
        return _id, q

    def get_ticker_queue(self, ticker: Ticker, period: int = None):
        _id = "{}-{}".format(ticker.id, str(period))
        return _id, self._queues.get(_id, None)

    def synchronize_time(self):
        result: TimeDiffResult = self.conn.get_server_time_difference()
        if result.success:
            self._time_offset = timedelta(seconds=result.diff + self.p.time_diff_adjustment)
            logging.info('Server time difference: %s sec', self._time_offset)

    def gettimeoffset(self):
        with self._lock_time_offset:
            return self._time_offset

    def place_order(self, order):
        params = order.tq_params.copy()
        # params.pop('client')
        # union = self._union
        command = params.pop('command')
        func = getattr(self.conn, command)
        result = func(**params)
        return result.success, result.id

    def cancel_order(self, transaction_id: str):
        response = self.conn.cancel_order(transaction_id)
        return response.success, response.text

    def cancel_stoporder(self, transaction_id: str):
        response = self.conn.cancel_stoporder(transaction_id)
        return response.success, response.text

    def request_history_data(self, ticker: Ticker, timeframe: TimeFrame,
                             enddate: datetime, begindate: datetime,
                             compression: int, tz: pytz.BaseTzInfo,
                             sessionend: time, reset=True):
        # Keep a copy for error reporting purposes
        kwargs = locals().copy()
        kwargs.pop('self', None)  # remove self, no need to report it
        kwargs.pop('reset', None)

        if enddate is None:
            enddate = datetime.now()

        if begindate is None:
            raise NotImplemented()

        candle_kind = self.get_candle_kind(timeframe, compression)
        if reset:
            ticker_id, q = self.make_ticker_queue(ticker, candle_kind.id)
        else:
            ticker_id, q = self.get_ticker_queue(ticker, candle_kind.id)

        candle_count = 100
        hparams = self._history_params.get(ticker_id, None)

        if hparams is None:
            candle_count = abs(
                math.ceil(
                    (enddate - begindate).total_seconds() / candle_kind.period /
                    (4 if timeframe < TimeFrame.Days else 1)
                )
            )
        else:
            candle_count = abs(
                math.ceil(
                    (hparams['earliest_candle_date'] - begindate).total_seconds() / candle_kind.period
                )
            )

        # Store the calculated data
        self._history_params.setdefault(ticker_id, {
            'request_kwargs': kwargs,
            'format': timeframe >= TimeFrame.Days,
            'session_end': sessionend,
            'tz': tz,
            'begindate': begindate,
        })

        self.conn.get_history_data(
            ticker,
            candle_kind.id,
            candle_count,
            reset=reset
        )

        return q

    @transaq_handler()
    def candles(self, candles: HistoryCandlePacket):
        if candles.status == HistoryCandleStatus.NO_DATA:
            logging.warning('Requested data are not available, try later.', candles.ticker)
            return
        tq: queue.Queue
        ticker_id, tq = self.get_ticker_queue(candles.ticker, candles.period)

        def stop_load_history():
            hparams_ = self._history_params.pop(ticker_id, None)
            s_ = hparams_.get('stack', [])
            s_[0].stop_marker = True
            while len(s_):
                tq.put(s_.pop())

            self.cancel_queue(tq)

        hparams = self._history_params.get(ticker_id)
        candle_count = 0

        for candle in reversed(candles.items):
            candle: HistoryCandle
            if candle.date < hparams['begindate']:
                stop_load_history()
                return

            if hparams['format']:
                session_end = hparams['session_end']
                dt = candle.date.date()
                date_eos = datetime.combine(dt, session_end)
                tz = hparams['tz']
                if tz:
                    date_eos_tz = tz.localize(date_eos)
                    date_eos_utc = date_eos_tz.astimezone(pytz.UTC).replace(tzinfo=None)
                    # When requesting for example daily bars, the current day
                    # will be returned with the already happened data. If the
                    # session end were added, the new ticks wouldn't make it
                    # through, because they happen before the end of time
                else:
                    date_eos_utc = date_eos

                if date_eos_utc <= datetime.utcnow():
                    dt = date_eos_utc

                candle.date = dt
            else:
                pass
            hparams: dict
            s = hparams.setdefault('stack', [])
            s.append(candle)
            hparams['earliest_candle_date'] = candle.date
            candle_count += 1

        if not candle_count:
            stop_load_history()
            return

        if candles.status == HistoryCandleStatus.EOD:
            stop_load_history()
        elif candles.status == HistoryCandleStatus.TO_BE_CONTINUE:
            return
        elif candles.status == HistoryCandleStatus.REQUEST_COMPLETED or candles.status == HistoryCandleStatus.EOD:
            first_candle = candles.items[0]
            if first_candle.date > hparams['begindate']:
                # запрос свечей в продолжение
                self.request_history_data(**hparams['request_kwargs'], reset=False)
                return
            else:
                stop_load_history()

    @transaq_handler(message_type="alltrades")
    def handle_tick(self, trades: TradePacket):
        for trade in trades.items:
            trade: Trade
            ticker = Ticker(trade.board, trade.seccode)
            # todo: take in account time offset between server and client
            _, q = self.get_ticker_queue(ticker)
            if q is None:
                # skip ticks from terminated subscription
                pass
            else:
                q.put(trade)

    def _on_unions_changed(self, union_id: str, removed=False):
        pass

    @transaq_handler(message_type='union')
    def update_unions(self, union: TransaqUnion):
        if union.active:
            if union.id not in self._unions:
                self._unions.append(union.id)
                self._on_unions_changed(union.id)
        else:
            if union.id in self._unions:
                self._unions.remove(union.id)
                self._on_unions_changed(union.id, removed=True)

    @transaq_handler(message_type='markets')
    def update_markets(self, markets: MarketPacket):
        self._markets = {}
        for market in markets.items:
            if self.p.market and market.name == self.p.market:
                self._current_market = market
            self._markets[market.id] = market.name

        logger.info('Markets: %s', ", ".join([str(id) + ': ' + (name or '') for id, name in self._markets.items()]))

    def get_position(self, ticker: Ticker):
        with self._lock_positions:
            return self._positions[ticker.seccode]

    @transaq_handler(message_type='positions')
    def update_positions(self, packet: PositionPacket):
        with self._lock_securities:
            with self._lock_positions:
                for item in packet.items:
                    if item.ROOT_NAME == SecurityPosition.ROOT_NAME:
                        if item.client != self._client_id:
                            continue
                        security = self._securities_bysecid.get(item.id)
                        if security is None:
                            continue
                        position = Position(item.saldo / security.lotsize, item.amount)
                        self._positions[item.seccode] = position

    @transaq_handler()
    def order(self, order):
        raise NotImplemented

    @transaq_handler()
    def orders(self, orders: ClientOrderPacket):
        for order_message in orders.items:
            if self.broker is None:
                logger.warning("order was received before broker set: %s", order_message.__repr__())
                continue

            if self._client_id != order_message.client:
                continue
            if order_message.ROOT_NAME == Order.ROOT_NAME:
                self.broker.update_order(order_message)
            elif order_message.ROOT_NAME == StopOrder.ROOT_NAME:
                self.broker.update_stop_order(order_message)

    @transaq_handler()
    def messages(self, messages: TextMessagePacket):
        for message in messages.items:
            func = logger.warning if message.urgent else logger.info
            func("Transaq server message from %s: %s", message.sender, message.text)

    def set_on_connect_callback(self, func: Callable):
        if not callable(func):
            raise Exception('Argument `func` should be callable')
        self._on_connect_callback = func
