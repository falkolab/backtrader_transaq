import logging
from enum import IntEnum

import backtrader as bt
from backtrader import num2date, date2num
from backtrader.feed import DataBase
from backtrader_transaq.store import TransaqStore
from backtrader.utils.py3 import with_metaclass
from transaqpy.commands import Ticker
from backtrader.utils.py3 import queue
from transaqpy.structures import Trade as TransaqTrade, HistoryCandle

logger = logging.getLogger(__name__)


class FeedState(IntEnum):
    FROM = 1
    START = 2
    LIVE = 3
    HISTORY_BACK = 4
    OVER = 5


class MetaTransaqData(DataBase.__class__):
    def __init__(cls, name, bases, dct):
        """Class has already been created ... register"""
        # Initialize the class
        super(MetaTransaqData, cls).__init__(name, bases, dct)

        # Register with the store
        TransaqStore.DataCls = cls


class TransaqData(with_metaclass(MetaTransaqData, DataBase)):
    """Transaq Data Feed.

    Params:
      - `historical` (default: `False`)

        If set to `True` the data feed will stop after doing the first
        download of data.

        The standard data feed parameters `fromdate` and `todate` will be
        used as reference.

        The data feed will make multiple requests if the requested duration is
        larger than the one allowed by IB given the timeframe/compression
        chosen for the data.
    
      - `backfill_start` (default: `True`)

        Perform backfilling at the start. The maximum possible historical data
        will be fetched in a single request.

      - `backfill` (default: `True`)

        Perform backfilling after a disconnection/reconnection cycle. The gap
        duration will be used to download the smallest possible amount of data

      - `backfill_from` (default: `None`)

        An additional data source can be passed to do an initial layer of
        backfilling. Once the data source is depleted and if requested,
        backfilling from IB will take place. This is ideally meant to backfill
        from already stored sources like a file on disk, but not limited to.
        
      - `tradename` (default: `None`)
        Useful for some specific cases like `CFD` in which prices are offered
        by one asset and trading happens in a different one

      - ``qcheck`` (default: ``0.5``)

        Time in seconds to wake up if no data is received to give a chance to
        resample/replay packets properly and pass notifications up the chain

      - ``latethrough`` (default: ``False``)

        If the data source is resampled/replayed, some ticks may come in too
        late for the already delivered resampled/replayed bar. If this is
        ``True`` those ticks will bet let through in any case.

        Check the Resampler documentation to see who to take those ticks into
        account.
    """
    params = (
        ('historical', False),  # only historical download
        ('backfill_start', True),  # do backfilling at the start
        ('backfill', True),  # do backfilling when reconnecting
        ('backfill_from', None),  # additional data source to do backfill from
        ('tradename', None),  # use a different asset as order target
        ('qcheck', 0.5),  # timeout in seconds (float) to check for events
        ('board', 'TQBR'),  # Board code (str)
        ('latethrough', False),  # let late samples through
    )
    _qlive = None
    _qhist = None
    _state: FeedState = None
    _store = TransaqStore
    _state_live_reconnecting = None
    _subscription_valid = None
    _stored_message = None

    def __init__(self, **kwargs):
        self._store = self._store(**kwargs)
        self.ticker = self.parse_ticker(self.p.dataname)
        self.trade_ticker = self.parse_ticker(self.p.tradename)

    def parse_ticker(self, dataname):
        # Set defaults for optional tokens in the ticker string
        if dataname is None:
            return None

        # split the ticker string
        tokens = iter(dataname.split('-'))

        seccode = next(tokens)
        try:
            board = next(tokens)
        except StopIteration:
            board = self.p.board

        return Ticker(board, seccode)

    def start(self):
        """Starts the Transaq Connector connection"""
        super().start()
        # Kickstart store and get queue to wait on
        self._qlive = self._store.start(data=self)
        self._qhist = None

        if self.p.backfill_from is not None:
            self._state = FeedState.FROM
            external_data = self.p.backfill_from
            external_data.setenvironment(self._env)
            external_data._start()
        else:
            self._state = FeedState.START  # initial state for _load

        if not self.resampling and not self.replaying:
            candle_kind = self._store.get_candle_kind(self._timeframe, self._compression)
            if candle_kind is None:
                logger.debug('Not supported timeframe')
                self.put_notification(self.NOTSUPPORTED_TF)
                self._state = FeedState.OVER
                return

        self._state_live_reconnecting = False  # if reconnecting in live state
        self._subscription_valid = False  # subscription state
        self._stored_message = dict()  # keep pending live message (under None)

        if not self._store.is_connected:
            return

        self.put_notification(self.CONNECTED)

        if self.trade_ticker is None:
            self.trade_ticker = self.ticker

        if self._state == FeedState.START:
            self._start_finish()  # to finish initialization
            self._st_start()

    def _gettz(self):
        tzstr = isinstance(self.p.tz, str)
        if self.p.tz is not None and not tzstr:
            return bt.utils.date.Localizer(self.p.tz)

        try:
            import pytz  # keep the import very local
        except ImportError:
            return None  # nothing can be done

        tzs = self.p.tz if tzstr else self._store.server_timezone
        try:
            tz = pytz.timezone(tzs)
        except pytz.UnknownTimeZoneError:
            return None  # nothing can be done

        # server_timezone there, import ok, timezone found, return it
        return tz

    def _timeoffset(self):
        # datetime.timedelta()
        return self._store.gettimeoffset()

    def _st_start(self) -> bool:
        if self.p.historical:
            self.put_notification(self.DELAYED)
            dtend = None
            if self.todate < float('inf'):
                dtend = num2date(self.todate)

            dtbegin = None
            if self.fromdate > float('-inf'):
                dtbegin = num2date(self.fromdate)

            timeframe, compression = self._timeframe, self._compression
            if self.resampling or self.replaying:
                timeframe, compression = self._store.get_most_suitable_period(self._timeframe, self._compression)

            self._qhist = self._store.request_history_data(
                self.ticker, timeframe, dtend, dtbegin,
                compression, self._tz, self.p.sessionend,
            )

            self._state = FeedState.HISTORY_BACK
            return True  # continue before

        # Live is requested
        if not self._store.reconnect(resubscribe_datas=True):
            logger.debug('Disconnected')
            self.put_notification(self.DISCONNECTED)
            self._state = FeedState.OVER
            return False  # failed - was so

        self._state_live_reconnecting = self.p.backfill_start
        if self.p.backfill_start:
            self.put_notification(self.DELAYED)

        self._state = FeedState.LIVE
        return True  # no return before - implicit continue

    def stop(self):
        """Stops and tells the store to stop"""
        super().stop()
        self._store.stop()

    def islive(self):
        """Returns `True` to notify `Cerebro` that preloading and runonce
        should be deactivated"""
        return not self.p.historical

    def request_data(self):
        if self._subscription_valid:
            return

        self._qlive = self._store.request_market_data(self.ticker)
        self._subscription_valid = True

    def cancel_data(self):
        """Cancels Market Data subscription"""
        self._store.cancel_market_data(self._qlive)

    def haslivedata(self):
        return bool(self._stored_message or self._qlive)

    def _load(self):
        if self.ticker is None or self._state == FeedState.OVER:
            logger.debug('Nothing can be done')
            return False  # nothing can be done

        while True:
            if self._state == FeedState.LIVE:
                try:
                    message: TransaqTrade = (self._stored_message.pop(None, None) or
                                             self._qlive.get(timeout=self._qcheck))
                except queue.Empty:
                    return None

                if message is None:  # Connection is broken during historical/backfilling
                    self._subscription_valid = False
                    self.put_notification(self.CONNBROKEN)
                    # Try to reconnect
                    if not self._store.reconnect(resubscribe_datas=True):
                        self.put_notification(self.DISCONNECTED)
                        return False  # failed
                    self._state_live_reconnecting = self.p.backfill
                    continue

                # Process the message according to expected return type
                if not self._state_live_reconnecting:
                    if self._laststatus != self.LIVE:
                        if self._qlive.qsize() <= 1:  # very short live queue
                            self.put_notification(self.LIVE)
                    if self._load_trade(message):
                        return True
                    # could not load bar ... go and get new one
                    continue

                # Fall through to processing reconnect - try to backfill
                self._stored_message[None] = message  # keep the msg

                # else do a backfill
                if self._laststatus != self.DELAYED:
                    self.put_notification(self.DELAYED)

                if len(self) > 1:
                    # len == 1 ... forwarded for the 1st time
                    dtbegin = num2date(self.datetime[-1], self._tz)
                elif self.fromdate > float('-inf'):
                    dtbegin = num2date(self.fromdate, self._tz)
                else:  # 1st bar and no begin set
                    # passing None to fetch max possible in 1 request
                    dtbegin = None

                dtend = message.time
                timeframe, compression = self._timeframe, self._compression
                if self.resampling or self.replaying:
                    timeframe, compression = self._store.get_most_suitable_period(self._timeframe, self._compression)

                self._qhist = self._store.request_history_data(
                    self.ticker, timeframe,
                    dtend, dtbegin, compression, self._tz,
                    self.p.sessionend)

                self._state = FeedState.HISTORY_BACK
                self._state_live_reconnecting = False  # no longer in live
                continue

            elif self._state == FeedState.HISTORY_BACK:
                message = self._qhist.get()
                #print('>>HISTORY_BACK: ', message)
                message: HistoryCandle
                if message is None:  # Conn broken during historical/backfilling
                    # Situation not managed. Simply bail out
                    self._subscription_valid = False
                    self.put_notification(self.DISCONNECTED)
                    return False  # error management cancelled the queue

                if not hasattr(message, 'stop_marker'):
                    if self._load_hbar(message):
                        return True  # loading worked

                    # the date is from overlapping historical request
                    continue
                else:
                    self._load_hbar(message)

                # End of histdata
                if self.p.historical:  # only historical
                    self.put_notification(self.DISCONNECTED)
                    return False  # end of historical

                # Live is also wished - go for it
                self._state = FeedState.LIVE
                continue
            elif self._state == FeedState.FROM:
                if not self.p.backfill_from.next():
                    # additional data source is consumed
                    self._state = FeedState.START
                    continue

                # copy lines of the same name
                for alias in self.lines.getlinealiases():
                    lsrc = getattr(self.p.backfill_from.lines, alias)
                    ldst = getattr(self.lines, alias)
                    ldst[0] = lsrc[0]
                return True
            elif self._state == FeedState.START:
                if not self._st_start():
                    return False

    def _load_trade(self, trade: TransaqTrade):
        logger.debug('Adding trade for %s: %s', self._dataname, trade.__repr__())
        dt = date2num(trade.time)
        if dt < self.lines.datetime[-1] and not self.p.latethrough:
            return False  # cannot deliver earlier than already delivered
        self.lines.datetime[0] = dt
        # Put the tick into the bar
        tick = trade.price
        self.lines.open[0] = tick
        self.lines.high[0] = tick
        self.lines.low[0] = tick
        self.lines.close[0] = tick
        self.lines.volume[0] = trade.quantity
        self.lines.openinterest[0] = 0
        return True

    def _load_hbar(self, candle: HistoryCandle):
        logger.debug('Adding hbar for %s: %s', self._dataname, candle.__repr__())
        dt = date2num(candle.date)
        if dt < self.lines.datetime[-1] and not self.p.latethrough:
            return False  # cannot deliver earlier than already delivered
        self.lines.datetime[0] = dt
        # Put the tick into the bar
        self.lines.open[0] = candle.open
        self.lines.high[0] = candle.high
        self.lines.low[0] = candle.low
        self.lines.close[0] = candle.close
        self.lines.volume[0] = candle.volume
        self.lines.openinterest[0] = 0
        return True
