import collections
import logging
from backtrader import BrokerBase, Order
from backtrader.utils.py3 import with_metaclass, queue
from backtrader_transaq.data import TransaqData
from backtrader_transaq.order import TransaqOrder
from backtrader_transaq.store import TransaqStore
from transaqpy.commands import BuySellAction
from transaqpy.structures import \
    Order as TqOrder, OrderStatus as TransaqOrderStatus, BaseOrder as TqBaseOrder, StopOrder

logger = logging.getLogger(__name__)


class MetaFinamBroker(BrokerBase.__class__):
    def __init__(cls, name, bases, dct):
        # Initialize the class
        super(MetaFinamBroker, cls).__init__(name, bases, dct)
        TransaqStore.BrokerCls = cls


class TransaqBroker(with_metaclass(MetaFinamBroker, BrokerBase)):
    """Broker implementation for Finam."""

    params = ()
    _notifs = None
    _tonotify = None

    def __init__(self, **kwargs):
        super().__init__()

        self._store = TransaqStore(**kwargs)
        self.startingcash = self.cash = 0.0
        self.startingvalue = self.value = 0.0
        self._notifs = queue.Queue()  # holds orders which are notified
        self._tonotify = collections.deque()  # hold oids to be notified
        self._orderbyid = dict()

        # to keep dependent orders if needed
        self._pchildren = collections.defaultdict(collections.deque)

    def start(self):
        super().start()
        self._store.start(broker=self)

        if self._store.is_connected:
            self._store.request_profile_updates(subscribe=False)
            self.startingcash = self.cash = self._store.get_account_cash()
            self.startingvalue = self.value = self._store.get_account_value()
        else:
            self.startingcash = self.cash = 0.0
            self.startingvalue = self.value = 0.0

    def stop(self):
        super().stop()
        self._store.stop()

    def notify(self, order):
        self._notifs.put(order.clone())

    def get_notification(self):
        try:
            return self._notifs.get(False)
        except queue.Empty:
            pass

        return None

    def next(self):
        self._notifs.put(None)  # mark notification boundary

    def add_order_history(self, orders, notify=False):
        raise NotImplemented()

    def set_fund_history(self, fund):
        raise NotImplemented()

    def getcash(self):
        self.cash = self._store.get_account_cash()
        return self.cash

    def getvalue(self, datas=None):
        """Стоимость портфеля"""

        if datas is not None:
            raise NotImplemented()
        # Текущая оценка стоимости единого портфеля
        self.value = self._store.get_account_value()
        return self.value

    def getposition(self, data: TransaqData):
        return self._store.get_position(data.trade_ticker)

    # def submit(self, order: TransaqOrder) -> Optional[Order]:
    #     success, transactionid = self._store.place_order(order)
    #     if not success:
    #         return None
    #
    #     order.submit(self)
    #     self._orderbyid[transactionid] = order
    #     self.notify(order)
    #     return order

    def _take_children(self, order):
        oref = order.ref
        pref = getattr(order.parent, 'ref', oref)  # parent ref or self

        if oref != pref:
            if pref not in self._pchildren:
                order.reject()  # parent not there - may have been rejected
                self.notify(order)  # reject child, notify
                return None

        return pref

    def submit(self, order, check=True):
        pref = self._take_children(order)
        if pref is None:  # order has not been taken
            return order

        pc = self._pchildren[pref]
        pc.append(order)  # store in parent/children queue

        if order.transmit:  # if single order, sent and queue cleared
            # if parent-child, the parent will be sent, the other kept
            rets = [self.transmit(x) for x in pc]
            return rets[-1]  # last one is the one triggering transmission

        return order

    def transmit(self, order):
        success, transactionid, message = self._store.place_order(order)
        if not success:
            logger.warning('Не удалось поместить заявку: %s', message)
            return None
        self._orderbyid[transactionid] = order
        order.tq_id = transactionid
        order.submit(self)
        self.notify(order)

        return order

    def cancel(self, order: TransaqOrder):
        try:
            idx = list(self._orderbyid.values()).index(order)
        except ValueError:
            return  # not found ... not cancellable

        if order.status == Order.Cancelled:  # already cancelled
            return

        transaction_id = list(self._orderbyid.keys())[idx]
        if 'new_sl_tp_order' == order.tq_params['command']:
            return self._store.cancel_stoporder(transaction_id)
        else:
            return self._store.cancel_order(transaction_id)

    def buy(self, owner, data, size, _checksubmit=True, **kwargs):
        order = TransaqOrder(
            BuySellAction.BUY,
            client_id=self._store.get_account().id,
            pricelimit=kwargs.pop('plimit', None),
            data=data,
            owner=owner,
            size=size,
            **kwargs
        )
        order.addinfo(**kwargs)
        return self.submit(order, check=_checksubmit)

    def sell(self, owner, data, size, _checksubmit=True, **kwargs):
        order = TransaqOrder(
            BuySellAction.SELL,
            client_id=self._store.get_account().id,
            pricelimit=kwargs.pop('plimit', None),
            data=data,
            owner=owner,
            size=size,
            **kwargs
        )
        order.addinfo(**kwargs)
        return self.submit(order, check=_checksubmit)

    # def getcommissioninfo(self, data):
    #     raise NotImplemented()

    def _update_common_orderstatus(self, order: Order, order_message: TqBaseOrder):

        if order_message.status == TransaqOrderStatus.CANCELED:
            if order.status == order.Cancelled:
                return True
            order.cancel()
            self.notify(order)
            return True
        elif order_message.status == TransaqOrderStatus.EXPIRED:
            if order.status in [order.Expired]:
                return True
            order.expire()
            self.notify(order)
            return True
        elif order_message.status in [
            TransaqOrderStatus.DENIED,
            TransaqOrderStatus.FAILED,
            TransaqOrderStatus.REJECTED
        ]:
            if order.status == order.Rejected:
                return True
            order.reject(self)
            self.notify(order)
            return True
        elif order_message.status == TransaqOrderStatus.DISABLED:
            if order.status == order.Cancelled:
                return True
            order.cancel()
            self.notify(order)
            return True
        elif order_message.status == TransaqOrderStatus.WATCHING:
            if order.status == order.Accepted:
                return True
            order.accept(self)
            self.notify(order)
            return True
        return False

    def update_order(self, order_message: TqOrder):
        #     Created, Submitted, Accepted, Partial, Completed, \
        #         Canceled, Expired, Margin, Rejected
        try:
            order = self._orderbyid[order_message.id]
        except KeyError:
            return

        if self._update_common_orderstatus(order, order_message):
            return

        if order_message.status == TransaqOrderStatus.ACTIVE:
            if order_message.order_no or order_message.order_no == 0 and order_message.condition is None:
                order.accept(self)
                self.notify(order)
        elif order_message.status == TransaqOrderStatus.FORWARDING:
            pass
        elif order_message.status == TransaqOrderStatus.INACTIVE:
            pass
        elif order_message.status == TransaqOrderStatus.MATCHED:
            order.completed()
            self.notify(order)
        elif order_message.status == TransaqOrderStatus.REFUSED:
            if order.status != order.Rejected:
                order.reject(self)
                self.notify(order)
        elif order_message.status == TransaqOrderStatus.REMOVED:
            # ??
            pass
        elif order_message.status == TransaqOrderStatus.WAIT:
            if order.status != order.Accepted:
                order.accept(self)
                self.notify(order)

    def update_stop_order(self, order_message: StopOrder):
        try:
            order = self._orderbyid[order_message.id]
        except KeyError:
            return

        if self._update_common_orderstatus(order, order_message):
            return

        if order_message.status == TransaqOrderStatus.LINKWAIT:
            if order.status == order.Accepted:
                return
            order.accept(self)
            self.notify(order)
        elif order_message.status == TransaqOrderStatus.SL_EXECUTED or \
                order_message.status == TransaqOrderStatus.TP_EXECUTED:
            order.completed()
            self.notify(order)
        elif order_message.status == TransaqOrderStatus.SL_FORWARDING:
            pass
        elif order_message.status == TransaqOrderStatus.SL_GUARDTIME:
            pass
        elif order_message.status == TransaqOrderStatus.TP_CORRECTION:
            pass
        elif order_message.status == TransaqOrderStatus.TP_CORRECTION_GUARDTIME:
            pass
        elif order_message.status == TransaqOrderStatus.TP_FORWARDING:
            pass
        elif order_message.status == TransaqOrderStatus.TP_GUARDTIME:
            pass


