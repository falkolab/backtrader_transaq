from backtrader import OrderBase
from backtrader_transaq import TransaqData
from transaqpy.commands import BuySellAction, StopOrderType


class TransaqOrder(OrderBase):
    """
    Bymarket / По рынку
        Заявка по рынку. При наличии тега bymarket, тег price игнорируется и может отсутствовать.

    Usecredit / Использовать кредит
        Чтобы воспользоваться кредитом при заключении сделки, установите параметр «Использовать кредит» в поручении на выставление заявки.
    Nosplit / По одной цене
        При наличии встречных заявок с пересекающимися ценами, сделка совершается по цене и в объеме лучшей встречной заявки. Неисполненная часть подаваемой заявки помещается в очередь заявок с ценой, равной цене совершенной сделки.
    PutInQueue / Поставить в очередь
        При поступлении заявки в торговую систему Биржи производится проверка наличия в очереди встречных заявок, цены которых совпадают или пересекаются с ценой подаваемой заявки. При наличии таких заявок в системе производятся сделки по цене ранее поданной заявки.
        Затем количество лотов инструмента, указанное в заявке, уменьшается на количество лотов в совершенной сделке и производится аналогичная процедура сопоставления новой заявки с оставшимися встречными заявками. Неисполненная часть заявки помещается в очередь заявок.
    FOK / Немедленно или отклонить
        Аналогично Поставить в очередь, но сделки совершаются только в том случае, если заявка может быть удовлетворена полностью.
        В противном случае заявка не выставляется
    IOC / Снять остаток
        Аналогично Поставить в очередь, но неисполненная часть заявки снимается с торгов
    """

    def __init__(self, action: BuySellAction, **kwargs):
        self.ordtype = self.Buy if action == BuySellAction.BUY else self.Sell
        # общие опции

        if self.exectype is None:
            if not {'stoploss', 'takeprofit'} & set(kwargs.keys()):
                self.exectype = self.Market  # default
            else:
                self.exectype = -1

        super().__init__()
        self.tq_id = None
        self.tq_action = action
        data: TransaqData = self.data

        self.tq_transmit = self.transmit
        # if self.parent is not None:
            # self.tq_parent_id = self.parent.m_orderId

        params = {key: kwargs[key] for key in (
            'brokerref', 'usecredit', 'expdate', 'union', 'stoploss', 'takeprofit',
        ) if key in kwargs.keys()}

        self.tq_params = params
        params.update(
            ticker=data.trade_ticker,
            buysell=action,
            client=kwargs['client_id'],
            quantity=abs(int(self.size)),
        )

        if self.exectype in (self.Market, self.Limit):
            params.update({key: kwargs[key] for key in (
                'nosplit'
            ) if key in kwargs.keys()})
            if 'cond_type' in kwargs:
                # условная заявка
                params.update({key: kwargs[key] for key in (
                    'cond_type', 'cond_value', 'validafter', 'validbefore', 'within_pos',
                ) if key in kwargs.keys()})
                params['command'] = 'new_conditional_order'
            else:
                params.update({key: kwargs[key] for key in (
                    'unfilled',
                ) if key in kwargs.keys()})
                params['command'] = 'new_order'

        if self.exectype in [self.Stop, self.StopLimit] or 'stoploss' in params:
            params['command'] = 'new_sl_tp_order'
            sl_params = params.setdefault('stoploss', {})
            for key in ('bymarket', 'usecredit', 'quantity', 'guardtime', 'brokerref', 'activationprice'):
                if key in kwargs:
                    sl_params.setdefault(key, kwargs.pop(key))

            # if 'activationprice' not in sl_params:
            #     sl_params['activationprice'] = self.price

        if self.exectype == self.Limit or 'takeprofit' in params:
            params['command'] = 'new_sl_tp_order'
            tp_params = params.setdefault('takeprofit', {})
            for key in ('bymarket', 'usecredit', 'quantity', 'guardtime', 'brokerref', 'correction', 'spread'):
                if key in kwargs:
                    tp_params.setdefault(key, kwargs.pop(key))

        if self.exectype == self.Market:
            params['bymarket'] = True
        elif self.exectype == self.Limit:
            tp_params = params.setdefault('takeprofit', {})
            tp_params.update(
                activationprice=self.price,  # Цена активации
                bymarket=True
            )
        elif self.exectype == self.Stop:
            # if stop_order_type is null
            # BUY: activate when the bar above stop-price (short stop-loss)
            # SELL: activate when the bar below stop-price (long stop-loss)
            sl_params = params.setdefault('stoploss', {})
            sl_params.update(
                activationprice=self.price,  # Цена активации
                bymarket=True,
                quantity=abs(int(params.pop('quantity')))
            )
        elif self.exectype == self.StopLimit:
            # Только stoploss заявка позволяет выставить лимитированную заявку
            sl_params = params.setdefault('stoploss', {})
            sl_params.update(
                activationprice=self.price,  # Цена активации
                orderprice=params.pop('pricelimit'),  # Цена заявки
                bymarket=False,
                quantity=abs(int(params.pop('quantity')))
            )
        elif self.exectype == self.StopTrail:
            raise NotImplemented
        elif self.exectype == self.StopTrailLimit:
            pass
        elif self.exectype == self.Close:
            pass

        stoploss = params.pop('stoploss', {})
        takeprofit = params.pop('takeprofit', {})

        if stoploss and stoploss['quantity'] != abs(self.size) \
                or takeprofit and takeprofit['quantity'] != abs(self.size):
            # Значение size может быть разное для stoploss и takeprofit
            # Установим в None чтобы не вводить в заблуждение
            self.size = None

        params.update({'sl_' + k: v for k, v in stoploss.items()})
        params.update({'tp_' + k: v for k, v in takeprofit.items()})

    @property
    def is_conditional(self):
        return 'cond_type' in self.tq_params.keys()
