import argparse
import datetime
import logging
import os
from dotenv import load_dotenv

import backtrader as bt
from backtrader_transaq.store import TransaqStore
from transaqpy.grpc_connector.connector import GRPCTransaqConnector

logger = logging.getLogger(__name__)

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("debug.log"),
        logging.StreamHandler()
    ]
)

load_dotenv()


# load_dotenv(".env_local")


def runstrategy():
    login = os.environ['TRANSAQ_LOGIN']
    password = os.environ['TRANSAQ_PASSWORD']
    host = os.environ['TRANSAQ_HOST']
    port = os.environ['TRANSAQ_PORT']
    bridge_server = os.environ['GRPC_SERVER']

    args = parse_args()
    connector = GRPCTransaqConnector(bridge_server)

    store = TransaqStore(
        transaq_connector=connector,
        login=login, password=password,
        host=host, port=port,
        reconnect=-1,
        reconnect_timeout=10,
        client_id=args.client_id,
        base_currency='RUB'
    )

    # with broker
    broker = store.getbroker()
    cerebro = bt.Cerebro()
    cerebro.setbroker(broker)

    timeframe = bt.TimeFrame.TFrame(args.timeframe)

    if args.resample or args.replay:
        datatf = datatf1 = bt.TimeFrame.Ticks
        datacomp = datacomp1 = 1
    else:
        datatf = timeframe
        datacomp = args.compression

    fromdate = None
    if args.fromdate:
        dtformat = '%Y-%m-%d' + ('T%H:%M:%S' * ('T' in args.fromdate))
        fromdate = datetime.datetime.strptime(args.fromdate, dtformat)

    datakwargs = dict(
        timeframe=datatf, compression=datacomp,
        historical=args.historical, fromdate=fromdate,
        qcheck=args.qcheck,
        backfill_start=not args.no_backfill_start,
        backfill=not args.no_backfill,
        tz=args.timezone
        # todo: add others
    )

    data0 = store.getdata(dataname=args.data0, **datakwargs)
    cerebro.adddata(data0)

    if args.valid is None:
        valid = None
    else:
        valid = datetime.timedelta(seconds=args.valid)

    cerebro.addstrategy(TestStrategy,
                        smaperiod=args.smaperiod,
                        trade=args.trade,
                        exectype=bt.Order.ExecType(args.exectype),
                        stake=args.stake,
                        stopafter=args.stopafter,
                        valid=valid,
                        cancel=args.cancel,
                        donotsell=args.donotsell,
                        stoptrail=args.stoptrail,
                        stoptraillimit=args.traillimit,
                        trailamount=args.trailamount,
                        trailpercent=args.trailpercent,
                        limitoffset=args.limitoffset,
                        oca=args.oca,
                        bracket=args.bracket
                        )
    cerebro.run()


class TestStrategy(bt.Strategy):
    params = dict(
        smaperiod=5,
        trade=False,
        stake=10,
        exectype=bt.Order.Market,
        stopafter=0,
        valid=None,
        cancel=0,
        donotsell=False,
        stoptrail=False,
        stoptraillimit=False,
        trailamount=None,
        trailpercent=None,
        limitoffset=None,
        oca=False,
        bracket=False,
    )

    def __init__(self):
        # To control operation entries
        self.orderid = list()
        self.order = None

        self.counttostop = 0
        self.datastatus = 0

        # Create SMA on 2nd data
        self.sma = bt.indicators.MovAv.SMA(self.data, period=self.p.smaperiod)

        print('--------------------------------------------------')
        print('Strategy Created')
        print('--------------------------------------------------')

    def notify_data(self, data, status, *args, **kwargs):
        print('*' * 5, 'DATA NOTIF:', data._getstatusname(status), *args)
        if status == data.LIVE:
            self.counttostop = self.p.stopafter
            self.datastatus = 1

    def notify_store(self, msg, *args, **kwargs):
        print('*' * 5, 'STORE NOTIF:', msg)

    def notify_order(self, order):
        print('ORDER STATUS: ', order.getstatusname())
        if order.status in [order.Completed, order.Cancelled, order.Rejected]:
            self.order = None

        print('-' * 50, 'ORDER BEGIN', datetime.datetime.now())
        print(order)
        print('-' * 50, 'ORDER END')

    # def notify_cashvalue(self, cash, value):
    #     print('CASH: ', cash, 'VALUE:', value)

    def notify_trade(self, trade):
        print('-' * 50, 'TRADE BEGIN', datetime.datetime.now())
        print(trade)
        print('-' * 50, 'TRADE END')

    def prenext(self):
        self.next(frompre=True)

    def next(self, frompre=False):
        # txt = list()
        # txt.append('Data0')
        # txt.append('%04d' % len(self.data0))
        # dtfmt = '%Y-%m-%dT%H:%M:%S.%f'
        # txt.append('{}'.format(self.data.datetime[0]))
        # txt.append('%s' % self.data.datetime.datetime(0).strftime(dtfmt))
        # txt.append('{}'.format(self.data.open[0]))
        # txt.append('{}'.format(self.data.high[0]))
        # txt.append('{}'.format(self.data.low[0]))
        # txt.append('{}'.format(self.data.close[0]))
        # txt.append('{}'.format(self.data.volume[0]))
        # txt.append('{}'.format(self.data.openinterest[0]))
        # txt.append('{}'.format(self.sma[0]))
        # print(', '.join(txt))

        # if len(self.datas) > 1 and len(self.data1):
        #     txt = list()
        #     txt.append('Data1')
        #     txt.append('%04d' % len(self.data1))
        #     dtfmt = '%Y-%m-%dT%H:%M:%S.%f'
        #     txt.append('{}'.format(self.data1.datetime[0]))
        #     txt.append('%s' % self.data1.datetime.datetime(0).strftime(dtfmt))
        #     txt.append('{}'.format(self.data1.open[0]))
        #     txt.append('{}'.format(self.data1.high[0]))
        #     txt.append('{}'.format(self.data1.low[0]))
        #     txt.append('{}'.format(self.data1.close[0]))
        #     txt.append('{}'.format(self.data1.volume[0]))
        #     txt.append('{}'.format(self.data1.openinterest[0]))
        #     txt.append('{}'.format(float('NaN')))
        #     print(', '.join(txt))

        if self.counttostop:  # stop after x live lines
            self.counttostop -= 1
            if not self.counttostop:
                self.env.runstop()
                return

        if not self.p.trade:
            return

        #if self.position.size and self.order is None:
           # self.order = self.close()
         #   return

        if len(self.orderid) < 1 and self.order is None:
            close = self.data0.close[0]
            self.order = self.sell(
                size=self.p.stake,
                #exectype=Order.Market,
                #price=round(close * 0.9, 2)
                #price=128,
                stoploss=dict(
                    activationprice=127,
                    bymarket=True,
                    guardtime=30,
                    quantity=1
                ),
                takeprofit=dict(
                    activationprice=128,
                    quantity=1,
                    guardtime=10,
                    correction='0.2%',
                    spread=0.02,
                )
            )
            self.orderid.append(self.order)
            return

        # if self.position:
        #     print(self.position)
        #     self.close()
        #     # self.order = self.sell(
        #     #     size=41,
        #     #     exectype=Order.Market,
        #     #     transmit=True)
        #     self.p.trade = False
        #     return
        #
        # if self.datastatus and not self.position and len(self.orderid) < 1:
        #     exectype = self.p.exectype if not self.p.oca else bt.Order.Limit
        #     close = self.data0.close[0]
        #     price = round(close * 0.90, 2)
        #     self.order = self.buy(
        #         size=self.p.stake,
        #         exectype=exectype,
        #         price=price,
        #         valid=self.p.valid,
        #         transmit=not self.p.bracket
        #     )
        #
        #     self.orderid.append(self.order)
        #
        #     if self.p.bracket:
        #         # low side
        #         self.sell(size=self.p.stake,
        #                   exectype=bt.Order.Stop,
        #                   price=round(price * 0.90, 2),
        #                   valid=self.p.valid,
        #                   transmit=False,
        #                   parent=self.order)
        #
        #         # high side
        #         self.sell(size=self.p.stake,
        #                   exectype=bt.Order.Limit,
        #                   price=round(close * 1.10, 2),
        #                   valid=self.p.valid,
        #                   transmit=True,
        #                   parent=self.order)
        #
        #     elif self.p.oca:
        #         self.buy(size=self.p.stake,
        #                  exectype=bt.Order.Limit,
        #                  price=round(self.data0.close[0] * 0.80, 2),
        #                  oco=self.order)
        #
        #     elif self.p.stoptrail:
        #         self.sell(size=self.p.stake,
        #                   exectype=bt.Order.StopTrail,
        #                   # price=round(self.data0.close[0] * 0.90, 2),
        #                   valid=self.p.valid,
        #                   trailamount=self.p.trailamount,
        #                   trailpercent=self.p.trailpercent)
        #
        #     elif self.p.stoptraillimit:
        #         p = round(self.data0.close[0] - self.p.trailamount, 2)
        #         # p = self.data0.close[0]
        #         self.sell(size=self.p.stake,
        #                   exectype=bt.Order.StopTrailLimit,
        #                   price=p,
        #                   plimit=p + self.p.limitoffset,
        #                   valid=self.p.valid,
        #                   trailamount=self.p.trailamount,
        #                   trailpercent=self.p.trailpercent)
        #
        # elif self.position.size > 0 and not self.p.donotsell:
        #     if self.order is None:
        #         self.order = self.sell(size=self.p.stake // 2,
        #                                exectype=bt.Order.Market,
        #                                price=self.data0.close[0])
        #
        # elif self.order is not None and self.p.cancel:
        #     if self.datastatus > self.p.cancel:
        #         self.cancel(self.order)
        #
        # if self.datastatus:
        #     self.datastatus += 1

    def start(self):
        # if self.data0.contractdetails is not None:
        #     print('Timezone from ContractDetails: {}'.format(
        #           self.data0.contractdetails.m_timeZoneId))

        header = ['Datetime', 'Open', 'High', 'Low', 'Close', 'Volume',
                  'OpenInterest', 'SMA']
        print(', '.join(header))
        self.done = False


def parse_args():
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        description='Test Transaq integration')

    parser.add_argument('--exactbars', default=1, type=int,
                        required=False, action='store',
                        help='exactbars level, use 0/-1/-2 to enable plotting')

    parser.add_argument('--plot',
                        required=False, action='store_true',
                        help='Plot if possible')

    parser.add_argument('--stopafter', default=0, type=int,
                        required=False, action='store',
                        help='Stop after x lines of LIVE data')

    parser.add_argument('--usestore',
                        required=False, action='store_true',
                        help='Use the store pattern')

    parser.add_argument('--notifyall',
                        required=False, action='store_true',
                        help='Notify all messages to strategy as store notifs')

    parser.add_argument('--debug',
                        required=False, action='store_true',
                        help='Display all info received form ')

    parser.add_argument('--host', default='127.0.0.1',
                        required=False, action='store',
                        help='Host for the Broker Connection')

    parser.add_argument('--qcheck', default=0.5, type=float,
                        required=False, action='store',
                        help=('Timeout for periodic '
                              'notification/resampling/replaying check'))

    parser.add_argument('--port', default=7496, type=int,
                        required=False, action='store',
                        help='Port for the Interactive Brokers TWS Connection')

    parser.add_argument('--client_id', default=None, type=int,
                        required=False, action='store',
                        help='Client Id to connect')

    parser.add_argument('--no-timeoffset',
                        required=False, action='store_true',
                        help=('Do not Use TWS/System time offset for non '
                              'timestamped prices and to align resampling'))

    parser.add_argument('--reconnect', default=3, type=int,
                        required=False, action='store',
                        help='Number of recconnection attempts to TWS')

    parser.add_argument('--timeout', default=3.0, type=float,
                        required=False, action='store',
                        help='Timeout between reconnection attempts to TWS')

    parser.add_argument('--data0', default=None,
                        required=True, action='store',
                        help='data 0 into the system')

    parser.add_argument('--data1', default=None,
                        required=False, action='store',
                        help='data 1 into the system')

    parser.add_argument('--timezone', default=None,
                        required=False, action='store',
                        help='timezone to get time output into (pytz names)')

    parser.add_argument('--what', default=None,
                        required=False, action='store',
                        help='specific price type for historical requests')

    parser.add_argument('--no-backfill-start',
                        required=False, action='store_true',
                        help='Disable backfilling at the start')

    parser.add_argument('--latethrough',
                        required=False, action='store_true',
                        help=('if resampling replaying, adjusting time '
                              'and disabling time offset, let late samples '
                              'through'))

    parser.add_argument('--no-backfill',
                        required=False, action='store_true',
                        help='Disable backfilling after a disconnection')

    parser.add_argument('--rtbar', default=False,
                        required=False, action='store_true',
                        help='Use 5 seconds real time bar updates if possible')

    parser.add_argument('--historical',
                        required=False, action='store_true',
                        help='do only historical download')

    parser.add_argument('--fromdate',
                        required=False, action='store',
                        help=('Starting date for historical download '
                              'with format: YYYY-MM-DD[THH:MM:SS]'))

    parser.add_argument('--smaperiod', default=5, type=int,
                        required=False, action='store',
                        help='Period to apply to the Simple Moving Average')

    pgroup = parser.add_mutually_exclusive_group(required=False)

    pgroup.add_argument('--replay',
                        required=False, action='store_true',
                        help='replay to chosen timeframe')

    pgroup.add_argument('--resample',
                        required=False, action='store_true',
                        help='resample to chosen timeframe')

    parser.add_argument('--timeframe', default=bt.TimeFrame.Names[0],
                        choices=bt.TimeFrame.Names,
                        required=False, action='store',
                        help='TimeFrame for Resample/Replay')

    parser.add_argument('--compression', default=1, type=int,
                        required=False, action='store',
                        help='Compression for Resample/Replay')

    parser.add_argument('--timeframe1', default=None,
                        choices=bt.TimeFrame.Names,
                        required=False, action='store',
                        help='TimeFrame for Resample/Replay - Data1')

    parser.add_argument('--compression1', default=None, type=int,
                        required=False, action='store',
                        help='Compression for Resample/Replay - Data1')

    parser.add_argument('--no-takelate',
                        required=False, action='store_true',
                        help=('resample/replay, do not accept late samples '
                              'in new bar if the data source let them through '
                              '(latethrough)'))

    parser.add_argument('--no-bar2edge',
                        required=False, action='store_true',
                        help='no bar2edge for resample/replay')

    parser.add_argument('--no-adjbartime',
                        required=False, action='store_true',
                        help='no adjbartime for resample/replay')

    parser.add_argument('--no-rightedge',
                        required=False, action='store_true',
                        help='no rightedge for resample/replay')

    parser.add_argument('--broker',
                        required=False, action='store_true',
                        help='Use IB as broker')

    parser.add_argument('--trade',
                        required=False, action='store_true',
                        help='Do Sample Buy/Sell operations')

    parser.add_argument('--donotsell',
                        required=False, action='store_true',
                        help='Do not sell after a buy')

    parser.add_argument('--exectype', default=bt.Order.ExecTypes[0],
                        choices=bt.Order.ExecTypes,
                        required=False, action='store',
                        help='Execution to Use when opening position')

    parser.add_argument('--stake', default=10, type=int,
                        required=False, action='store',
                        help='Stake to use in buy operations')

    parser.add_argument('--valid', default=None, type=int,
                        required=False, action='store',
                        help='Seconds to keep the order alive (0 means DAY)')

    pgroup = parser.add_mutually_exclusive_group(required=False)
    pgroup.add_argument('--stoptrail',
                        required=False, action='store_true',
                        help='Issue a stoptraillimit after buy( do not sell')

    pgroup.add_argument('--traillimit',
                        required=False, action='store_true',
                        help='Issue a stoptrail after buying (do not sell')

    pgroup.add_argument('--oca',
                        required=False, action='store_true',
                        help='Test oca by putting 2 orders in a group')

    pgroup.add_argument('--bracket',
                        required=False, action='store_true',
                        help='Test bracket orders by issuing high/low sides')

    pgroup = parser.add_mutually_exclusive_group(required=False)
    pgroup.add_argument('--trailamount', default=None, type=float,
                        required=False, action='store',
                        help='trailamount for StopTrail order')

    pgroup.add_argument('--trailpercent', default=None, type=float,
                        required=False, action='store',
                        help='trailpercent for StopTrail order')

    parser.add_argument('--limitoffset', default=None, type=float,
                        required=False, action='store',
                        help='limitoffset for StopTrailLimit orders')

    parser.add_argument('--cancel', default=0, type=int,
                        required=False, action='store',
                        help=('Cancel a buy order after n bars in operation,'
                              ' to be combined with orders like Limit'))

    return parser.parse_args()


if __name__ == '__main__':
    runstrategy()
