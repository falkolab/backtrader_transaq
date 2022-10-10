from backtrader.sizers import PercentSizer

from backtrader_transaq.store import TransaqStore


class LotsAwarePercentSizer(PercentSizer):

    def __init__(self):
        pass

    def _getsizing(self, comminfo, cash, data, isbuy):
        position = self.broker.getposition(data)
        store: TransaqStore = data._store

        if not position:
            # if not isbuy:
            #     portfolio = store.get_portfolio()
            #     portfolio.equity

            lotsize = store._securities[str(data.trade_ticker.id)].lotsize
            size = cash / (data.close[0] * lotsize) * (self.params.percents / 100)
        else:
            size = position.size

        if self.p.retint:
            size = int(size)

        return size
