from backtrader.btrun.btrun import DATAFORMATS
# from backtrader_transaq.connection import Connection
from backtrader_transaq.data import TransaqData

try:
    from .broker import TransaqBroker
except ImportError as e:
    pass


try:
    DATAFORMATS['transaqdata'] = TransaqData
except AttributeError:
    pass  # no comtypes available

# transaq_connection = Connection.create
