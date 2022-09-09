from transaqpy.client import TransaqClient
from .dispatcher import Dispatcher
from .receiver import Receiver
from .sender import Sender


class Connection:
    def __init__(self, host, port, login, password, receiver, sender, dispatcher, **kwargs):
        self.host = host
        self.port = port
        self.login = login
        self.password = password
        self.receiver = receiver
        self.sender = sender
        self.dispatcher = dispatcher
        self.kwargs = kwargs

    def __getattr__(self, name):
        """ x.__getattr__('name') <==> x.name
        @return attribute of instance dispatcher, receiver, or sender
        """
        for obj in (self.dispatcher, self.receiver, self.sender):
            try:
                return getattr(obj, name)
            except (AttributeError, ):
                pass
        err = "'%s' object has no attribute '%s'"
        raise AttributeError(err % (self.__class__.__name__, name))

    def connect(self, **kwargs) -> bool:
        kw = {**self.kwargs, **kwargs}
        return self.sender.connect(self.host, self.port, self.login, self.password, **kw)

    @classmethod
    def create(cls, transaq_connector, login, password, host='localhost', port=7496,
               receiver=None, sender=None, dispatcher=None):
        dispatcher = Dispatcher() if dispatcher is None else dispatcher
        receiver = Receiver(dispatcher) if receiver is None else receiver
        client = TransaqClient(receiver, transaq_connector)
        sender = Sender(dispatcher, client) if sender is None else sender
        return cls(host, port, login, password, receiver, sender, dispatcher)
