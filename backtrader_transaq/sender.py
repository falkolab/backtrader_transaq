from typing import Type

from backtrader_transaq.receiver import Receiver
from transaqpy.client import TransaqClient
from transaqpy.connector import TransaqConnector


class SenderError(Exception):
    pass


class Sender(object):
    """ Encapsulates an EClientSocket instance, and proxies attribute
        lookup to it.
    """
    client = None
    _reconnect = None

    def __init__(self, dispatcher, client):
        """ Initializer.
        @param dispatcher message dispatcher instance
        """
        self.dispatcher = dispatcher
        self.client = client

    def connect(self, host, port, login, password, **kwargs):
        """

        :param self:
        :param host:
        :param port:
        :param login:
        :param password:
        :param kwargs:
        :return: Command execution status, but not connection status!
        """
        def func():
            return self.client.connect(login, password, host, port, **kwargs)
        self._reconnect = func
        return self.reconnect()

    def reconnect(self):
        """

        :return: Command execution status, but not connection status!
        """
        if not self._reconnect:
            raise SenderError('Should be connected at least once before call this method')
        return self._reconnect()

    # def disconnect(self):
    #     """ Disconnects the client.
    #     @return True if disconnected, False otherwise
    #     """
    #     client = self.client
    #     if client and client.is_connected:
    #         return client.disconnect()
    #     return False

    def __getattr__(self, name):
        """ x.__getattr__('name') <==> x.name
        @return named attribute from EClientSocket object
        """
        try:
            value = getattr(self.client, name)
        except (AttributeError, ):
            raise
        # if name not in self.clientMethodNames:
        #     return value
        return value
