import logging
from transaqpy.structures import TransaqMessage
logger = logging.getLogger(__name__)


def make_message_method(name, parameters):
    """ Creates method for dispatching messages.

    @param name name of method as string
    @param parameters list of method argument names
    @return newly created method (as closure)
    """
    def dispatch_method(self, *arguments):
        self.dispatcher(name, dict(zip(parameters, arguments)))
    dispatch_method.__name__ = name
    return dispatch_method


class ReceiverType(type):
    """ Metaclass to add EWrapper methods to Receiver class.

    When the Receiver class is defined, this class adds all of the
    wrapper methods to it.
    """
    def __new__(cls, name, bases, namespace):
        """ Creates a new type.

        @param name name of new type as string
        @param bases tuple of base classes
        @param namespace dictionary with namespace for new type
        @return generated type
        """
        return type(name, bases, namespace)


class Receiver:
    __metaclass__ = ReceiverType

    def __init__(self, dispatcher):
        self.dispatcher = dispatcher

    def __call__(self, message: TransaqMessage):
        self.dispatcher(message.ROOT_NAME, [message])
