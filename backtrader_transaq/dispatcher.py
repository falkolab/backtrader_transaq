import time

from queue import Queue, Empty
import logging

from backtrader_transaq.utils import maybe_name
from transaqpy.structures import STRUCTURE_CLASSES_BY_ROOT_TAG, TransaqMessage

logger = logging.getLogger(__name__)


class Dispatcher(object):
    def __init__(self, listeners=None, message_types=None):
        """ Initializer.

        @param listeners=None mapping of existing listeners
        @param message_types=None method name to message type lookup
        """
        self.listeners = listeners if listeners else {}
        self.message_types = message_types if message_types else STRUCTURE_CLASSES_BY_ROOT_TAG
        self.logger = logger

    def __call__(self, name, args):
        """ Send message to each listener.

        @param name method name
        @param args arguments for message instance
        @return None
        """
        results = []
        try:
            message_type: TransaqMessage = self.message_types[name]
            listeners = self.listeners[message_type.ROOT_NAME]
        except (KeyError,):
            return results

        for listener in listeners:
            try:
                results.append(listener(*args))
            except (Exception,):
                errmsg = ("Exception in message dispatch.  "
                          "Handler '%s' for '%s'")
                self.logger.exception(errmsg, maybe_name(listener), name)
                results.append(None)
        return results

    def enable_logging(self, enable=True):
        """ Enable or disable logging of all messages.

        @param enable if True (default), enables logging; otherwise disables
        @return True if enabled, False otherwise
        """
        if enable:
            self.register_all(self.log_message)
        else:
            self.unregister_all(self.log_message)
        return enable

    def log_message(self, message):
        """ Format and send a message values to the logger.

        @param message instance of Message
        @return None
        """
        line = str.join(', ', ('%s=%s' % item for item in message.items()))
        self.logger.debug('%s(%s)', message.typeName, line)

    def iterator(self, *types):
        """ Create and return a function for iterating over messages.

        @param *types zero or more message types to associate with listener
        @return function that yields messages
        """
        queue = Queue()
        closed = []

        def message_generator(block=True, timeout=0.1):
            while True:
                try:
                    yield queue.get(block=block, timeout=timeout)
                except (Empty,):
                    if closed:
                        break

        self.register(closed.append, 'ConnectionClosed')
        if types:
            self.register(queue.put, *types)
        else:
            self.register_all(queue.put)
        return message_generator

    def register(self, listener, *types):
        """ Associate listener with message types created by this Dispatcher.

        @param listener callable to receive messages
        @param *types zero or more message types to associate with listener
        @return True if associated with one or more handler; otherwise False
        """
        count = 0
        for message_type in types:
            key = maybe_name(message_type)
            listeners = self.listeners.setdefault(key, [])
            if listener not in listeners:
                listeners.append(listener)
                count += 1
        return count > 0

    def register_all(self, listener):
        """ Associate listener with all messages created by this Dispatcher.

        @param listener callable to receive messages
        @return True if associated with one or more handler; otherwise False
        """
        return self.register(listener, *[maybe_name(v.ROOT_NAME) for v in self.message_types.values()])

    def unregister(self, listener, *types):
        """ Disassociate listener with message types created by this Dispatcher.

        @param listener callable to no longer receive messages
        @param *types zero or more message types to disassociate with listener
        @return True if disassociated with one or more handler; otherwise False
        """
        count = 0
        for messagetype in types:
            try:
                listeners = self.listeners[maybe_name(messagetype)]
            except (KeyError,):
                pass
            else:
                if listener in listeners:
                    listeners.remove(listener)
                    count += 1
        return count > 0

    def unregister_all(self, listener):
        """ Disassociate listener with all messages created by this Dispatcher.

        @param listener callable to no longer receive messages
        @return True if disassociated with one or more handler; otherwise False
        """
        return self.unregister(listener, *[maybe_name(v.ROOT_NAME) for v in list(self.message_types.values())])
