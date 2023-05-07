import logging
import sys
import traceback
from collections import defaultdict
from queue import Queue
from threading import RLock, Thread

from cachylogger.exceptions import NoLiveReceiverError
from cachylogger.sender import Sender

QUEUE_MAX_SIZE = 10000


class CachyLoggerHandler(logging.Handler):
    """
    A handler to send messages to cachylogger.
    """

    _queues = defaultdict(lambda: Queue(QUEUE_MAX_SIZE))
    _senders = {}
    _producer_threads = {}
    # idx is used to have an incrementing value for records in case a timestamp for two in a row match.
    _idx: int = 0
    _idx_lock = RLock()
    _get_sender_lock = RLock()

    def __init__(
        self,
        level=logging.NOTSET,
        port: int = 9999,
        host: str = "localhost",
        do_async: bool = True,
        block: bool = True,
    ):
        """
        Initializer for CachyLoggerHandler.

        do_async will have each log message get sent to a queue to be processed via a producer thread.
        When block is True, this will wait until the message has made it into the queue. If block is False,
        a queue.Full error may be raised.
        """
        super().__init__(level=level)

        self.do_async = do_async

        self._port = port
        self._host = host
        self._block = block

        if self.do_async:
            if (host, port) not in self._producer_threads or not self._producer_threads[
                (host, port)
            ].is_alive():
                self._producer_threads[(host, port)] = Thread(
                    target=self._producer, args=(host, port), daemon=True
                )
                self._producer_threads[(host, port)].start()

    @classmethod
    def _get_queue(cls, host: str, port: int) -> Queue:
        """
        Gets the queue for the given host/port
        """
        return cls._queues[(host, port)]

    @classmethod
    def _get_sender(cls, host: str, port: int) -> Sender:
        """
        Gets the sender for the given host/port
        """
        with cls._get_sender_lock:
            if (host, port) not in cls._senders:
                s = Sender(host=host, port=port)
                if not s.has_live_receiver():
                    raise NoLiveReceiverError(
                        f"No live receiver founder on {host}:{port}"
                    )
                cls._senders[(host, port)] = s

            return cls._senders[(host, port)]

    @classmethod
    def _producer(cls, host: str, port: int):
        """
        Ran inside a thread to pop messages from the queue and pass them one-by-one to a sender.
        """
        did_log_err = False
        while True:
            try:
                queue = cls._get_queue(host, port)
                sender = cls._get_sender(host, port)
                break
            except Exception:
                if not did_log_err:
                    print(
                        f"Error in CachyLoggerHandler's producer thread (setup).. only logging this once.:\n{traceback.format_exc()}",
                        file=sys.stderr,
                    )
                    did_log_err = True

        did_log_err = False
        while True:
            try:
                record = queue.get()
                sender.send_log_record(record)
                did_log_err = False
            except Exception:
                if not did_log_err:
                    print(
                        f"Error in CachyLoggerHandler's producer thread (loop).. only logging this once until it passes.:\n{traceback.format_exc()}",
                        file=sys.stderr,
                    )
                    did_log_err = True

    def emit(self, record: logging.LogRecord):
        """
        Called to emit the given logging record.
        """
        with self._idx_lock:
            record._cl_idx = self._idx
            self._idx += 1

        if self.do_async:
            self._get_queue(self._host, self._port).put(record, block=self._block)
        else:
            self._get_sender(self._host, self._port).send_log_record(record)


if __name__ == "__main__":
    log = logging.getLogger(__name__)
    log.setLevel(logging.DEBUG)
    log.addHandler(CachyLoggerHandler())
