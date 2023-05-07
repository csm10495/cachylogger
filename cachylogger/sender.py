"""
Home to the Sender class.
"""

import json
import logging
import socket
from dataclasses import dataclass, field

import backoff

from cachylogger.constants import RECONNECT_ERRORS, OpCode
from cachylogger.exceptions import MissingAckError
from cachylogger.message import AckMessage, Message

RETRY_SEND_ERRORS = (MissingAckError,)


@dataclass
class Sender:
    """
    The sender is responsible for sending messages to the receiver.

    Internally it manages a tcp socket that connects to the receiver on a given host/port.
    """

    port: int
    host: str = "localhost"

    _current_socket: socket.socket | None = field(default=None, init=False, repr=False)

    def _get_socket(self) -> socket.socket:
        """Returns either the current socket or creates a new one if it is currently set to None"""
        if self._current_socket is None:
            self._current_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self._current_socket.connect((self.host, self.port))

        return self._current_socket

    def _close_socket(self) -> None:
        """
        Closes the current socket if it exists and sets it to None.
        Raises a ValueError if the current socket is None
        """
        if self._current_socket is not None:
            self._current_socket.close()
            self._current_socket = None
        else:
            raise ValueError("Attempting to close a non-existant socket")

    def _recv_ack(self):
        """
        Attempts to read an AckMessage from the current socket.

        If an AckMessage is not read, will raise a MissingAckError.
        """
        # note that this can raise RECONNECT_ERRORS.. should be caught and retried higher up
        should_be_ack = Message.from_socket_recv(self._get_socket(), recv_timeout=1)

        if should_be_ack != AckMessage:
            raise MissingAckError(f"Expected an ack message.. but got: {should_be_ack}")

    @backoff.on_exception(backoff.expo, RECONNECT_ERRORS, max_tries=5)
    def send_message(self, msg: Message):
        """
        Attempts to send the given message, and then wait for an ack.
        """
        try:
            msg.send_message(self._get_socket())
            self._recv_ack()
        except RECONNECT_ERRORS:
            self._close_socket()
            raise

    def send_log_record(self, record: logging.LogRecord):
        """
        Sends a logging.LogRecord via the socket
        """
        msg = Message(op_code=OpCode.JSON, data=json.dumps(vars(record)).encode())
        self.send_message(msg)

    def has_live_receiver(self) -> bool:
        """
        Returns True if we appear to be connected to a live receiver. Note that this
        function may internally disconnect/reconnect as needed to attempt connection.

        Internally this sends an Ack and looks for an Ack to come back.
        """
        try:
            self.send_message(AckMessage)
            return True
        except RECONNECT_ERRORS:
            return False


if __name__ == "__main__":
    s = Sender(9999)
    j = Message(op_code=OpCode.JSON, data=b"hello world")
