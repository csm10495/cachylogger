from __future__ import annotations

import socket
from dataclasses import dataclass
from typing import Callable

from cachylogger.constants import DATA_LEN_SIZE_IN_BYTES, MAX_DATA_SIZE, OpCode
from cachylogger.exceptions import TooMuchDataError
from cachylogger.recv import smart_recv


@dataclass
class Message:
    """
    Abstraction for a message to be sent/recv over the socket.
    """

    op_code: OpCode
    data: bytes

    # If we didn't recv all the data specified, mark this as False.
    data_complete: bool = True

    def to_bytes(self) -> bytes:
        """
        Returns the wire-bytes for this message.

        This is DATA_LEN_SIZE_IN_BYTES bytes for the data length, followed by the op_code byte, followed by the data.
        The data length includes the op_code byte and following data.
        """
        op_and_data = self.op_code.value + self.data
        data_len = len(op_and_data)
        if data_len > MAX_DATA_SIZE:
            raise TooMuchDataError(
                f"data is too long. Size = {data_len}.. Max size: {MAX_DATA_SIZE}"
            )

        data_len_as_bytes = data_len.to_bytes(
            DATA_LEN_SIZE_IN_BYTES, byteorder="little"
        )
        return data_len_as_bytes + op_and_data

    @classmethod
    def from_socket_recv(
        cls,
        sock: socket.socket,
        stop_condition: Callable | None = None,
        recv_timeout: int | None = None,
    ) -> Message:
        """
        Performs a couple recvs on the socket to get a corresponding Message.

        stop_condition is a Callable used to return True when we should stop trying to recv.
        recv_timeout is the timeout to use for each recv call.
        """
        data_len_bytes = smart_recv(
            sock, DATA_LEN_SIZE_IN_BYTES, stop_condition, timeout=recv_timeout
        )
        if not data_len_bytes:
            return IncompleteMessage

        data_len = int.from_bytes(data_len_bytes, byteorder="little")
        op_and_data = smart_recv(sock, data_len, stop_condition, timeout=recv_timeout)
        if not op_and_data:
            return IncompleteMessage

        data_complete = len(op_and_data) == data_len

        op = op_and_data[:1]
        data = op_and_data[1:]
        return cls(OpCode(op), data, data_complete=data_complete)

    def send_message(self, sock: socket.socket):
        """
        Sends this message's bytes over the socket.
        """
        sock.sendall(self.to_bytes())


AckMessage = Message(op_code=OpCode.ACK, data=b"", data_complete=True)

# A special message that means it wasn't fully received
IncompleteMessage = Message(op_code=OpCode.NONE, data=b"", data_complete=False)
