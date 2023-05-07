from enum import Enum

from cachylogger.exceptions import MissingAckError

MAX_DATA_SIZE = 0xFFFFFFFF
DATA_LEN_SIZE_IN_BYTES = (MAX_DATA_SIZE.bit_length() + 7) // 8
RECONNECT_ERRORS = (
    ConnectionResetError,
    ConnectionRefusedError,
    ConnectionAbortedError,
    OSError,
    MissingAckError,
)
POLL_TIME = 0.1


class OpCode(Enum):
    """
    An OpCode is an operation code for a given message. It tells us what the message is attempting to do.
    """

    JSON = b"j"
    ACK = b"k"
    NONE = None
