"""
Home to exceptions
"""


class TooMuchDataError(ValueError):
    """Raised when the data to be logged is too large."""

    pass


class MissingAckError(ValueError):
    """Raised if we send but don't get an ack back."""

    pass


class UnknownOpCodeError(ValueError):
    """Raised when we get an unknown OpCode."""

    pass


class NoLiveReceiverError(ValueError):
    """Raised when we try to send but there's no live receiver."""

    pass
