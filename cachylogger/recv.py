import math
import socket
import time
from typing import Callable

from cachylogger.constants import POLL_TIME
from cachylogger.timeout import tmp_timeout


def smart_recv(
    sock: socket.socket,
    bufsize: int,
    stop_condition: Callable | None,
    timeout: int | None = None,
) -> None | bytes:
    """
    Our recv is like socket.recv() but it internally will timeout periodically.
    We use the stop_condition to have a way to know when to give-up the recv loop.

    If the recv loop was stopped via the stop_condition, this will return None.
    If the timeout option is given and elapses, this will return None.

    Note that we are not guaranteeing that len(returned_bytes) == bufsize.
    """
    death_time = math.inf
    if timeout is not None:
        death_time = time.time() + timeout

    with tmp_timeout(sock, POLL_TIME):
        while time.time() < death_time:
            if stop_condition and stop_condition():
                break

            try:
                return sock.recv(bufsize)
            except socket.timeout:
                continue
