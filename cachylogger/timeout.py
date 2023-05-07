import contextlib
import socket


@contextlib.contextmanager
def tmp_timeout(sock: socket.socket, timeout: float):
    """
    Temporarily change the timeout of a socket
    """
    old_timeout = sock.gettimeout()
    try:
        sock.settimeout(timeout)
        yield
    finally:
        sock.settimeout(old_timeout)
