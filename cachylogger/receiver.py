"""
Home to the Receiver class.
"""

import socket
import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field

from cachylogger.constants import POLL_TIME, RECONNECT_ERRORS
from cachylogger.message import AckMessage, Message


@dataclass
class Receiver:
    """
    The receiver is responsible for receiving messages from the sender.

    Internally it starts with a tcp socket and then on each accept, sends it to a thread for
    further processing.
    """

    port: int
    host: str = "localhost"
    max_connections: int = 8

    is_stopping: threading.Event = field(
        default=threading.Event(), init=False, repr=False
    )

    def start(self):
        """
        Starts the receiver. This will block until the receiver is stopped.
        """
        self.is_stopping.clear()
        with ThreadPoolExecutor(max_workers=self.max_connections) as executor:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                sock.bind((self.host, self.port))
                sock.listen()
                # set a timeout so we can keyboard interrupt if needed
                sock.settimeout(POLL_TIME)
                while True:
                    try:
                        conn, _ = sock.accept()
                    except socket.timeout:
                        continue
                    executor.submit(self._handle_connection, conn)
            finally:
                sock.close()
                self.is_stopping.set()

    def _handle_connection(self, conn: socket.socket):
        """
        Each new connection will be handled in a new worker via this function.
        """
        try:
            peer = conn.getpeername()
            print(f"Connected to: {peer}")
            while True:
                msg = Message.from_socket_recv(conn, self.is_stopping.is_set)
                if msg.data_complete:
                    self._process_message(msg)
                    AckMessage.send_message(conn)
                else:
                    print("Partial message?")
                    # an incorrect amount of data was sent that didn't match the first 4 bytes
                    break
        except (*RECONNECT_ERRORS, KeyboardInterrupt):
            pass
        finally:
            print(f"Disconnected from: {peer}")
            conn.close()

    def _process_message(self, msg: Message):
        """
        Called to process/handle a single message
        """
        print(msg)


if __name__ == "__main__":
    r = Receiver(9999)
    r.start()
