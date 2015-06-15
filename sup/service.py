import threading
from multiprocessing import connection


class Client():
    """
    Simple wrapper around `multiprocessing`'s `Client` class.
    """
    def __init__(self, host='localhost', port=6001, authkey=b'password'):
        self.conn = connection.Client((host, port), authkey=authkey)

    def send(self, message):
        self.conn.send(message)
        return self.conn.recv()

    def close(self):
        self.conn.close()


class Service():
    """
    To define a custom service, inherit from this class and override the `handle` method.
    If you override the `__init__` method, be sure to call `super().__init__()` (optionally with args)
    at the start.
    """
    def __init__(self, host='localhost', port=6001, authkey=b'password'):
        self.address = (host, port)
        self.authkey = authkey

    def run(self):
        print('Running sevice at {}'.format(self.address))
        with connection.Listener(self.address, authkey=self.authkey) as listener:
            while True:
                conn = listener.accept()
                addr = listener.last_accepted
                self.start_thread(conn, addr)
                print('Connection accepted from {}'.format(addr))

    def start_thread(self, conn, address):
        import logging
        logging.basicConfig(level=logging.DEBUG)
        logger = logging.getLogger('process-{}'.format(address[1]))
        server_thread = threading.Thread(target=self._handle, args=(conn, logger))

        # Exit the server thread when the main thread terminates
        server_thread.daemon = True
        server_thread.start()

        print('Server loop running in thread:', server_thread.name)

    def _handle(self, conn, logger):
        while True:
            try:
                self.handle(conn)
            except Exception as e:
                logger.exception("Problem handling request: {}".format(e))
            finally:
                logger.debug("Closing socket")
                conn.close()
                break

    def handle(self, conn):
        msg = conn.recv()
        conn.send(msg)
