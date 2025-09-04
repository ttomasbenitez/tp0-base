import socket
import logging
import signal
import sys
from common.utils import store_bets, Bet

EXPECTED_BET_FIELDS = 5

DATA_MESSAGE_TYPE = b"\x01"
END_MESSAGE_TYPE = b"\x02"

MESS_TYPE_BYTES = 1
MESS_LENGTH_BYTES = 2

class Server:
    def __init__(self, port, listen_backlog):
        # Initialize server socket
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind(('', port))
        self._server_socket.listen(listen_backlog)
        self._running = True
        self._clients = []

        signal.signal(signal.SIGTERM, self.__handle_shutdown)

    def run(self):
        """
        Dummy Server loop

        Server that accept a new connections and establishes a
        communication with a client. After client with communucation
        finishes, servers starts to accept new connections again
        """

        while self._running:
            try:
                client_sock = self.__accept_new_connection()
                self._clients.append(client_sock)
                self.__handle_client_connection(client_sock)
            except OSError as e:
                if self._running:
                    logging.error(f'action: accept_connections | result: fail | error: {e}')
                else:
                    break

    def _recv_all(self, sock, nbytes):
        """Read exactly nbytes from socket, handling short reads."""
        data = b""
        while len(data) < nbytes:
            chunk = sock.recv(nbytes - len(data))
            if not chunk:
                return None
            data += chunk
        return data

    def __receive_bet_data(self, sock):
        msg_type = self._recv_all(sock, MESS_TYPE_BYTES)
        if msg_type is None or msg_type == END_MESSAGE_TYPE:
            return None
        
        header = self._recv_all(sock, MESS_LENGTH_BYTES)
        if header is None:
            return None
        message_length = int.from_bytes(header, "big")
        if message_length == 0:
            return None

        message = self._recv_all(sock, message_length)
        if message is None:
            return None

        return self._parse_bets(message.decode("utf-8").strip())

    def _parse_bets(self, bets_str):
        bets = []
        for bet in bets_str.split("\n"):
            parts = bet.split("|")
            if len(parts) == EXPECTED_BET_FIELDS:
                name, surname, id, birthdate, number = parts
                bets.append(Bet(1, name, surname, id, birthdate, number))
        return bets

    def __handle_client_connection(self, client_sock):
        """
        Read message from a specific client socket and closes the socket

        If a problem arises in the communication with the client, the
        client socket will also be closed
        """
        try:
            while True:
                try:
                    bets = self.__receive_bet_data(client_sock)
                    if not bets:
                        break
                    store_bets(bets)
                    logging.info(f'action: apuesta_recibida | result: success | cantidad: {len(bets)}')
                except OSError as e:
                    logging.error(f"action: receive_message | result: fail | error: {e}")
                    break
            try:
                client_sock.sendall(END_MESSAGE_TYPE)
            except Exception as e:
                logging.error(f"action: send_end_message | result: fail | error: {e}")
        finally:
            client_sock.close()
            if client_sock in self._clients:
                self._clients.remove(client_sock)

    def __accept_new_connection(self):
        """
        Accepts new connections, with logging and error handling.
        """
        logging.info('action: accept_connections | result: in_progress')
        c, addr = self._server_socket.accept()
        logging.info(f'action: accept_connections | result: success | ip: {addr[0]}')
        return c

    def __handle_shutdown(self, signum, frame):
        self._running = False
        for client in self._clients:
            client.close()
        self._server_socket.close()
        logging.info(f'action: server shutdown | result: success')