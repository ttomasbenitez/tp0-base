import socket
import logging
import signal
import sys
from common.utils import store_bets, Bet

class Server:
    def __init__(self, port, listen_backlog):
        # Initialize server socket
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind(('', port))
        self._server_socket.listen(listen_backlog)
        self._clients = []

        signal.signal(signal.SIGTERM, self.__handle_shutdown)

    def run(self):
        """
        Dummy Server loop

        Server that accept a new connections and establishes a
        communication with a client. After client with communucation
        finishes, servers starts to accept new connections again
        """

        # TODO: Modify this program to handle signal to graceful shutdown
        # the server
        while True:
            client_sock = self.__accept_new_connection()
            self._clients.append(client_sock)
            self.__handle_client_connection(client_sock)

    def __receive_bet_data(self, sock):
        length_byte = sock.recv(1)

        message_length = ord(length_byte)

        parts = sock.recv(message_length).decode('utf-8').strip().split('|')
        if len(parts) == 5:
            name, surname, id, birthdate, number = parts
            return Bet(0, name, surname, id, birthdate, number)
        return None

    def __handle_client_connection(self, client_sock):
        """
        Read message from a specific client socket and closes the socket

        If a problem arises in the communication with the client, the
        client socket will also be closed
        """
        try:
            bet = self.__receive_bet_data(client_sock)
            if not bet:
                logging.error("action: apuesta_almacenada | result: fail")
                return
            addr = client_sock.getpeername()
            store_bets([bet])
            logging.info(f'action: apuesta_almacenada | result: success | ip: {addr[0]} | dni: {bet.document}')
            
            # TODO: Modify the send to avoid short-writes
            client_sock.send("{}\n".format(bet.number).encode('utf-8'))
        except OSError as e:
            logging.error("action: receive_message | result: fail | error: {e}")
        finally:
            client_sock.close()
            self._clients.remove(client_sock)

    def __accept_new_connection(self):
        """
        Accept new connections

        Function blocks until a connection to a client is made.
        Then connection created is printed and returned
        """

        # Connection arrived
        logging.info('action: accept_connections | result: in_progress')
        c, addr = self._server_socket.accept()
        logging.info(f'action: accept_connections | result: success | ip: {addr[0]}')
        return c
    
    def __handle_shutdown(self, signum, frame):
        for client in self._clients:
            client.close()
            
        self._server_socket.close()
        logging.info(f'action: server shutdown | result: success')
        sys.exit(0)