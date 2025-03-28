import socket
import logging
import signal
import sys
from common.utils import store_bets, Bet

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
        self._clients = []

        signal.signal(signal.SIGTERM, self.__handle_shutdown)

    def run(self):
        """
        Dummy Server loop

        Server that accept a new connections and establishes a
        communication with a client. After client with communucation
        finishes, servers starts to accept new connections again
        """

        while True:
            client_sock = self.__accept_new_connection()
            self._clients.append(client_sock)
            self.__handle_client_connection(client_sock)

    def __receive_bet_data(self, sock):
        msg_type = sock.recv(MESS_TYPE_BYTES)  # Read Type (1 byte) + Length (2 bytes)
        if msg_type == END_MESSAGE_TYPE:  # END Message
            return None
        
        header = sock.recv(MESS_LENGTH_BYTES)
        message_length = int.from_bytes(header[0:], "big")
        if message_length == 0:
            return None

        message = b""
        while len(message) < message_length:
            chunk = sock.recv(message_length - len(message))
            if not chunk:
                return None
            message += chunk

        data = message.decode("utf-8").strip().split("\n")
        bets = []
        for bet in data:
            parts = bet.split("|")
            if len(parts) == 5:
                name, surname, id, birthdate, number = parts
                bets.append(Bet(1, name, surname, id, birthdate, number))

        return bets


        
    def __handle_client_connection(self, client_sock):
        """
        Read message from a specific client socket and closes the socket

        If a problem arises in the communication with the client, the
        client socket will also be closed
        """
        while True:
            try:
                bets = self.__receive_bet_data(client_sock)
                if not bets:
                    break
                
                store_bets(bets)
                logging.info(f'action: apuesta_recibida | result: success | cantidad: {len(bets)}')   
            
            except OSError as e:
                logging.error(f"action: receive_message | result: fail | error: {e}")
        
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