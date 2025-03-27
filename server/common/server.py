import socket
import logging
import signal
import sys
import multiprocessing
from common.utils import store_bets, load_bets, has_won, Bet

DATA_MESSAGE_TYPE = b"\x01"
END_MESSAGE_TYPE = b"\x02"
ASK_WINNER_TYPE = b"\x03"

class Server:
    def __init__(self, port, listen_backlog, clients_amount):
        # Initialize server socket
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind(('', port))
        self._server_socket.listen(listen_backlog)
        self._clients_amount = clients_amount
        self._waiting_clients = multiprocessing.Manager().list()
        self._clients = multiprocessing.Manager().dict()
        self._current_clients_count = 0

        signal.signal(signal.SIGTERM, self.__handle_shutdown)

    def run(self):
        """
        Dummy Server loop

        Server that accept new connections and establishes a
        communication with a client. After client with communication
        finishes, servers starts to accept new connections again
        """
        processes = []
        while len(self._waiting_clients) < self._clients_amount and self._current_clients_count < self._clients_amount:
            client_sock = self.__accept_new_connection()
            self._current_clients_count += 1

            # Start a new process to handle the client
            p = multiprocessing.Process(target=self.__handle_client_connection, args=(client_sock,))
            p.start()
            processes.append(p)

        # Wait for all processes to finish
        for p in processes:
            p.join()

        self.__send_winners(self._waiting_clients)

    def __receive_bet_data(self, sock):
        msg_type = sock.recv(1)  # Read Type (1 byte) + Length (2 bytes)
        if msg_type == END_MESSAGE_TYPE:
            logging.debug(f'action: END_MESS_RECEIVED | result: success')   
            return None
        header = sock.recv(2)
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
            if len(parts) == 6:
                agency, name, surname, id, birthdate, number = parts
                bets.append(Bet(agency, name, surname, id, birthdate, number))
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
                    logging.debug(f'action: BREAK | result: success')
                    break
                
                store_bets(bets)
                self._clients[bets[0].agency] = client_sock
                logging.info(f'action: apuesta_recibida | result: success | cantidad: {len(bets)}')   
            
            except OSError as e:
                logging.error(f"action: receive_message | result: fail | error: {e}")

        try:
            msg_type = client_sock.recv(1)
            if msg_type == ASK_WINNER_TYPE:
                self._waiting_clients.append(client_sock)
        except OSError as e:
            logging.error(f"action: receive_message | result: fail | error: {e}")
    
    def __send_winners(self, clients_socks):
        logging.info(f'action: sorteo | result: success')
        bets = load_bets()
        winner_bets = [bet for bet in bets if has_won(bet)]
        
        for winner_bet in winner_bets:
            client_sock = self._clients[winner_bet.agency]
            self.__send_winner_to_client(client_sock, winner_bet)
        
        for client_id, client_sock in self._clients.items():
            client_sock.send(END_MESSAGE_TYPE)

    def __send_winner_to_client(self, client_sock, winner_bet):
        """Send winner document to a specific client"""
        client_sock.send(DATA_MESSAGE_TYPE + f"{winner_bet.document}\n".encode('utf-8'))
        logging.debug(f"action: send_winner_to_client | result: success | winner_bet: {winner_bet}")

    def __accept_new_connection(self):
        """
        Accept new connections

        Function blocks until a connection to a client is made.
        Then connection created is printed and returned
        """
        logging.info('action: accept_connections | result: in_progress')
        c, addr = self._server_socket.accept()
        logging.info(f'action: accept_connections | result: success | ip: {addr[0]}')
        return c
    
    def __handle_shutdown(self, signum, frame):
        for client in self._clients.values():
            client.close()
            
        self._server_socket.close()
        logging.info(f'action: server shutdown | result: success')
        sys.exit(0)
