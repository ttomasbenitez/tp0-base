import socket
import logging
import signal
import sys
import multiprocessing
from common.utils import store_bets, load_bets, has_won, Bet

EXPECTED_BET_FIELDS = 6

DATA_MESSAGE_TYPE = b"\x01"
END_MESSAGE_TYPE = b"\x02"
ASK_WINNER_TYPE = b"\x03"

MESS_TYPE_BYTES = 1
MESS_LENGTH_BYTES = 2

class ConnectionClosedException(Exception):
    """Exception raised when a client connection is closed unexpectedly."""
    pass

class Server:
    def __init__(self, port, listen_backlog, clients_amount):
        """
        Initializes the server, binds the socket to the given port, and sets up shared resources and locks.
        """
        # Initialize server socket
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind(('', port))
        self._server_socket.listen(listen_backlog)
        self._running = True
        self._processes = []
        self._clients_amount = clients_amount
        self._clients = []

        # Shared resources
        manager = multiprocessing.Manager()
        self._agencies_ids = manager.dict()

        # Locks for specific resources
        self._file_lock = multiprocessing.Lock()           # For file operations (store_bets/load_bets)
        self._agencies_ids_lock = multiprocessing.Lock()    # For _agencies_ids dict
    
        signal.signal(signal.SIGTERM, self.__handle_shutdown)

    def run(self):
        """
        Main server loop to accept new client connections and handle them in separate processes.
        Synchronizes all processes using a barrier.
        """
        sendWinnersBarrier = multiprocessing.Barrier(self._clients_amount)
        
        while self._running and len(self._clients) < self._clients_amount:
            try:
                client_sock = self.__accept_new_connection()
                self._clients.append(client_sock)

                # Start a new process to handle each client
                p = multiprocessing.Process(target=self.__handle_client_connection, args=(client_sock, sendWinnersBarrier))
                p.start()
                self._processes.append(p)
            except OSError as e:
                if self._running:
                    logging.error(f'action: accept_connections | result: fail | error: {e}')
                else:
                    break

        self.__handle_shutdown(None, None)


    def __recv_all(self, sock, size):
        """
        Helper function to ensure that the exact number of bytes is received.
        Raises ConnectionClosedException if the connection is closed.
        """
        data = b""
        while len(data) < size:
            chunk = sock.recv(size - len(data))
            if not chunk:
                logging.error('action: connection_ended | result: fail')
                raise ConnectionClosedException("Client connection closed unexpectedly")
            data += chunk
        return data

    def __parse_bet_data(self, message):
        """
        Parses bet data from the received message and returns a list of Bet objects.
        """
        data = message.decode("utf-8").strip().split("\n")
        bets = []
        for bet in data:
            parts = bet.split("|")
            if len(parts) == EXPECTED_BET_FIELDS:
                agency, name, surname, id, birthdate, number = parts
                bets.append(Bet(agency, name, surname, id, birthdate, number))
        return bets

    def __receive_bet_data(self, sock):
        """
        Receives bet data from the client. Returns a list of Bet objects or None if no data is received.
        Ensures no short reads by receiving the entire message.
        """
        msg_type = self.__recv_all(sock, MESS_TYPE_BYTES)
        if msg_type == END_MESSAGE_TYPE:
            logging.debug(f'action: END_MESS_RECEIVED | result: success')   
            return None
        
        header = self.__recv_all(sock, MESS_LENGTH_BYTES)
        message_length = int.from_bytes(header[0:], "big")
        if message_length == 0:
            return None

        message = self.__recv_all(sock, message_length)
        return self.__parse_bet_data(message)

    def __process_bets(self, client_sock):
        """
        Process all incoming bet data from client.
        """
        while True:
            try:
                bets = self.__receive_bet_data(client_sock)
                if not bets:
                    break

                with self._file_lock:
                    store_bets(bets)

                logging.info(f'action: apuesta_recibida | result: success | cantidad: {len(bets)}')

                with self._agencies_ids_lock:
                    self._agencies_ids.setdefault(client_sock.fileno(), bets[0].agency)

            except (OSError, ConnectionClosedException) as e:
                logging.error(f"action: receive_message | result: fail | error: {e}")
                raise  # Re-raise to let the parent handle it

    def __check_for_winner_request(self, client_sock, sendWinnersBarrier):
        """
        Waits for the winner request from the client and ensures all processes reach the barrier before continuing.
        """
        try:
            msg_type = client_sock.recv(1)
            if msg_type == ASK_WINNER_TYPE:
                sendWinnersBarrier.wait()  # Ensure synchronization between all processes
                self.__send_winners(client_sock)
        except OSError as e:
            logging.error(f"action: receive_message | result: fail | error: {e}")
            raise

    def __handle_client_connection(self, client_sock, sendWinnersBarrier):
        """
        Handles communication with a client: receives bet data, stores it, and waits for the barrier to send winners.
        """
        try:
            self.__process_bets(client_sock)
            self.__check_for_winner_request(client_sock, sendWinnersBarrier)
        except (OSError, ConnectionClosedException) as e:
            logging.error(f"action: handle_client | result: fail | error: {e}")
        finally:
            client_sock.close()

    def __send_winner_to_client(self, client_sock, winner_bet):
        """
        Sends a single winner bet to the client.
        Uses sendall to handle short writes automatically.
        """
        message = DATA_MESSAGE_TYPE + f"{winner_bet.document}\n".encode('utf-8')
        client_sock.sendall(message)

    def __send_end_message(self, client_sock):
        """
        Send end message to client.
        """
        client_sock.sendall(END_MESSAGE_TYPE)

    def __send_winners(self, client_sock):
        """
        Sends the winners to the requesting client after processing all bets.
        Uses sendall to handle short writes automatically.
        """
        logging.info(f'action: sorteo | result: success')

        with self._file_lock:
            bets = load_bets()
        
        with self._agencies_ids_lock:
            agency_id = self._agencies_ids[client_sock.fileno()]

        agency_winner_bets = [bet for bet in bets if has_won(bet) and bet.agency == agency_id]
        
        for winner_bet in agency_winner_bets:
            self.__send_winner_to_client(client_sock, winner_bet)
        
        self.__send_end_message(client_sock)

    def __accept_new_connection(self):
        """
        Accepts new client connections and returns the client socket.
        """
        logging.info('action: accept_connections | result: in_progress')
        c, addr = self._server_socket.accept()
        logging.info(f'action: accept_connections | result: success | ip: {addr[0]}')
        return c
    
    def __handle_shutdown(self, signum, frame):
        """
        Closes all client connections and shuts down the server.
        """
        self._running = False
        for client in self._clients:
            client.close()

        # Wait for all client handler processes to finish
        for p in getattr(self, "_processes", []):
            p.join()

        self._server_socket.close()
        logging.info(f'action: server shutdown | result: success')