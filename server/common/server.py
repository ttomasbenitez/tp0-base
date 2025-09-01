import socket
import logging
import signal
import sys
from common.utils import store_bets, Bet

class Server:
    def __init__(self, port, listen_backlog):
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind(('', port))
        self._server_socket.listen(listen_backlog)
        self._clients = []

        signal.signal(signal.SIGTERM, self.__handle_shutdown)

    def run(self):
        while True:
            client_sock = self.__accept_new_connection()
            self._clients.append(client_sock)
            self.__handle_client_connection(client_sock)

    def __recv_all(self, sock, n):
        data = b""
        while len(data) < n:
            packet = sock.recv(n - len(data))
            if not packet:
                raise ConnectionError("Cliente cerró la conexión antes de enviar todos los datos")
            data += packet
        return data

    def __receive_bet_data(self, sock):
        length_byte = self.__recv_all(sock, 1)
        message_length = ord(length_byte)
        message_bytes = self.__recv_all(sock, message_length)
        parts = message_bytes.decode('utf-8').strip().split('|')
        if len(parts) == 5:
            name, surname, id, birthdate, number = parts
            return Bet(0, name, surname, id, birthdate, number)
        return None

    def __handle_client_connection(self, client_sock):
        try:
            bet = self.__receive_bet_data(client_sock)
            if not bet:
                logging.error("action: apuesta_almacenada | result: fail")
                return

            addr = client_sock.getpeername()
            store_bets([bet])
            logging.info(f'action: apuesta_almacenada | result: success | ip: {addr[0]} | dni: {bet.document}')
            
            # Sendall ya asegura envío completo
            client_sock.sendall("{}\n".format(bet.number).encode('utf-8'))
        except (OSError, ConnectionError) as e:
            logging.error(f"action: client_communication | result: fail | error: {e}")
        finally:
            client_sock.close()
            if client_sock in self._clients:
                self._clients.remove(client_sock)

    def __accept_new_connection(self):
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
