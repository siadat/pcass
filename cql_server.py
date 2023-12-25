
import argparse
import socket
import threading
import sstable.utils

def handle_client_connection(client_socket):
    try:
        while True:
            request = client_socket.recv(1024)
            print("Received", len(request), "bytes")
            bytes_in_hex = [sstable.utils.hex(i) for i in request]

            # enumerate each byte in the request
            for i, byte in enumerate(request):
                print(str(i) + "	")
                print(sstable.utils.byte_repr(byte))

            # Here, you would add the logic to parse the CQL binary protocol
            # and send an appropriate response.
            # For now, we just send back a simple message.
            client_socket.send("ACK".encode())
    except Exception as e:
        print("An error occurred:", str(e))
    finally:
        client_socket.close()


def start_tcp_server(ip, port):
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind((ip, port))
    server.listen(5)  # max backlog of connections

    print("Listening on", ip, ":", port)
    while True:
        client_sock, address = server.accept()
        print("Accepted connection from", address[0], ":", address[1])
        client_handler = threading.Thread(
            target=handle_client_connection, args=(client_sock,)
        )
        client_handler.start()


parser = argparse.ArgumentParser()
parser.add_argument("-p", "--port", help="port to bind", type=int, default=9042)
args = parser.parse_args()

if __name__ == "__main__":
    start_tcp_server("0.0.0.0", args.port)

