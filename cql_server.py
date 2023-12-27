import os
import io
import argparse
import socket
import threading
import sstable.utils
import cql_struct
import sstable.positioned_construct

def handle_REGISTER(parsed_request):
    return b"NOT IMPLEMENTED"

def handle_STARTUP(parsed_request):
    request_body = cql_struct.string_map.parse(parsed_request.body)
    print("received startup", request_body)
    return cql_struct.frame.build({
        "version": 0x04 | cql_struct.RESPONSE_FLAG,
        "flags": 0x00,
        "stream": parsed_request.stream,
        "opcode": cql_struct.OpCode.READY,
        "length": 0,
        "body": b"",
    })

def handle_OPTIONS(parsed_request):
    if parsed_request.version != 0x04:
        body = cql_struct.string_multimap.build({
            "count": 2,
            "keyvals": [
                {
                    "length": len("CQL_VERSION"),
                    "key": "CQL_VERSION",
                    "values": {
                        "count": 1,
                        "strings": [
                            {
                                "length": len("3.4.0"),
                                "string": "3.4.0",
                            },
                        ],
                    },
                },
                {
                    "length": len("COMPRESSION"),
                    "key": "COMPRESSION",
                    "values": {
                        "count": 1,
                        "strings": [
                            {
                                "length": len("snappy"),
                                "string": "snappy",
                            },
                        ],
                    },
                },
            ],
        })

        return cql_struct.frame.build({
            "version": 0x04 | cql_struct.RESPONSE_FLAG,
            "flags": 0x00,
            "stream": 0x0000,
            "opcode": cql_struct.OpCode.SUPPORTED,
            "length": len(body),
            "body": body,
        })
    else:
        message = f'Invalid or unsupported protocol version ({parsed_request.version}); the lowest supported version is 3 and the greatest is 4'
        body = cql_struct.error.build({
            "code": cql_struct.ErrorCode.PROTOCOL_ERROR,
            "length": len(message),
            "message": message,
        })
        return cql_struct.frame.build({
            "version": 0x04 | cql_struct.RESPONSE_FLAG,
            "flags": 0x00,
            "stream": 0x0000,
            "opcode": cql_struct.OpCode.ERROR,
            "length": len(body),
            "body": body,
        })

def handle_client_connection(client_socket):
    try:
        while True:
            request = client_socket.recv(1024)
            print("Received", len(request), "bytes")
            if len(request) == 0:
                break

            sstable.positioned_construct.init()
            parsed_request = cql_struct.frame.parse(request)
            stream = io.BytesIO(request)
            print(parsed_request)
            response = b"NOT IMPLEMENTED"
            if parsed_request.opcode == cql_struct.OpCode.OPTIONS:
                response = handle_OPTIONS(parsed_request)
            elif parsed_request.opcode == cql_struct.OpCode.STARTUP:
                response = handle_STARTUP(parsed_request)
                print("sending", response)
            else:
                print(f"UNKNOWN OPCODE {hex(parsed_request.opcode)}")

            print(f"Sending {len(response)} bytes")
            client_socket.send(response)

    finally:
        client_socket.close()


def start_tcp_server(ip, port):
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((ip, port))
    server.listen(5)  # max backlog of connections

    print("Listening on", ip, ":", port)
    try:
        while True:
            client_sock, address = server.accept()
            print("Accepted connection from", address[0], ":", address[1])
            client_handler = threading.Thread(
                target=handle_client_connection, args=(client_sock,)
            )
            client_handler.start()
    finally:
        print("Closing server")
        server.close()


parser = argparse.ArgumentParser()
parser.add_argument("-p", "--port", help="port to bind", type=int, default=9042)
args = parser.parse_args()

if __name__ == "__main__":
    start_tcp_server("0.0.0.0", args.port)

