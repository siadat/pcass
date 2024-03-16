from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra import ProtocolVersion

# Let's see debug logs printed by the driver
import logging
logging.basicConfig(level=logging.DEBUG)

import argparse

def execute_query(session, query):
    try:
        result = session.execute(query)
        return result
    except Exception as e:
        print("An error occurred:", e)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-H", "--host", default="127.0.0.1", help="The host to connect to.")
    parser.add_argument("-p", "--port", type=int, default=9042, help="The port to connect to.")
    args = parser.parse_args()

    cluster = Cluster(contact_points=[args.host], port=args.port) # , protocol_version=ProtocolVersion.V4)
    session = cluster.connect()
    # print(cluster.metadata.keyspaces)

    query = "SELECT * FROM your_keyspace.your_table"
    result = execute_query(session, query)
    for row in result:
        print(row)
