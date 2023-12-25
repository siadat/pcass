
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import argparse

def connect_to_cassandra(hosts, port, username=None, password=None):
    if username and password:
        auth_provider = PlainTextAuthProvider(username=username, password=password)
        cluster = Cluster(contact_points=hosts, port=port, auth_provider=auth_provider)
    else:
        cluster = Cluster(contact_points=hosts, port=port)

    session = cluster.connect()
    return session

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

    session = connect_to_cassandra([args.host], args.port)

    query = "SELECT * FROM your_keyspace.your_table"
    result = execute_query(session, query)
    for row in result:
        print(row)

