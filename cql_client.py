from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

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
        for row in result:
            print(row)
    except Exception as e:
        print("An error occurred:", e)

if __name__ == '__main__':
    # Set the connection details
    hosts = ['127.0.0.1']  # List of hosts, add more if needed
    port = 9090  # The port your server is listening on

    # Connect to Cassandra
    session = connect_to_cassandra(hosts, port)

    # Execute a query
    query = "SELECT * FROM your_keyspace.your_table"
    execute_query(session, query)
