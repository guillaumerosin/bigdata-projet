from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

SCYLLA_NODES = ['172.20.0.171', '172.20.0.172', '172.20.0.173']
SCYLLA_USER = 'user_kawasaki'
SCYLLA_PASS = 'mot_de_passe'
SCYLLA_KEYSPACE = 'mon_keyspace'

auth = PlainTextAuthProvider(username=SCYLLA_USER, password=SCYLLA_PASS)
cluster = Cluster(SCYLLA_NODES, auth_provider=auth)
session = cluster.connect(SCYLLA_KEYSPACE)

print("Connexion Scylla OK :", session)

session.shutdown()
cluster.shutdown()