import logging
import os
from functools import lru_cache

from cassandra.cluster import Cluster, Session
from cassandra.policies import DCAwareRoundRobinPolicy
from cassandra.query import dict_factory

logger = logging.getLogger(__name__)

KEYSPACE = "market_analytics"


@lru_cache(maxsize=1)
def get_session() -> Session:
    host = os.getenv("CASSANDRA_HOST", "cassandra")
    port = int(os.getenv("CASSANDRA_PORT", "9042"))

    logger.info("Connecting to Cassandra at %s:%d", host, port)
    cluster = Cluster(
        contact_points=[host],
        port=port,
        load_balancing_policy=DCAwareRoundRobinPolicy(local_dc="datacenter1"),
        protocol_version=4,
    )
    session = cluster.connect(KEYSPACE)
    session.row_factory = dict_factory
    logger.info("Cassandra session established (keyspace=%s)", KEYSPACE)
    return session


def execute(cql: str, params=None):
    session = get_session()
    rows = session.execute(cql, params or [])
    return list(rows)
