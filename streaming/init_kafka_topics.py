"""
init_kafka_topics.py
"""

from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"

TOPIC_CONFIG = [
    {
        "name": "stock_prices",
        "num_partitions": 3, 
        "replication_factor": 1,
        "config": {
            "retention.ms": "864000000", #24 hours
            "cleanup.policy": "delete"
        }
    },
    {
        "name": "risk_alerts",
        "num_partitions": 1,
        "replication_factor": 1,
        "config": {
            "retention.ms": "604800000", #7 days
        }
    },
]

def create_topics():
    logger.info(f"Connecting to Kafka at {KAFKA_BOOTSTRAP_SERVERS}...")

    admin_client = KafkaAdminClient(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        client_id="equity_topic_initializer"
    )

    new_topics = [
        NewTopic(
            name=t["name"],
            num_partitions=t["num_partitions"],
            replication_factor=t["replication_factor"],
            topic_configs=t.get("config", {})
        )
        for t in TOPIC_CONFIG
    ]

    success_count = 0
    for topic in new_topics:
        try:
            admin_client.create_topics([topic], validate_only=False)
            logger.info(f"Topic was created successdully: {topic.name}")
            success_count += 1
        except TopicAlreadyExistsError:
            logger.info(f"Topic already exists: {topic.name}")
        except Exception as e:
            logger.error(f"Failed to create topic {topic.name}: {e}")
    
    admin_client.close()
    logger.info(f"created {success_count} topics successfully.")

if __name__=="__main__":
    create_topics()
