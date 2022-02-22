import asyncio

from confluent_kafka import Consumer, Producer, OFFSET_BEGINNING
from confluent_kafka.admin import AdminClient, NewTopic


BROKER_URL = "PLAINTEXT://localhost:9092"


async def consume(topic_name):
    """Consumes data from the Kafka Topic"""
    # Sleep for a few seconds to give the producer time to create some data
    await asyncio.sleep(2.5)

    # TODO: Set the offset reset to earliest
    c = Consumer(
        {
            "bootstrap.servers": BROKER_URL,
            "group.id": "0",
            "auto.offset.reset": "earliest",
        }
    )

    # TODO: Configure the on_assign callback
    c.subscribe([topic_name], on_assign=on_assign)

    while True:
        message = c.poll(1.0)
        if message is None:
            print("no message received by consumer")
        elif message.error() is not None:
            print(f"error from consumer {message.error()}")
        else:
            print(f"consumed message {message.key()}: {message.value()}")
        await asyncio.sleep(0.1)


def on_assign(consumer, partitions):
    """Callback for when topic assignment takes place"""
    # TODO: If the topic is configured to use `offset_earliest` set the partition offset to
    # the beginning or earliest
    for partition in partitions:
        partition.offset = OFFSET_BEGINNING

    # TODO: Assign the consumer the partitions
    consumer.assign(partitions)


def main():
    """Runs the exercise"""
    try:
        asyncio.run(produce_consume("org.chicago.cta.stations.table.v1"))
    except KeyboardInterrupt as e:
        print("shutting down")

async def produce_consume(topic_name):
    """Runs the Producer and Consumer tasks"""
    t2 = asyncio.create_task(consume(topic_name))
    #await t1
    await t2


if __name__ == "__main__":
    main()

