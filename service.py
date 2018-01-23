#!/usr/bin/env python3
import asyncio
from aio_pika import connect, IncomingMessage
import json

from kiss_api import KissApi
from settings import CLOUD_RABBITMQ_URL
from settings import CLOUD_RABBITMQ_ROUTING_KEY

kiss_api = KissApi()

async def on_message(message: IncomingMessage):
    """
    on_message doesn't necessarily have to be defined as async.
    Here it is to show that it's possible.
    """
    msg = json.dumps({
        'headers': message.headers,
        'content_encoding': message.content_encoding,
        'message_id': message.message_id,
        'type': message.type,
        'routing_key': message.routing_key,
        'body': message.body.decode(message.content_encoding or 'utf8')
    })
    kiss_api.send_msg(msg)
    print(" [x] Received message %r" % message)
    await asyncio.sleep(5) # Represents async I/O operations
    print("After sleep!")


async def main(loop):
    # Perform connection
    connection = await connect(
        CLOUD_RABBITMQ_URL,
        loop=loop
    )

    # Creating a channel
    channel = await connection.channel()

    # Declaring queue
    queue = await channel.declare_queue('kiss')

    # Start listening the queue with name 'hello'
    await queue.consume(on_message, no_ack=True)


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.create_task(main(loop))

    # we enter a never-ending loop that waits for data and runs callbacks whenever necessary.
    print(" [*] Waiting for messages. To exit press CTRL+C")
    loop.run_forever()

