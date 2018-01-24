#!/usr/bin/env python3
import asyncio
from aio_pika import connect, IncomingMessage
from datetime import datetime
import json
import logging
import traceback

from kiss_api import KissApi, KissApiException
from settings import CLOUD_RABBITMQ_URL
from settings import CLOUD_RABBITMQ_ROUTING_KEY

kiss_api = KissApi()
logger = logging.getLogger('Service')

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
    response = await kiss_api.send_msg(msg)
    await asyncio.sleep(5)

def exception_handler(loop, context):
    e = context.get('exception', None)
    if not e:
        logger.error('Unknown Exception: {}'.format(context['message']))
    elif not isinstance(e, KissApiException):
        logger.error(
            ''.join(
                traceback.format_exception(e, None, e.__traceback__)
            )
        )
        logger.error('{}: {}'.format(e.__class__.__name__, e))
    loop.stop()
    loop.call_soon_threadsafe(loop.stop)

async def main(loop):
    connection = await connect(
        CLOUD_RABBITMQ_URL,
        loop=loop
    )
    channel = await connection.channel()
    queue = await channel.declare_queue('iao-restrabbit')
    await queue.consume(on_message, no_ack=True)


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.set_exception_handler(exception_handler)
    loop.create_task(main(loop))
    logger.debug(
        " [*] {} Service started. To exit press CTRL+C"
        .format(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    )
    loop.run_forever()
    loop.close()

