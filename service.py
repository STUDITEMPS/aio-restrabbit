#!/usr/bin/env python3
import asyncio
from aiohttp import web
import aio_pika
from datetime import datetime
import json
import logging
import traceback
import sys

from kiss_api import KissApi, KissApiException
from conf.settings import CLOUD_RABBITMQ_URL
from conf.settings import CLOUD_RABBITMQ_EXCHANGE
from conf.settings import CLOUD_RABBITMQ_CHANNEL
from conf.settings import DEBUG
from conf.settings import WEBSERVER_HOST
from conf.settings import WEBSERVER_PORT

kiss_api = KissApi()
logger = logging.getLogger('Service')

class AioServer(object):
    """
    This is the base Server that runs the aiohttp web server and the
    aio-pika connection.
    The Rest connection is created by kiss_api.KissApi()
    """
    def __init__(self, host='localhost', port=12345):
        self.host = host
        self.port = port
        self.loop = asyncio.get_event_loop()
        self.loop.set_exception_handler(self.exception_handler)
        self.kiss_api = KissApi()
        self.aio_pika_connection = None
        self.logger = logging.getLogger('AioService')
        self.startup_timestamp = datetime.now()

    def run_app(self):
        loop = self.loop
        self.app = loop.run_until_complete(self.create_web_app())
        self.app.on_startup.append(self.start_aoi_pika_task)
        self.app.on_cleanup.append(self.cleanup_aoi_pika_task)
        self.logger.debug('starting Webserver')
        web.run_app(self.app, host=self.host, port=self.port)
        self.logger.debug('Webserver started on port {}'.format(self.port))

    async def create_web_app(self):
        self.logger.debug('registering Webserver urls...')
        app = web.Application()
        app.router.add_post('/', self.index)
        app.router.add_get('/heartbeat', self.heartbeat)
        app.router.add_post('/oauth2/access_token/', self.get_token)
        return app

    async def index(self, request):
        return web.Response(text="YEAAAYYY")

    async def heartbeat(self, request):
        msg = (
            'Welcome Stranger. Stay a while and listen. I am alive since {}.'
            .format(self.startup_timestamp.strftime('%Y-%m-%d %H:%M:%S'))
        )
        return web.Response(text=msg)

    async def get_token(self, request):
        return web.json_response({'access_token': 'not_valid'})

    async def start_aoi_pika_task(self, app):
        self.logger.debug('Starting aio-pika connection...')
        self.aio_pika_connection = await aio_pika.connect(
            CLOUD_RABBITMQ_URL,
            loop=self.loop
        )
        channel = await self.aio_pika_connection.channel()
        kiss_exchange = await channel.declare_exchange(
            CLOUD_RABBITMQ_EXCHANGE,
            aio_pika.ExchangeType.TOPIC,
            durable=True
        )
        queue = await channel.declare_queue(CLOUD_RABBITMQ_CHANNEL)
        await queue.bind(kiss_exchange, routing_key='#')
        self.logger.debug('aio-pika connection ready')
        app['aio_pika'] = app.loop.create_task(
            queue.consume(self.on_rabbitmq_message, no_ack=True)
        )

    async def cleanup_aoi_pika_task(self, app):
        self.logger.debug('closing aio-pika connection...')
        app['aio_pika'].cancel()
        await app['aio_pika']
        await self.aio_pika_connection.close()
        self.logger.debug('aio-pika connection closed')

    async def on_rabbitmq_message(self, message: aio_pika.IncomingMessage):
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
        response = await self.kiss_api.send_msg(msg)
        await asyncio.sleep(5)

    def exception_handler(self, loop, context):
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
        self.app.shutdown()
        loop.call_soon_threadsafe(loop.stop)


def setup_verbose_console_logging():
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    ch.setFormatter(formatter)
    root.addHandler(ch)

if __name__ == "__main__":
    if DEBUG:
        setup_verbose_console_logging()
    server = AioServer(host=WEBSERVER_HOST, port=WEBSERVER_PORT)
    server.run_app()


