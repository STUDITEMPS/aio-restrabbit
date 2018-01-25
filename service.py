#!/usr/bin/env python3
import asyncio
from aiohttp import web
import aio_pika
from datetime import datetime
import json
import logging
import pika
import sys
import traceback
import yaml

from kiss_api import KissApi, KissApiException

class AioServer(object):
    """
    This is the base Server that runs the aiohttp web server and the
    aio-pika connection.
    The Rest connection is created by kiss_api.KissApi()
    """
    def __init__(self, config):
        self.config = config
        self.host = self.config.get('WEBSERVER', 'HOST')
        self.port = self.config.get('WEBSERVER', 'PORT')
        self.loop = asyncio.get_event_loop()
        self.loop.set_exception_handler(self.exception_handler)
        self.kiss_api = KissApi(config)
        self.aio_pika_connection = None
        self.logger = logging.getLogger('AioService')
        self.startup_timestamp = datetime.now()
        self.rabbitmq_channel = None

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
            self.config.get('CLOUD_RABBITMQ', 'URL'),
            loop=self.loop
        )
        self.rabbitmq_channel = await self.aio_pika_connection.channel()
        exchange_data = self.config.get('CLOUD_RABBITMQ', 'EXCHANGES')
        for exchange_name, data in exchange_data.items():
            exchange = await self.rabbitmq_channel.declare_exchange(
                exchange_name,
                aio_pika.ExchangeType.TOPIC,
                durable=True
            )
            queue = await self.rabbitmq_channel.declare_queue(
                self.config.get('CLOUD_RABBITMQ', 'CHANNEL')
            )
            for routing_key in data.keys():
                await queue.bind(exchange, routing_key=routing_key)
        self.logger.debug('aio-pika connection ready')
        app['aio_pika'] = app.loop.create_task(
            queue.consume(self.on_rabbitmq_message, no_ack=False)
        )

    async def cleanup_aoi_pika_task(self, app):
        self.logger.debug('closing aio-pika connection...')
        app['aio_pika'].cancel()
        await app['aio_pika']
        await self.aio_pika_connection.close()
        self.logger.debug('aio-pika connection closed')

    def get_callback_for_message(self, message):
        exchanges_data = self.config.get('CLOUD_RABBITMQ', 'EXCHANGES')
        exchange_routing = exchanges_data.get(message.exchange)
        if exchange_routing is None:
            return None
        for routing_key, callback in exchange_routing.items():
            if routing_key[-1] == '#':
                if message.routing_key.startswith(routing_key[:-1]):
                    return callback
            elif routing_key == message.routing_key:
                return callback
        return None

    def get_exchange(self, message):
        return self.registed_exchanges.get(message.exchange)

    async def on_rabbitmq_message(self, message: aio_pika.IncomingMessage):
        """
        on_message doesn't necessarily have to be defined as async.
        Here it is to show that it's possible.
        """
        callback_url = self.get_callback_for_message(message)
        if not callback_url:
            self.logger.error('WTF? unknown routing key: {}'.format(message.routing_key))
            return

        msg = json.dumps({
            'headers': message.headers,
            'content_encoding': message.content_encoding,
            'message_id': message.message_id,
            'type': message.type,
            'routing_key': message.routing_key,
            'body': message.body.decode(message.content_encoding or 'utf8')
        })
        try:
            await self.kiss_api.send_msg(msg, callback_url)
            message.ack()
        except:
            try:
                message.reject(requeue=True)
            except pika.exceptions.ChannelClosed:
                return
            except RuntimeError:
                return
            raise
        await asyncio.sleep(5)

    def exception_handler(self, loop, context):
        e = context.get('exception', None)
        if not e:
            self.logger.error('Unknown Exception: {}'.format(context['message']))
        elif isinstance(e, RuntimeError):
            pass
        elif not isinstance(e, KissApiException):
            self.logger.error(
                ''.join(
                    traceback.format_exception(e, None, e.__traceback__)
                )
            )
            self.logger.error('{}: {}'.format(e.__class__.__name__, e))
        self.app.shutdown()
        try:
            loop.call_soon_threadsafe(loop.stop)
        except RuntimeError:
            pass

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

class Config(object):
    def __init__(self):
        self.current_subpath = []

        with open("conf/settings.yml", 'r') as fstream:
            self.data = yaml.load(fstream)

    def get(self, *splitted_path):
        if len(splitted_path) == 0:
            self.current_subpath = []
            raise AttributeError('Must be called with at last one attr')
        src = self.data
        for part in self.current_subpath:
            src = self.data.get(part)
        active_param = splitted_path[0]
        if len(splitted_path) == 1:
            try:
                value = src.get(active_param)
                if value is None:
                    raise AttributeError
                self.current_subpath = []
                return value
            except AttributeError:
                raise self.ConfigException(self, active_param)
        else:
            try:
                if src.get(active_param) is None:
                    raise AttributeError
            except AttributeError:
                raise self.ConfigException(self, active_param)
        self.current_subpath.append(splitted_path[0])
        splitted_path = splitted_path[1:]
        return self.get(*splitted_path)


    class ConfigException(Exception):
        def __init__(self, config, active_param):
            text = (
                'Unable to find Parameter "{}" in config section {}'
                .format(active_param, config.current_subpath)
            )
            config.current_subpath = []
            super().__init__(text)



if __name__ == "__main__":
    config = Config()
    if config.get('DEBUG'):
        setup_verbose_console_logging()
    server = AioServer(config)
    server.run_app()


