#!/usr/bin/env python3
import asyncio
from aiohttp import web
import aio_pika
from datetime import datetime
import json
import logging
import sys
import traceback
import yaml

from kiss_api import KissApi, KissApiException


class AioClientService(object):
    def __init__(self, root_service):
        self.root_service = root_service
        self.config = root_service.config
        self.active = False
        self.loop = asyncio.get_event_loop()
        self.app = None
        self.logger = logging.getLogger(self.__class__.__name__)

    async def startup(self, app):
        self.app = app
        self.active = True
        await self.startup_service()

    async def startup_service(self):
        pass

    async def shutdown(self, app):
        await self.shutdown_service()
        self.active = False

    async def shutdown_service(self):
        pass


class AioPikaService(AioClientService):
    def __init__(self, root_service):
        super().__init__(root_service)
        self.aio_pika_connection = None
        self.rabbitmq_channel = None
        self.kiss_api = KissApi(self.config)

    async def startup_service(self):
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
        self.app['aio_pika'] = self.app.loop.create_task(
            queue.consume(self.on_rabbitmq_message, no_ack=False)
        )

    async def shutdown_service(self):
        if not self.active:
            self.logger.debug('Service is not active. no shutdown needed')
            return
        self.logger.debug('closing aio-pika connection...')
        self.app['aio_pika'].cancel()
        await self.app['aio_pika']
        await asyncio.sleep(0.5)
        self.logger.debug('wating for pending requests')
        await self.kiss_api.wait_for_active_requests()
        self.logger.debug('done')
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

    async def on_rabbitmq_message(self, message: aio_pika.IncomingMessage):
        callback_url = self.get_callback_for_message(message)
        if not callback_url:
            self.logger.error('WTF? unknown routing key: {}'.format(
                message.routing_key
            ))
            return

        msg = json.dumps({
            'headers': message.headers,
            'content_encoding': message.content_encoding,
            'message_id': message.message_id,
            'type': message.type,
            'routing_key': message.routing_key,
            'body': message.body.decode(message.content_encoding or 'utf8')
        })
        await self.kiss_api.send_msg(msg, callback_url)
        message.ack()
        await asyncio.sleep(5)


class AioWebServer(object):
    """
    This is the base Server that runs the aiohttp web server and shares its
    mainloop with its CLIENT_SERVICES of class AioClientService
    """
    CLIENT_SERVICES = (
        AioPikaService,
    )

    def __init__(self, config):
        self.config = config
        self.host = self.config.get('WEBSERVER', 'HOST')
        self.port = self.config.get('WEBSERVER', 'PORT')
        self.loop = asyncio.get_event_loop()
        self.loop.set_exception_handler(self.exception_handler)
        self.logger = logging.getLogger('AioService')
        self.startup_timestamp = datetime.now()
        self.active = False
        self.client_services = {}
        for cls in self.CLIENT_SERVICES:
            self.client_services[cls.__name__] = cls(self)

    def run_app(self):
        self.active = True
        loop = self.loop
        self.app = loop.run_until_complete(self.create_web_app())
        self.logger.debug('starting Client Services')
        for client_service in self.client_services.values():
            self.app.on_startup.append(client_service.startup)
            self.app.on_cleanup.append(client_service.shutdown)
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

    def exception_handler(self, loop, context):
        e = context.get('exception', None)
        if not e:
            if context['message'] == 'Task was destroyed but it is pending!':
                pass
            else:
                self.logger.error(
                    'Unknown Exception: {}'.format(context['message'])
                )
        elif not isinstance(e, KissApiException):
            self.logger.error(
                ''.join(
                    traceback.format_exception(e, None, e.__traceback__)
                )
            )
            self.logger.error('{}: {}'.format(e.__class__.__name__, e))
        if self.active:
            self.active = False
            self.app.shutdown()
            loop.call_soon_threadsafe(loop.stop)


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
    config = Config()
    if config.get('DEBUG'):
        setup_verbose_console_logging()
    server = AioWebServer(config)
    server.run_app()
