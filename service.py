#!/usr/bin/env python3

from aiohttp import web
import aiohttp_jinja2
import aio_pika
from aiohttp_session import setup
from aiohttp_session.cookie_storage import EncryptedCookieStorage
import argparse
import asyncio
import base64
import cryptography.fernet
from datetime import datetime
import jinja2
import json
import logging
import os
import pika
import sys
import time
import traceback


import auth
import config
from kiss_api import KissApi, KissApiException, KissOfflineException

RUNNING_INDICATOR = '.running'
STOPPING_INDICATOR = '.stopping'

class AioClientService(object):
    def __init__(self, root_service):
        self.root_service = root_service
        self.config = root_service.config
        self.active = False
        self.taking_a_break = False
        self.loop = asyncio.get_event_loop()
        self.app = None
        self.logger = logging.getLogger(self.__class__.__name__)

    def status(self):
        if not self.active:
            return ('error', 'offline')
        if self.taking_a_break:
            return ('warning', 'paused')
        return ('success', 'active')

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

    def __repr__(self):
        return self.__class__.__name__

    async def take_a_break(self, timeout):
        if self.taking_a_break:
            return
        self.taking_a_break = True
        self.logger.error(
            u'Service {} is not working properly and takes a break for {} '
            u'minutes'.format(self, timeout)
        )
        await self.shutdown_service()
        await asyncio.sleep(timeout)
        await self.startup_service()
        self.taking_a_break = False
        self.logger.error(u'Back Online. Lets see if it works now')


class AioPikaService(AioClientService):
    def __init__(self, root_service):
        super().__init__(root_service)
        self.aio_pika_connection = None
        self.rabbitmq_channel = None
        self.kiss_api = KissApi(self.config)
        self.exchanges = {}

    async def startup_service(self):
        self.logger.debug('Starting aio-pika connection...')
        self.aio_pika_connection = await aio_pika.connect(
            self.config.get('CLOUD_RABBITMQ', 'URL'),
            loop=self.loop
        )
        self.logger.debug('creating channel')
        self.rabbitmq_channel = await self.aio_pika_connection.channel()
        self.logger.debug('creating routing keys')
        exchange_data = self.config.get('CLOUD_RABBITMQ', 'EXCHANGES')
        for exchange_name, data in exchange_data.items():
            exchange = await self.get_exchange(exchange_name)
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
        await self.kiss_api.shutdown()
        self.logger.debug('done')
        await self.aio_pika_connection.close()
        self.rabbitmq_channel = None
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
        if self.taking_a_break:
            return
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
        try:
            await self.kiss_api.send_msg(msg, callback_url)
            message.ack()
        except KissApiException:
            message.reject(requeue=True)
        except KissOfflineException:
            message.reject(requeue=True)
            self.root_service.loop.create_task(self.take_a_break(5))

    async def get_exchange(self, exchange_name):
        exchange = self.exchanges.get(exchange_name)
        if exchange is None:
            exchange = await self.rabbitmq_channel.declare_exchange(
                exchange_name,
                aio_pika.ExchangeType.TOPIC,
                durable=True
            )
            self.exchanges[exchange_name] = exchange
        return exchange

    async def send_message(self, routing_key, msg, headers={}, **kwargs):
        if 'exchange_name' not in kwargs.keys():
            exchange_name = self.config.get(
                'CLOUD_RABBITMQ',
                'DEFAULT_SENDER_EXCHANGE'
            )
        exchange = await self.get_exchange(exchange_name)
        message = aio_pika.Message(
            msg.encode('utf8'),
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            headers=headers,
            **kwargs
        )
        await exchange.publish(message, routing_key=routing_key)


class StartStopService(AioClientService):

    async def startup_service(self):
        self.root_service.loop.create_task(self.checker())

    async def shutdown_service(self):
        self.active = False

    async def checker(self):
        while self.active and not os.path.exists(STOPPING_INDICATOR):
            await asyncio.sleep(.1)
        self.root_service.shutdown()

class AioWebServer(auth.OAuth2):
    """
    This is the base Server that runs the aiohttp web server and shares its
    mainloop with its CLIENT_SERVICES of class AioClientService
    """
    CLIENT_SERVICES = (
        AioPikaService,
        StartStopService,
    )

    def __init__(self, config):
        super().__init__(config)
        self.host = self.config.get('WEBSERVER', 'HOST')
        self.port = self.config.get('WEBSERVER', 'PORT')
        self.project_dir = os.path.dirname(os.path.abspath(__file__))
        self.template_dir = os.path.join(self.project_dir, 'templates')
        self.static_dir = os.path.join(self.project_dir, 'static')
        self.loop = asyncio.get_event_loop()
        self.loop.set_exception_handler(self.exception_handler)
        self.logger = logging.getLogger('WebService')
        self.startup_timestamp = datetime.now()
        self.active = False
        self.client_services = {}
        self.session_key = base64.urlsafe_b64decode(
            cryptography.fernet.Fernet.generate_key()
        )
        self.active_oauth2_tokens = {}
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
        app['static_root_url'] = '/static'
        setup(app, EncryptedCookieStorage(self.session_key))
        aiohttp_jinja2.setup(
            app,
            loader=jinja2.FileSystemLoader(self.template_dir)
        )
        app.router.add_static('/static', self.static_dir)
        app.router.add_post('/api/send_message', self.send_cloudamqp_message)
        app.router.add_get('/', self.index)
        app.router.add_post('/oauth2/access_token/', self.get_access_token)
        return app

    @auth.OAuth2.required
    async def send_cloudamqp_message(self, request):
        data = await request.post()
        routing_key = data.get('routing_key', '')
        message_body = data.get('message_body', '')
        headers = json.loads(data.get('headers', '{}'))
        await self.client_services.get('AioPikaService').send_message(
            routing_key,
            message_body,
            headers=headers
        )
        return web.json_response({'status': 'success'})

    @aiohttp_jinja2.template('status.html')
    async def index(self, request):
        return {
            'server': self,
            'active_since':
            self.startup_timestamp.strftime('%d.%m.%Y %H:%M:%S')
        }

    def _rm_file_is_exists(self, name):
        if os.path.exists(name):
            os.remove(name)

    def shutdown(self):
        self._rm_file_is_exists(RUNNING_INDICATOR)
        self._rm_file_is_exists(STOPPING_INDICATOR)
        if self.active:
            self.active = False
            self.app.shutdown()
            self.loop.call_soon_threadsafe(self.loop.stop)

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
        self.shutdown()


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

def start(args):
    running_indicator = open(RUNNING_INDICATOR, 'w')
    running_indicator.close()
    c = config.Config()
    if c.get('DEBUG') or args.verbose:
        setup_verbose_console_logging()
    server = AioWebServer(c)
    server.run_app()

def stop(args):
    if not os.path.exists(RUNNING_INDICATOR):
        return
    want_to_stop = open(STOPPING_INDICATOR, 'w')
    want_to_stop.close()
    while os.path.exists(RUNNING_INDICATOR):
        time.sleep(0.1)

def restart(args):
    stop()
    start(args)

def clean(args):
    for i in (RUNNING_INDICATOR, STOPPING_INDICATOR):
        if os.path.exists(i):
            os.rm(i)

if __name__ == "__main__":
    action_mapper = {
        'start': start,
        'stop': stop,
        'restart': restart,
        'clean': clean
    }
    parser = argparse.ArgumentParser(description='AIORestRabbit Service')
    parser.add_argument('-v', '--verbose', action='store_const', const=True)
    parser.add_argument('action', nargs='?', default="restart", choices=action_mapper.keys())
    args = parser.parse_args()
    action_mapper[args.action](args)
    restart(args)
