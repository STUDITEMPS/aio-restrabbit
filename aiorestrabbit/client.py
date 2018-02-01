import aio_pika
import asyncio
import json
import logging
import os

from aiorestrabbit.kiss_api import KissApi
from aiorestrabbit.kiss_api import KissApiException
from aiorestrabbit.kiss_api import KissOfflineException


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
        except KissApiException as e:
            message.reject(requeue=True)
            raise e
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
    RUNNING_INDICATOR = '.running'
    STOPPING_INDICATOR = '.stopping'

    async def startup_service(self):
        self.root_service.loop.create_task(self.checker())

    async def shutdown_service(self):
        self.active = False

    async def checker(self):
        while self.active and not os.path.exists(self.STOPPING_INDICATOR):
            await asyncio.sleep(.1)
        self.root_service.shutdown()

    @classmethod
    def run(cls):
        # create RUNNING_INDICATOR file
        open(cls.RUNNING_INDICATOR, 'w').close()

    @classmethod
    def stop(cls):
        # create STOPPING_INDICATOR file
        open(cls.STOPPING_INDICATOR, 'w').close()

    @classmethod
    def is_running(cls):
        return os.path.exists(cls.RUNNING_INDICATOR)

    @classmethod
    def is_stopping(cls):
        return os.path.exists(cls.STOPPING_INDICATOR)

    @classmethod
    def cleanup(cls):
        for i in (cls.RUNNING_INDICATOR, cls.STOPPING_INDICATOR):
            if os.path.exists(i):
                os.remove(i)
