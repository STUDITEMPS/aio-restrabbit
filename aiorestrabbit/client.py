import aio_pika
import asyncio
import json
import logging
import os

from aiorestrabbit.kiss_api import KissApi
from aiorestrabbit.kiss_api import KissApiException
from aiorestrabbit.kiss_api import KissOfflineException


class AioClientStatus(object):
    INIT = 0
    STARTUP = 1
    ACTIVE = 2
    BREAK = 3
    SHUTDOWN = 4
    HALT = 5

    STATE_NAMES = {
        INIT: 'initialized',
        STARTUP: 'starting',
        ACTIVE: 'active',
        BREAK: 'taking a break',
        SHUTDOWN: 'shutting down',
        HALT: 'halt'
    }

    def __init__(self):
        self.status = self.INIT

    def is_init(self):
        return self.status == self.INIT

    def set_starting(self):
        self.status = self.STARTUP

    def is_starting(self):
        return self.status == self.STARTUP

    def set_active(self):
        self.status = self.ACTIVE

    def is_active(self):
        return self.status == self.ACTIVE

    def set_break(self):
        self.status = self.BREAK

    def is_break(self):
        return self.status == self.BREAK

    def set_shutdown(self):
        self.status = self.SHUTDOWN

    def is_shutdown(self):
        return self.status == self.SHUTDOWN

    def set_halt(self):
        self.status = self.HALT

    def is_halt(self):
        return self.status == self.HALT

    def is_in(self, *states):
        if len(states) == 1 and isinstance(states[0], (tuple, list)):
            states = states[0]
        return self.status in states

    @property
    def name(self):
        return self.STATE_NAMES[self.status]

    def __repr__(self):
        return self.name


class AioClientService(object):
    def __init__(self, root_service):
        self.root_service = root_service
        self.config = root_service.config
        self.status = AioClientStatus()
        self.loop = asyncio.get_event_loop()
        self.app = None
        self.logger = logging.getLogger(self.__class__.__name__)

    def get_status(self):
        state_type = 'error'
        if self.status.is_active():
            state_type = 'success'
        elif self.status.is_in(AioClientStatus.STARTUP, AioClientStatus.BREAK):
            state_type = 'warning'
        return (state_type, self.status)

    async def startup(self, app):
        self.status.set_starting()
        self.app = app
        await self.startup_service()
        self.status.set_active()

    async def startup_service(self):
        pass

    async def shutdown(self, app):
        self.status.set_shutdown()
        await self.shutdown_service()
        self.status.set_halt()

    async def shutdown_service(self):
        pass

    def __repr__(self):
        return self.__class__.__name__

    async def take_a_break(self, timeout, msg=''):
        if self.status.is_break():
            return
        self.status.set_break()
        if msg:
            self.logger.error(msg)
        else:
            self.logger.error(
                u'Service {} is not working properly and takes a break for {} '
                u'seconds'.format(self, timeout)
            )
        await self.shutdown_service()
        await asyncio.sleep(timeout)
        self.logger.debug(u'Timeout is finished. Starting up {}'.format(self))
        self.status.set_starting()
        await self.startup_service()
        self.logger.debug(u'Startup of {} Done.'.format(self))
        self.status.set_active()
        self.logger.debug(u'Break of {} Done.'.format(self))

    def handle_exception(self, exception):
        """
        This function recieves an exception from the main loop excpetion handler
        returns True is the exception is handeled, False otherwise
        """
        return False


class AioPikaService(AioClientService):
    def __init__(self, root_service):
        super().__init__(root_service)
        self.aio_pika_connection = None
        self.rabbitmq_channel = None
        self.kiss_api = KissApi(self.config)
        self.exchanges = {}
        self.concurrent_requests = 0
        self.max_concurrent_requests= self.config.get(
            'KISS',
            'MAX_CONCURRENT_REQUESTS'
        )

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
        self.kiss_api.in_break = False
        self.app['aio_pika'] = self.app.loop.create_task(
            queue.consume(self.on_rabbitmq_message, no_ack=False)
        )

    async def shutdown_service(self):
        connection_states = (
            AioClientStatus.STARTUP,
            AioClientStatus.BREAK,
            AioClientStatus.ACTIVE
        )
        if not self.status.is_in(connection_states):
            self.logger.debug('Service is not active. no shutdown needed')
            return
        self.logger.debug('closing aio-pika connection...')
        self.app['aio_pika'].cancel()
        await self.app['aio_pika']
        await asyncio.sleep(.5)
        self.logger.debug('wating for pending requests')
        await self.kiss_api.shutdown()
        self.logger.debug('done')
        await self.aio_pika_connection.close()
        self.rabbitmq_channel = None
        self.logger.debug('aio-pika connection closed')

    def handle_exception(self, exception):
        if isinstance(exception, aio_pika.exceptions.ChannelClosed):
            need_channel_states = (
                AioClientStatus.STARTUP,
                AioClientStatus.ACTIVE
            )
            if not self.status.is_in(need_channel_states):
                return True
            self.root_service.loop.create_task(
                self.take_a_break(
                    60,
                    'connection to CLOUDAMQP is broken. sleeping for 1 min.'
                )
            )
            return True
        elif isinstance(exception, KissApiException):
            self.root_service.shutdown()
            return True
        return False

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
        self.concurrent_requests += 1
        if self.concurrent_requests >= self.max_concurrent_requests:
            self.logger.warning('TOO MANY CONCURRENT REQUESTS... requeuing')
            await asyncio.sleep(1)
            message.reject(requeue=True)
            self.concurrent_requests -= 1
            return
        if not self.status.is_active():
            message.reject(requeue=True)
            self.concurrent_requests -= 1
            return
        callback_url = self.get_callback_for_message(message)
        if not callback_url:
            self.logger.error('WTF? unknown routing key: {}'.format(
                message.routing_key
            ))
            message.reject(requeue=True)
            self.concurrent_requests -= 1
            return
        try:
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
        except KissApiException as e:
            message.reject(requeue=True)
            raise e
        except KissOfflineException as e:
            message.reject(requeue=True)
            self.root_service.loop.create_task(
                self.take_a_break(
                    60,
                    'Kiss is Offline. Stopping {} for 1 min. Details: {}'
                    .format(self, e)
                )
            )
        finally:
            self.concurrent_requests -= 1

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
        StartStopService.cleanup()

    async def checker(self):
        while not self.status.is_halt() and not StartStopService.is_stopping():
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
