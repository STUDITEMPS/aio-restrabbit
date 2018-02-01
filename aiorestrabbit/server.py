from aiohttp import web
import aiohttp_jinja2
from aiohttp_session import setup
from aiohttp_session.cookie_storage import EncryptedCookieStorage
import asyncio
import base64
import cryptography.fernet
from datetime import datetime
import jinja2
import json
import logging
import os
import traceback

import aiorestrabbit.client
from aiorestrabbit.kiss_api import KissApiException


class AioWebServer(aiorestrabbit.auth.OAuth2):
    """
    This is the base Server that runs the aiohttp web server and shares its
    mainloop with its CLIENT_SERVICES of class AioClientService
    """
    CLIENT_SERVICES = (
        aiorestrabbit.client.AioPikaService,
        aiorestrabbit.client.StartStopService,
    )

    def __init__(self, config):
        super().__init__(config)
        self.host = self.config.get('WEBSERVER', 'HOST')
        self.port = self.config.get('WEBSERVER', 'PORT')
        self.project_dir = os.path.abspath(
            os.path.join(
                os.path.dirname(os.path.abspath(__file__)),
                '..'
            )
        )
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

    @aiorestrabbit.auth.OAuth2.required
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
        if self.active:
            aiorestrabbit.client.StartStopService.cleanup()
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
