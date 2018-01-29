import codecs
import os
from aiohttp import web
from datetime import datetime, timedelta
import json

class OAuth2(object):
    def __init__(self, config):
        self.config = config
        self.active_oauth2_tokens = {}
        self.client_id = config.get(
            'WEBSERVER',
            'OAUTH_CREDENTIALS',
            'CLIENT_ID'
        )
        self.client_secret = config.get(
            'WEBSERVER',
            'OAUTH_CREDENTIALS',
            'CLIENT_SECRET'
        )
        self.oauth2_timeout = timedelta(
            seconds=config.get('WEBSERVER', 'OAUTH_TIMEOUT_SECONDS')
        )

    def active_oauth2_token(self, token):
        valid_util = self.active_oauth2_tokens.get(token)
        if not valid_util:
            return False
        if valid_util < datetime.now():
            del self.active_oauth2_tokens[token]
            return False
        return True

    def has_valid_token(self, request):
        auth = request.headers.get('Authorization', '')
        if not auth.startswith('Bearer '):
            return False
        return self.active_oauth2_token(auth[7:])

    async def no_valid_oauth2_callback(self, request):
        return web.json_response({'error': 'not authorized'}, status=401)

    async def _generate_token(self):
        return codecs.encode(os.urandom(32), 'hex').decode()

    async def get_access_token(self, request):
        data = await request.post()
        if data.get('grant_type') != 'client_credentials':
            return web.json_response({
                'error': 'atm only supported grant_type is "client_credentials"'
            })
        keys = ('client_id', 'client_secret')
        if any([data.get(key) != getattr(self, key) for key in keys]):
            return await self.no_valid_oauth2_callback(request)
        # FIXME: Add check for username and password


        valid_until = datetime.now() + self.oauth2_timeout
        new_token = await self._generate_token()
        self.active_oauth2_tokens[new_token] = valid_until
        data = {
            "access_token": new_token,
            "token_type": "BEARER",
            "expires_in": int(self.oauth2_timeout.total_seconds()),
            "refresh_token": "",
            "scope": "read"
        }
        return web.json_response(data)


    @staticmethod
    def required(func):
        async def wrapper(self, request, *args, **kwargs):
            if not isinstance(self, OAuth2):
                raise ValueError('Only use this  decorator on OAuth2 classes')
            if not self.has_valid_token(request):
                return await self.no_valid_oauth2_callback(request)
            return await func(self, request, *args, **kwargs)
        return wrapper
