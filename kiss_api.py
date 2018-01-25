import json
import traceback
import logging
import aiohttp

class KissApi(object):
    def __init__(self, config):
        self.config = config
        self.base_url = config.get('KISS', 'BASE_URL')
        token_part = config.get('KISS', 'TOKEN_URL')
        self.token_url = '{}{}'.format(self.base_url, token_part)
        self.access_token = None
        self.logger = logging.getLogger('KissApi')

    async def refresh_access_token(self):
        self.logger.debug('getting a new_access token')
        status, data = await self.send_async_post(
            self.token_url,
            self.config.get('KISS', 'OAUTH_CREDENTIALS'),
            allow_redirects=True,
            verify_ssl=True
        )
        if status != 200:
            self.logger.error(
                'Unable to fetch the access token.\n'
                'Service Response was: {} - {}'.format(status, data)
            )
            raise KissApiException('unable to fetch api token')
        self.access_token = data['access_token']

    async def send_async_post(self, url, data, **kwargs):
        async with aiohttp.ClientSession() as session:
            try:
                resp = await session.post(url, data=data, **kwargs)
                status = resp.status
                data = await resp.json()
                return status, data
            except aiohttp.client_exceptions.ContentTypeError as e:
                return status, await resp.text()
            except aiohttp.client_exceptions.ClientConnectorError as e:
                return 0, 'Connection to {} failed'.format(url)
            finally:
                session.close()

    async def send_msg(self, json_data, callback_url, first=True):
        self.logger.debug('sending msg')
        if self.access_token is None:
            await self.refresh_access_token()
            self.logger.debug('Recieved new access token')
        headers = {
            'Authorization': 'Bearer {}'.format(self.access_token),
            'content-type': 'application/json',
            'Accept': 'application/json',
        }
        status, data = await self.send_async_post(
            '{}{}'.format(self.base_url, callback_url),
            json_data,
            headers=headers,
            verify_ssl=True,
            allow_redirects=True
        )
        if status == 401 and 'Invalid token' in str(data) and first:
            self.access_token = None
            self.logger.debug('invalid token! retry.')
            return self.send_msg(json_data, first=False)
        elif status != 200:
            self.logger.error(
                'Error while sending to kiss endpoint.\nMessage was: {}\n'
                'Response was: {} - {}\n'
                .format(json_data, status, data)
            )
            raise KissApiException(
                'error while sending msg to kiss endpoint: {}'.format(data)
            )
        return response

class KissApiException(Exception):
    pass
