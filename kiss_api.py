import json
import traceback
import logging
import requests


from settings import KISS_BASE_URL
from settings import KISS_TOKEN_URL
from settings import KISS_CREDENTIALS
from settings import KISS_CLOUDAMQP_ENDPOINT

class KissApi(object):
    def __init__(self):
        self.base_url = KISS_BASE_URL
        self.token_url = '{}{}'.format(KISS_BASE_URL, KISS_TOKEN_URL)
        self.send_url = '{}{}'.format(KISS_BASE_URL, KISS_CLOUDAMQP_ENDPOINT)
        self.access_token = 'None'
        self.logger = logging.getLogger('KissApi')
        self.refresh_access_token()

    def refresh_access_token(self):
        self.logger.debug('getting a new_access token')
        resp = requests.post(
            self.token_url,
            data=KISS_CREDENTIALS,
            allow_redirects=True,
            verify=True
        )
        content = resp.content.decode('utf-8')
        try:
            data = json.loads(content)
            self.access_token = data['access_token']
        except Exception:
            traceback.print_exc()
            raise KissApiException(
                'Unable to get new token. Server response was: "{}"'
                .format(content)
            )

    def send_msg(self, json_data, first=True):
        self.logger.debug('sending msg')
        if self.access_token is None:
            self.refresh_access_token()
        headers = {
            'Authorization': 'Bearer {}'.format(self.access_token),
            'content-type': 'application/json',
            'Accept': 'application/json',
        }
        kwargs = {
            'allow_redirects': True,
            'headers': headers,
            'verify': True,
            'data': json_data
        }
        response = requests.post(self.send_url, **kwargs)
        content = response.content.decode('utf-8')
        if response.status_code == 401 and 'Invalid token' in content and first:
            self.access_token = None
            self.logger.debug('invalid token! retry.')
            return self.send_msg(json_data, first=False)
        elif response.status_code != 200:
            self.logger.error(
                'Error while sending to kiss endpoint.\nMessage was: {}\n'
                'Response was: {} - {}\n'
                .format(json_data, response.status_code, content)
            )
            raise KissApiException(
                'error while sending msg to kiss endpoint: {}'.format(content))
        return response

class KissApiException(Exception):
    pass
