"""
Copyright 2013 Russell Haering.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

import json
from StringIO import StringIO
from uuid import uuid4


from treq import content, json_content
from treq.client import HTTPClient
from twisted.internet import reactor, defer, error, task
from twisted.python import log
from twisted.web.client import (
    Agent,
    RedirectAgent,
    ContentDecoderAgent,
    GzipDecoder,
    FileBodyProducer,
    _HTTP11ClientFactory,
    HTTPConnectionPool
)
from twisted.web._newclient import RequestTransmissionFailed
from txKeystone import KeystoneAgent

from txmarconi.version import __version__


class MarconiError(Exception):
    pass


class MarconiMessage(object):
    def __init__(self, **kwargs):
        self.body = kwargs.get('body')
        self.ttl = kwargs.get('ttl')
        self.age = kwargs.get('age')
        self.href = kwargs.get('href')


class ClaimedMarconiMessage(MarconiMessage):
    def __init__(self, **kwargs):
        super(ClaimedMarconiMessage, self).__init__(**kwargs)
        self.claim_href = kwargs.get('claim_href')


class QuieterFileBodyProducer(FileBodyProducer):
    """
    A hack to keep Twisted quieter. From: http://twistedmatrix.com/trac/ticket/6528
    """
    def stopProducing(self):
        try:
            FileBodyProducer.stopProducing(self)
        except task.TaskStopped:
            pass


class QuieterHTTP11ClientFactory(_HTTP11ClientFactory):
    """
    Normally, an _HTTP11ClientFactory logs two messages for every HTTP
    request. When polling at high frequency this can result in a lot of
    log messages. Use of this ClientFactory allows users to suppress
    these messages.
    """
    noisy = False


class MarconiClient(object):
    USER_AGENT = 'txmarconi/{version}'.format(version=__version__)
    RETRYABLE_ERRORS = [RequestTransmissionFailed]

    def __init__(self, base_url='http://localhost:8888', quiet_requests=True, **kwargs):
        self.client_id = str(uuid4())
        self.base_url = base_url
        pool = HTTPConnectionPool(reactor, persistent=True)
        agent = ContentDecoderAgent(RedirectAgent(Agent(reactor, pool=pool)), [('gzip', GzipDecoder)])

        if quiet_requests:
            pool._factory = QuieterHTTP11ClientFactory

        auth_url = kwargs.get('auth_url')
        if auth_url:
            username = kwargs.get('username')
            password = kwargs.get('password')
            api_key = kwargs.get('api_key')

            if not username:
                raise RuntimeError('Marconi "auth_url" specified with no username')

            if api_key:
                cred = api_key
                auth_type = 'api_key'
            elif password:
                cred = password
                auth_type = 'password'
            else:
                raise RuntimeError('Marconi "auth_url" specified with no "password" or "api_key"')

            agent = KeystoneAgent(agent, auth_url, (username, cred), auth_type=auth_type)

        self.http_client = HTTPClient(agent)

    def _wrap_error(self, failure):
        if not failure.check(MarconiError):
            log.err(failure)
            raise MarconiError(failure.value)

        log.err(failure.value)
        return failure

    def _handle_error_response(self, response):
        def _raise_error(content_str):
            content_str = content_str.strip()
            if len(content_str) > 0:
                raise MarconiError(json.loads(content_str))
            else:
                msg = 'Received {code} response with empty body'.format(code=response.code)
                raise MarconiError(msg)

        d = content(response)
        d.addCallback(_raise_error)
        return d

    def _request(self, method, path, params=None, data=None):
        url = '{base_url}{path}'.format(
            base_url=self.base_url,
            path=path,
        )

        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'User-Agent': self.USER_AGENT,
            'Client-ID': self.client_id,
        }

        def _possibly_retry(failure):
            # Either I'm doing something wrong (likely) or Marconi is doing
            # something unpleasant to connections after it returns a 201 to us,
            # because the next request always seems to get one of these.
            if failure.check(*self.RETRYABLE_ERRORS):
                return self._request(method, path, params=params, data=data)
            else:
                return failure

        if data:
            body = QuieterFileBodyProducer(StringIO(json.dumps(data)))
        else:
            body = None

        d = self.http_client.request(method, url, headers=headers, data=body, params=params)
        d.addErrback(_possibly_retry)
        return d

    def _expect_204(self, response):
        if response.code == 204:
            return None
        else:
            return self._handle_error_response(response)

    def push_message(self, queue_name, body, ttl):
        path = '/v1/queues/{queue_name}/messages'.format(queue_name=queue_name)
        data = [
            {
                'ttl': ttl,
                'body': body,
            }
        ]

        def _construct_message(obj):
            return MarconiMessage(body=body, ttl=ttl, age=0, href=obj['resources'][0])

        def _on_response(response):
            if response.code == 201:
                return json_content(response).addCallback(_construct_message)
            else:
                return self._handle_error_response(response)

        d = self._request('POST', path, data=data)
        d.addCallback(_on_response)
        d.addErrback(self._wrap_error)
        return d

    def claim_message(self, queue_name, ttl, grace, polling_interval=1):
        path = '/v1/queues/{queue_name}/claims'.format(queue_name=queue_name)
        data = {
            'ttl': ttl,
            'grace': grace,
        }
        params = {
            'limit': 1,
        }

        d = defer.Deferred()

        def _construct_message(obj, response):
            claim_href = response.headers.getRawHeaders('location')[0]
            d.callback(ClaimedMarconiMessage(claim_href=claim_href, **obj[0]))

        def _on_response(response):
            if response.code == 201:
                json_content(response).addCallback(_construct_message, response)
            elif response.code == 204:
                reactor.callLater(polling_interval, _perform_call)
            else:
                return self._handle_error_response(response)

        def _perform_call():
            d1 = self._request('POST', path, data=data, params=params)
            d1.addCallback(_on_response)
            d1.addErrback(self._wrap_error)
            d1.addErrback(d.errback)

        _perform_call()
        return d

    def update_claim(self, claimed_message, ttl):
        data = {
            'ttl': ttl,
        }

        d = self._request('PATCH', claimed_message.claim_href, data=data)
        d.addCallback(self._expect_204)
        d.addErrback(self._wrap_error)
        return d

    def release_claim(self, claimed_message):
        d = self._request('DELETE', claimed_message.claim_href)
        d.addCallback(self._expect_204)
        d.addErrback(self._wrap_error)
        return d

    def delete_message(self, message):
        d = self._request('DELETE', message.href)
        d.addCallback(self._expect_204)
        d.addErrback(self._wrap_error)
        return d
