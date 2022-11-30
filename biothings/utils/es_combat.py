import elasticsearch
import requests
from elastic_transport._models import DEFAULT
from requests_aws4auth import AWS4Auth

IS_OLD_VERSION = elasticsearch.VERSION[0] < 8

if IS_OLD_VERSION:
    from elasticsearch import (
        AIOHttpConnection,
        ElasticsearchException as ApiError,
        RequestsHttpConnection as TransportClass,
    )  # pylint: disable=unused-import
else:
    from elastic_transport import AiohttpHttpNode, RequestsHttpNode as TransportClass
    from elasticsearch import ApiError  # pylint: disable=unused-import  # noqa


def get_es_transport_conf(klass):
    if not klass:
        klass = TransportClass
    if IS_OLD_VERSION:
        return {"connection_class": klass}
    return {"node_class": klass}


class AsyncConnMixin:
    def __init__(self, *args, **kwargs):
        self.aws_auth = None
        if isinstance(kwargs.get("http_auth"), AWS4Auth):
            self.aws_auth = kwargs["http_auth"]
            kwargs["http_auth"] = None
        super().__init__(*args, **kwargs)

    def update_headers(self, method, host, url, body=None, params=None, headers=None):
        req = requests.PreparedRequest()
        req.prepare(method, host + url, headers, None, data=body, params=params)
        self.aws_auth(req)  # sign the request
        headers = headers or {}
        headers.update(req.headers)
        return headers


if IS_OLD_VERSION:
    class AsyncTransportClass(AsyncConnMixin, AIOHttpConnection):
        async def perform_request(
            self, method, url, params=None, body=None, timeout=None, ignore=(), headers=None
        ):
            headers = self.update_headers(
                method, self.host, url, body=body, params=params, headers=headers
            )
            return await super().perform_request(
                method, url, params, body, timeout, ignore, headers
            )

else:
    class AsyncTransportClass(AsyncConnMixin, AiohttpHttpNode):
        async def perform_request(
            self,
            method,
            target,
            body=None,
            headers=None,
            request_timeout=DEFAULT,
        ):
            return await super().perform_request(method, target, body, headers, request_timeout)


