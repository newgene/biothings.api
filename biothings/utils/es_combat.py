import elasticsearch
import requests
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
    from elastic_transport._models import DEFAULT
    from elasticsearch import ApiError  # pylint: disable=unused-import  # noqa


def get_es_transport_conf(klass=None):
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


class SnapshotCompat:
    @staticmethod
    def get_repository_args(name):
        repo_name_field = "repository" if IS_OLD_VERSION else "name"
        return {repo_name_field: name}

    @staticmethod
    def create_repository_args(name, **extra_settings):
        repo_name_field = "repository" if IS_OLD_VERSION else "name"
        if IS_OLD_VERSION:
            return {repo_name_field: name, "body": extra_settings}

        if "acl" in extra_settings:
            extra_settings["settings"]["canned_acl"] = extra_settings.pop("acl")
        if "region" in extra_settings:
            extra_settings["settings"]["region"] = extra_settings.pop("region")
        return {repo_name_field: name, **extra_settings}

    @staticmethod
    def delete_repository_args(name):
        repo_name_field = "repository" if IS_OLD_VERSION else "name"
        return {repo_name_field: name}

    @staticmethod
    def get_args(repo_name, snapshot_name, **extra_settings):
        return {
            "repository": repo_name,
            "snapshot": snapshot_name,
            **extra_settings,
        }

    @staticmethod
    def create_args(repo_name, snapshot_name, **extra_settings):
        return SnapshotCompat.get_args(repo_name, snapshot_name, **extra_settings)

    @staticmethod
    def delete_args(repo_name, snapshot_name, **extra_settings):
        return SnapshotCompat.get_args(repo_name, snapshot_name, **extra_settings)
