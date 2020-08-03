# coding=utf-8
from __future__ import unicode_literals

import asyncio, aiohttp
import hashlib
import os
from datetime import datetime, timedelta
from functools import wraps
from urllib.parse import urlparse, urlsplit
from typing import Dict, List, Any, Tuple

from . import utils


class SelectelCDNApiException(Exception):

    def __init__(self, message, *args, **kwargs):
        message = "Selectel: {}".format(message)
        self.response = kwargs.pop('response', None)
        super(SelectelCDNApiException, self).__init__(message)


class SelectelCDNApi(object):
    """Класс для работы с API Облачного хранилища Selectel

        На текущий момент с помощью API можно выполнять следующие операции:
            * получать содержимое контейнера;
            * загружать файлы в хранилище;
            * удалять файлы из хранилища;

        DOC:
            `Документация по API Selecte <https://kb.selectel.ru/22058988.html>`_
    """

    def __init__(self, cloud_url, threshold=None, max_retry=None, retry_delay=None):
        parse_result = urlparse(cloud_url)
        replaced = parse_result._replace(netloc="{}".format(parse_result.hostname))
        self.user = parse_result.username
        self.password = parse_result.password
        self.auth_url = replaced.geturl()
        self.threshold = threshold or 0
        self.max_retry = max_retry
        self.retry_delay = retry_delay

        self._token_expire_dt = None
        self._storage_url = None
        self._session = None

    def update_expired_token(fn):
        @wraps(fn)
        async def wrapper(self, *args, **kwargs):
            if await self.is_token_expire or not self._session:
                await self.authenticate()
            try:
                response = await fn(self, *args, **kwargs)
            except (aiohttp.ClientError, SelectelCDNApiException) as e:
                if e.response.status == 401:
                    await self.authenticate()
                    response = await fn(self, *args, **kwargs)
                else:
                    raise e
            await self.close_session()
            return response

        return wrapper

    def attempts(fn):
        @wraps(fn)
        async def wrapper(self, *args, **kwargs):
            if self.max_retry is not None:
                retries = self.max_retry
                while retries > 1:
                    try:
                        return await fn(self, *args, **kwargs)
                    except (aiohttp.ClientError, SelectelCDNApiException):
                        retries -= 1
                        await asyncio.sleep(self.retry_delay)
                response = await fn(self, *args, **kwargs)
                return response

        return wrapper

    @property
    async def is_token_expire(self):
        if not self._token_expire_dt:
            return True

        return (self._token_expire_dt - datetime.now()).total_seconds() < self.threshold

    @update_expired_token
    async def get_url(self, container, path):
        return os.path.join(self._storage_url, container, path)

    async def close_session(self):
        await self._session.close()
        self._session = None

    async def authenticate(self):
        if not self.user or not self.password:
            raise SelectelCDNApiException("Not set user or password")
        headers = {
            "X-Auth-User": self.user,
            "X-Auth-Key": self.password
        }
        async with aiohttp.ClientSession() as session:
            async with session.get(self.auth_url, headers=headers) as resp:
                if resp.status != 204:
                    raise SelectelCDNApiException("Authenticate error ({})".format(resp.status))
        self._token_expire_dt = datetime.now() + timedelta(seconds=int(resp.headers['X-Expire-Auth-Token']))
        self._storage_url = resp.headers['X-Storage-Url']

        self._session = aiohttp.ClientSession(headers={
            "X-Auth-Token": resp.headers['X-Auth-Token']
        })

    @attempts
    @update_expired_token
    async def get(self, path_cloud: str, headers: Dict = None) -> bytes:
        """Получение файлов из контейнера

            Args:
                path_cloud: пусть до файла
                headers: дополнительные заголовки
            Return:
                Возвращает бинарные данные
        """
        container, path = self._parse_path(path_cloud)
        url = os.path.join(self._storage_url, container, path)
        if headers is None:
            headers = {}
        response = await self._session.get(url, headers=headers)
        try:
            response.raise_for_status()
        except Exception as e:
            await self.close_session()
            raise SelectelCDNApiException("Error get file {}: {}".format(url, str(e)), response=response)
        return await response.read()

    @attempts
    @update_expired_token
    async def get_steam(self, path_cloud: str, headers: Dict = None, chunk: int = 2 ** 20) -> Any:
        """Получение файла из контейнера частями (chunk)

            Args:
                path_cloud: пусть до файла
                force: пропустить генерацию exception в случае ошибки (отсутствие файла..)
            Return:
                Заголовки ответа
        """
        container, path = self._parse_path(path_cloud)
        url = os.path.join(self._storage_url, container, path)
        if headers is None:
            headers = {}
        r = await self._session.get(url, headers=headers)
        try:
            r.raise_for_status()
        except Exception as e:
            await self.close_session()
            raise SelectelCDNApiException("Error get file {}: {}".format(url, str(e)), response=r)
        return r.content.iter_chunked(chunk)

    @attempts
    @update_expired_token
    async def remove(self, path_cloud: str, force: bool = False) -> bool:
        """Удаление файла из контейнера

            Args:
                path_cloud: пусть до файла
                force: пропустить генерацию exception в случае ошибки (отсутствие файла..)
            Return:
                Заголовки ответа
        """
        container, path = self._parse_path(path_cloud)
        url = os.path.join(self._storage_url, container, path)
        r = await self._session.delete(url)
        if force:
            if r.status == 404:
                return True
        try:
            r.raise_for_status()
            assert r.status == 204
        except Exception as e:
            await self.close_session()
            raise SelectelCDNApiException("Error remove file {}: {}".format(url, str(e)), response=r)
        return True

    @attempts
    @update_expired_token
    async def put(self, path_cloud: str, content: bytes, headers: Dict = None) -> bool:
        """Загрузка файла в контейнер

            Args:
                path_cloud: пусть до файла
            Return:
                True/except
            """
        container, path = self._parse_path(path_cloud)
        url = os.path.join(self._storage_url, container, path)
        if headers is None:
            headers = {}
        if utils.is_py3():
             etag = hashlib.md5(content.encode('utf8') if hasattr(content, 'encode') else content).hexdigest()
        else:
            etag = hashlib.md5(content).hexdigest()
        headers["ETag"] = etag
        r = await self._session.put(url, data=content, headers=headers)
        try:
            r.raise_for_status()
            assert r.status == 201
        except Exception as e:
            await self.close_session()
            raise SelectelCDNApiException("Error create file {}: {}".format(url, str(e)), response=r)
        return True

    @attempts
    @update_expired_token
    async def exist(self, path_cloud: str) -> bool:
        """Проверка существования файла в контейнере

            Args:
                path_cloud: пусть до файла
            Return:
                True/False
        """
        container, path = self._parse_path(path_cloud)
        url = os.path.join(self._storage_url, container, path)
        r = await self._session.head(url)
        if r.status not in (200, 404):
            r.raise_for_status()
        return r.status == 200

    @attempts
    @update_expired_token
    async def size(self, path_cloud: str) -> int:
        """Размер файла

            Args:
                path_cloud: пусть до файла
            Return:
                Content-Length
        """
        container, path = self._parse_path(path_cloud)
        url = os.path.join(self._storage_url, container, path)
        r = await self._session.head(url)
        r.raise_for_status()
        if r.status != 200:
            await self.close_session()
            raise SelectelCDNApiException("file {} not exists".format(os.path.sep.join([container, path])), response=r)
        return int(r.headers['Content-Length'])

    def _parse_path(self, path: str) -> Tuple:
        splited_path = path.split(os.path.sep)
        if len(splited_path) > 1:
            return splited_path[0], os.path.sep.join(splited_path[1:])
        return splited_path[0], ""

    @attempts
    @update_expired_token
    async def list(self, container_name: str, headers: Dict = None) -> List:
        """Получение списка файлов в контейнере

            Args:
                container_name: имя контейнера
            Return:
                Возвращает список файлов, находящихся в указанном контейнере.
                Список объектов ограничен 10000. Воспользуйтесь query-параметрами marker и limit для гибкого
                получения списков объектов в контейнере
            """
        url = os.path.join(self._storage_url, container_name)
        if headers is None:
            headers = {}
        response = await self._session.get(url, headers=headers)
        try:
            response.raise_for_status()
        except Exception as e:
            await self.close_session()
            raise SelectelCDNApiException("Error get file {}: {}".format(url, str(e)), response=response)
        return await response.text()
