#!/usr/bin/python

import os
import pytest
import tempfile
import logging
from PIL import Image

from .. import utils
from ..selectel_storage import SelectelCDNApiException

if utils.is_py3():
    from io import StringIO, BytesIO
else:
    from StringIO import StringIO

logger = logging.getLogger('selectel_api.test')
SELECTEL_CONTAINER_PATH = "dev"

@pytest.mark.asyncio
async def test_put(resource):
    client = resource
    result = await client.exist(f"{SELECTEL_CONTAINER_PATH}/test1.png")
    if result:
        return True

    size = (1000, 1000)
    client = resource

    script_dir = os.path.dirname(os.path.abspath(__file__))
    image = os.path.join(script_dir, 'test1.png')
    try:
        original_image = Image.open(image)
    except IOError as error:
        logger.info(f'Cannot identify image file: {error.__repr__()}, {error.__class__}, {error.args}')
        raise
    tmp_file = tempfile.TemporaryFile()

    if original_image.format == 'JPEG':
        original_image.save(tmp_file, 'PNG')
        img = Image.open(tmp_file)
    else:
        img = original_image

    img_format = img.format
    resized_img = img.resize(size, Image.ANTIALIAS)
    if utils.is_py3():
        buffer = BytesIO()
    else:
        buffer = StringIO()


    resized_img.save(buffer, format=img_format)
    buffer = buffer.getvalue()

    headers = {'Content-Type': f'image/{img_format.lower()}',
                'X-Object-Meta': ''
               }
    result = await client.put("mcd-dev/test1.png", buffer, headers=headers)
    assert result == True

@pytest.mark.asyncio
async def test_delete(resource):
    print('test_delete')
    client = resource
    result = await client.remove(f"{SELECTEL_CONTAINER_PATH}/test1.png")
    assert result == True

@pytest.mark.asyncio
async def test_exist(resource):
    print('test_exist')
    client = resource
    result = await client.exist(f"{SELECTEL_CONTAINER_PATH}/not_exist_file.png")
    assert result == False

    await test_put(client)

    result = await client.exist(f"{SELECTEL_CONTAINER_PATH}/test1.png")
    assert result == True

    await test_delete(client)
    result = await client.exist(f"{SELECTEL_CONTAINER_PATH}/test1.png")
    assert result == False

@pytest.mark.asyncio
async def test_list(resource):
    print('test_list')
    client = resource
    result = await client.list("mcd-dev")
    assert len(result) > 100


@pytest.mark.asyncio
async def test_get_content(resource):
    print('test_get_content')
    client = resource
    await test_put(resource)
    result = await client.get(f"{SELECTEL_CONTAINER_PATH}/test1.png")

    assert type(result) == bytes
    try:
        await client.get(f"{SELECTEL_CONTAINER_PATH}/not_exist_file.png")
    except SelectelCDNApiException as e:
        assert str(e).startswith("Selectel: Error get file")


@pytest.mark.asyncio
async def test_get_steam(resource):
    print('test_get_steam')
    client = resource
    await test_put(resource)
    result = await client.get_steam(f"{SELECTEL_CONTAINER_PATH}/test1.png", chunk=10)
    async for chunk in result:
        assert len(chunk) <= 10
        break

    try:
        await client.get_steam(f"{SELECTEL_CONTAINER_PATH}/not_exist_file.png")
    except SelectelCDNApiException as e:
        assert str(e).startswith("Selectel: Error get file")


