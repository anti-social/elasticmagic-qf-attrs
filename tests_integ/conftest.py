import os
import uuid
from itertools import count

from elasticmagic.ext.asyncio import AsyncCluster

from elasticsearch_async import AsyncElasticsearch

import pytest

from .attrs import Country
from .attrs import Display
from .attrs import Manufacturer
from .attrs import Waterproof
from .docs import ProductDoc


@pytest.fixture
def es_url():
    return 'http://localhost:9200'


@pytest.fixture
def index_name():
    return 'test-{}'.format(str(uuid.uuid4()).split('-')[0])


@pytest.fixture
async def es_client(es_url):
    es_url = os.environ.get('ES_URL', es_url)
    es_client = AsyncElasticsearch([es_url])
    yield es_client
    await es_client.transport.close()


@pytest.fixture
def es_cluster(es_client):
    yield AsyncCluster(es_client)


@pytest.fixture
async def es_index(es_cluster, es_client, index_name):
    await es_client.indices.create(index=index_name)
    es_index = es_cluster[index_name]
    await es_index.put_mapping(ProductDoc)
    yield es_index
    await es_client.indices.delete(index=index_name)


@pytest.fixture
async def products(es_index):
    ids = count(1)
    products = [
        ProductDoc(
            _id=next(ids),
            model='Iphone XS',
            attrs=[Manufacturer.apple, Country.usa],
            attrs_bool=[Waterproof.no()],
            attrs_range=[Display.value(6.5)],
        ),
        ProductDoc(
            _id=next(ids),
            model='Galaxy A20',
            attrs=[Manufacturer.samsung, Country.korea],
            attrs_bool=[Waterproof.no()],
            attrs_range=[Display.value(6.4)],
        ),
        ProductDoc(
            _id=next(ids),
            model='P smart Z',
            attrs=[Manufacturer.huawei, Country.china],
            attrs_bool=[Waterproof.no()],
            attrs_range=[Display.value(6.59)],
        ),
        ProductDoc(
            _id=next(ids),
            model='P30 Pro',
            attrs=[Manufacturer.huawei, Country.china],
            attrs_bool=[Waterproof.yes()],
            attrs_range=[Display.value(6.47)],
        ),
        ProductDoc(
            _id=next(ids),
            model='Mi MIX',
            attrs=[Manufacturer.xiaomi, Country.china],
            attrs_range=[Display.value(6.4)],
        ),
    ]
    yield await es_index.add(products, refresh=True)
