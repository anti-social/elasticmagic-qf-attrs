Elasticmagic query filters for attributes
=========================================

Library to store, filter and build facets for custom attributes

The problem
-----------

Each attribute pair can be stored in the index as a nested document.
We can use following mapping for that:

```yaml
attrs:
  type: nested
  properties:
    attr_id:
      type: integer
    # usually only one of the next fields should be populated
    value_int:
      type: integer
    value_bool:
      type: boolean
    value_float:
      type: float
```

This makes it possible to filter documents by an attribute id and its value
(for example we want to find all the documents with `attr_id = 1234` and `value = 5678`):

```yaml
query:
  bool:
    filter:
    - nested:
        path: attrs
        query:
          bool:
            must:
            - term:
                attrs.attr_id: 1234
            - term:
                attrs.value_int: 5678
```

It is also possible to build a facets for all attributes at once:

```yaml
aggs:
  attrs_nested:
    nested:
      path: attrs
    aggs:
      attrs:
        terms:
          field: attrs.attr_id
        aggs:
          values:
            field: attrs.value_int
```

or for a single attribute:

```yaml
aggs:
  attrs_nested:
    nested:
      path: attrs
    aggs:
      attr_1234:
        filter:
          term:
            attrs.attr_id: 1234
        aggs:
          values:
            field: attrs.value_int
```

But nested documents have some drawbacks. Every nested document is stored
in the index as different document. For instance, next document will be stored
as 5 lucene documents:

```yaml
name: "I'm a document with nested attributes"
attrs:
- attr_id: 1
  value_int: 42
- attr_id: 2
  value_int: 43
- attr_id: 3
  value_bool: true
- attr_id: 4
  value_float: 99.9
```

Nested queries are slow by itself:
   
> In particular, joins should be avoided. nested can make queries several times
slower and parent-child relations can make queries hundreds of times slower.
> - https://www.elastic.co/guide/en/elasticsearch/reference/master/tune-for-search-speed.html#_document_modeling

But what is worse regular queries are also slower when there are nested documents
in the index. It is because of all the fields of main documents becomes sparse.
This in turn degrades performance of all filters and accesses to doc_values.

The solution
------------

The idea is to encode pair of an attribute id and a corresponding value into
a single value. If our attribute ids are 32-bit integers and all value types
also fit into 32 bits we can store them as a single 64-bit value.

So our mapping can be:

```yaml
attrs:
  type: object
  properties:
    int:
      type: long
    bool:
      type: long
    float:
      type: long
```

Document with encoded attributes:

```yaml
name: "I'm a document with packed attributes"
attrs:
# (1 << 32) | 42
- int: 0x1_0000002a
# (2 << 32) | 43
- int: 0x2_0000002b
# (3 << 1) | 1
- bool: 0x7
# (4 << 32) | {integer representation of 99.9}
# (4 << 32) | struct.unpack('=I', struct.pack('=f', 99.9))[0]
- float: 0x4_42c7cccd
```

Now with a bit of bit magic we can emulate nested queries.

Filtering by attribute id `1234` with value `5678`:

```yaml
query:
  bool:
    filter:
    - term:
        attrs.int: 0x4d2_0000162e
```

Building facet for all attribute values:

```yaml
aggs:
  attrs_int:
    terms:
      field: attrs.int
      # specify big enough aggregation size
      # so all flat attrite values should fit
      size: 10000
```

One more step that client should do is to decode and group values by
attribute id.

How to use it in python
-----------------------

```python
from elasticsearch import Elasticsearch

from elasticmagic import Cluster, Document, Field
from elasticmagic.types import List, Long
from elasticmagic.ext.queryfilter import QueryFilter

from elasticmagic_qf_attrs import AttrBoolFacetFilter
from elasticmagic_qf_attrs import AttrIntFacetFilter
from elasticmagic_qf_attrs import AttrRangeFacetFilter
from elasticmagic_qf_attrs.util import merge_attr_value_bool
from elasticmagic_qf_attrs.util import merge_attr_value_float
from elasticmagic_qf_attrs.util import merge_attr_value_int

# Specify document
class AttrsDocument(Document):
    __doc_type__ = 'attrs'

    ints = Field(List(Long))
    bools = Field(List(Long))
    floats = Field(List(Long))

# Create an index
index_name = 'test-attrs'
client = Elasticsearch()
client.indices.create(index=index_name)
cluster = Cluster(client)
index = cluster.get_index(index_name)
index.put_mapping(AttrsDocument)

# Index example document
index.add([
    AttrsDocument(
        ints=[
            merge_attr_value_int(1, 42),
            merge_attr_value_int(2, 43),
        ],
        bools=[merge_attr_value_bool(3, True)],
        floats=[merge_attr_value_float(4, 99.9)],
    ),
], refresh=True)

# Define a query filter
class AttrsQueryFilter(QueryFilter):
    ints = AttrIntFacetFilter(AttrsDocument.ints, alias='a')
    bools = AttrBoolFacetFilter(AttrsDocument.bools, alias='a')
    ranges = AttrRangeFacetFilter(AttrsDocument.floats, alias='a')

# Now we can build facets
qf = AttrsQueryFilter()
sq = index.search_query()
sq = qf.apply(sq, {})
res = sq.get_result()
assert res.total == 1
qf_res = qf.process_result(res)

# And finally lets print results
for attr_id, facet in qf_res.ints.facets.items():
    print(f'> {attr_id}:')
    for facet_value in facet.all_values:
        print(f'  {facet_value.value}: ({facet_value.count_text})')

for attr_id, facet in qf_res.bools.facets.items():
    print(f'> {attr_id}:')
    for facet_value in facet.all_values:
        print(f'  {facet_value.value}: ({facet_value.count_text})')

for attr_id, facet in qf_res.ranges.facets.items():
    print(f'> {attr_id}: ({facet.count})')

# Also we can filter documents:
qf = AttrsQueryFilter()
sq = index.search_query()
sq = qf.apply(
    sq,
    {
        'a1': '42',
        'a3': 'true',
        'a4__lte': '100',
    }
)
res = sq.get_result()
assert res.total == 1

qf = AttrsQueryFilter()
sq = index.search_query()
sq = qf.apply(
    sq,
    {
        'a4__gte': '100',
    }
)
res = sq.get_result()
assert res.total == 0
```

This script should print:

```
> 1:
  42: (1)
> 2:
  43: (1)
> 3:
  True: (1)
> 4: (1)
```
