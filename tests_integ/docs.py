from elasticmagic import Document
from elasticmagic import Field
from elasticmagic.types import List, Long, Text


class ProductDoc(Document):
    __doc_type__ = 'product'

    model = Field(Text)
    attrs = Field(List(Long))
    attrs_bool = Field(List(Long))
    attrs_range = Field(List(Long))
