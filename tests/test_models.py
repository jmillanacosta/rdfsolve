from rdfsolve.models import SchemaPattern
from rdfsolve.schema_models import AboutMetadata, MinedSchema


def _schema(*patterns: SchemaPattern) -> MinedSchema:
    return MinedSchema(patterns=list(patterns), about=AboutMetadata(dataset_name="test"))


DATA = SchemaPattern(
    subject_class="http://ex.org/Person",
    property_uri="http://ex.org/name",
    object_class="Literal",
    count=7,
    graphs={"https://example.org/data": 7},
)


def test_per_graph_counts_survive_the_canonical_round_trip():
    schema = _schema(DATA)
    restored = MinedSchema.from_dict(schema.to_dict())
    assert restored.patterns[0].graphs == {"https://example.org/data": 7}
