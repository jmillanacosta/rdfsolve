"""Tests for mine_all_sources batch mining.""""""Tests for mine_all_sources batch mining."""



import csvimport csv

import jsonimport json

import osimport os

from unittest.mock import patchfrom unittest.mock import patch



from rdfsolve.api import mine_all_sourcesfrom rdfsolve.api import mine_all_sources

from rdfsolve.models import AboutMetadata, MinedSchema, SchemaPatternfrom rdfsolve.models import AboutMetadata, MinedSchema, SchemaPattern



# The mock target – _mine is imported lazily inside mine_all_sources as# The mock target – _mine is imported lazily inside mine_all_sources as

# ``from .miner import mine_schema as _mine``, so we patch the source.# ``from .miner import mine_schema as _mine``, so we patch the source.

_MINE = "rdfsolve.miner.mine_schema"_MINE_TARGET = "rdfsolve.miner.mine_schema"





# -------------------------------------------------------------------# -------------------------------------------------------------------

# Helpers# Helpers

# -------------------------------------------------------------------# -------------------------------------------------------------------



def _write_sources_csv(path: str, rows: list[dict]) -> None:def _write_sources_csv(path: str, rows: list[dict]) -> None:

    """Write a minimal sources CSV."""    """Write a minimal sources CSV."""

    fieldnames = [    fieldnames = [

        "dataset_name", "void_iri", "graph_uri",        "dataset_name", "void_iri", "graph_uri",

        "endpoint_url", "use_graph",        "endpoint_url", "use_graph",

    ]    ]

    with open(path, "w", newline="") as fh:    with open(path, "w", newline="") as fh:

        writer = csv.DictWriter(fh, fieldnames=fieldnames)        writer = csv.DictWriter(fh, fieldnames=fieldnames)

        writer.writeheader()        writer.writeheader()

        for row in rows:        for row in rows:

            writer.writerow(row)            writer.writerow(row)





def _make_schema(name: str = "test") -> MinedSchema:def _make_schema(name: str = "test") -> MinedSchema:

    """Return a tiny MinedSchema for mocking."""    """Return a tiny MinedSchema for mocking."""

    return MinedSchema(    return MinedSchema(

        patterns=[        patterns=[

            SchemaPattern(            SchemaPattern(

                subject_class="http://example.org/A",                subject_class="http://example.org/A",

                property_uri="http://example.org/p",                property_uri="http://example.org/p",

                object_class="http://example.org/B",                object_class="http://example.org/B",

            ),            ),

        ],        ],

        about=AboutMetadata.build(        about=AboutMetadata.build(

            endpoint="http://example.org/sparql",            endpoint="http://example.org/sparql",

            dataset_name=name,            dataset_name=name,

            strategy="miner",            strategy="miner",

            pattern_count=1,            pattern_count=1,

        ),        ),

    )    )





# -------------------------------------------------------------------# -------------------------------------------------------------------

# Tests# Tests

# -------------------------------------------------------------------# -------------------------------------------------------------------



class TestMineAllSources:class TestMineAllSources:

    """Tests for mine_all_sources API function."""    """Tests for mine_all_sources API function."""



    def test_skips_rows_without_endpoint(self, tmp_path):    def test_skips_rows_without_endpoint(self, tmp_path):

        """Rows with empty endpoint_url should be skipped."""        """Rows with empty endpoint_url should be skipped."""

        csv_path = str(tmp_path / "sources.csv")        csv_path = str(tmp_path / "sources.csv")

        _write_sources_csv(csv_path, [        _write_sources_csv(csv_path, [

            {            {

                "dataset_name": "no_ep",                "dataset_name": "no_ep",

                "void_iri": "",                "void_iri": "",

                "graph_uri": "",                "graph_uri": "",

                "endpoint_url": "",                "endpoint_url": "",

                "use_graph": "False",                "use_graph": "False",

            },            },

        ])        ])



        result = mine_all_sources(        result = mine_all_sources(

            sources_csv=csv_path,            sources_csv=csv_path,

            output_dir=str(tmp_path / "out"),            output_dir=str(tmp_path / "out"),

        )        )



        assert result["skipped"] == ["no_ep"]        assert result["skipped"] == ["no_ep"]

        assert result["succeeded"] == []        assert result["succeeded"] == []

        assert result["failed"] == []        assert result["failed"] == []



    def test_succeeds_for_valid_source(self, tmp_path):    @patch(_MINE_TARGET, None)

        """A row with a valid endpoint should produce output files."""    def test_succeeds_for_valid_source(self, tmp_path):

        csv_path = str(tmp_path / "sources.csv")        """A row with a valid endpoint should produce output files."""

        _write_sources_csv(csv_path, [        csv_path = str(tmp_path / "sources.csv")

            {        _write_sources_csv(csv_path, [

                "dataset_name": "myds",            {

                "void_iri": "http://example.org/",                "dataset_name": "myds",

                "graph_uri": "http://example.org/g",                "void_iri": "http://example.org/",

                "endpoint_url": "http://example.org/sparql",                "graph_uri": "http://example.org/g",

                "use_graph": "True",                "endpoint_url": "http://example.org/sparql",

            },                "use_graph": "True",

        ])            },

        ])

        schema = _make_schema("myds")

        out_dir = str(tmp_path / "out")        schema = _make_schema("myds")

        out_dir = str(tmp_path / "out")

        with patch(_MINE, return_value=schema):

            result = mine_all_sources(        with patch(

                sources_csv=csv_path,            _MINE_TARGET, return_value=schema,

                output_dir=out_dir,        ):

                fmt="all",            result = mine_all_sources(

            )                sources_csv=csv_path,

                output_dir=out_dir,

        assert "myds" in result["succeeded"]                fmt="all",

        assert result["failed"] == []            )



        # Check that files were written        assert "myds" in result["succeeded"]

        assert (tmp_path / "out" / "myds_schema.jsonld").exists()        assert result["failed"] == []

        assert (tmp_path / "out" / "myds_void.ttl").exists()

        # Check that files were written

        # Verify JSON-LD content        assert (tmp_path / "out" / "myds_schema.jsonld").exists()

        with open(tmp_path / "out" / "myds_schema.jsonld") as f:        assert (tmp_path / "out" / "myds_void.ttl").exists()

            data = json.load(f)

        assert "@context" in data        # Verify JSON-LD content

        assert "@graph" in data        with open(tmp_path / "out" / "myds_schema.jsonld") as f:

        assert "@about" in data            data = json.load(f)

        assert "@context" in data

    def test_jsonld_only(self, tmp_path):        assert "@graph" in data

        """fmt='jsonld' should only produce .jsonld files."""        assert "@about" in data

        csv_path = str(tmp_path / "sources.csv")

        _write_sources_csv(csv_path, [    def test_jsonld_only(self, tmp_path):

            {        """fmt='jsonld' should only produce .jsonld files."""

                "dataset_name": "ds1",        csv_path = str(tmp_path / "sources.csv")

                "void_iri": "",        _write_sources_csv(csv_path, [

                "graph_uri": "",            {

                "endpoint_url": "http://example.org/sparql",                "dataset_name": "ds1",

                "use_graph": "False",                "void_iri": "",

            },                "graph_uri": "",

        ])                "endpoint_url": "http://example.org/sparql",

                "use_graph": "False",

        schema = _make_schema("ds1")            },

        out_dir = str(tmp_path / "out")        ])



        with patch(_MINE, return_value=schema):        schema = _make_schema("ds1")

            result = mine_all_sources(        out_dir = str(tmp_path / "out")

                sources_csv=csv_path,

                output_dir=out_dir,        with patch(

                fmt="jsonld",            "rdfsolve.api._mine", return_value=schema,

            )        ):

            result = mine_all_sources(

        assert "ds1" in result["succeeded"]                sources_csv=csv_path,

        assert (tmp_path / "out" / "ds1_schema.jsonld").exists()                output_dir=out_dir,

        assert not (tmp_path / "out" / "ds1_void.ttl").exists()                fmt="jsonld",

            )

    def test_void_only(self, tmp_path):

        """fmt='void' should only produce .ttl files."""        assert "ds1" in result["succeeded"]

        csv_path = str(tmp_path / "sources.csv")        assert (tmp_path / "out" / "ds1_schema.jsonld").exists()

        _write_sources_csv(csv_path, [        assert not (tmp_path / "out" / "ds1_void.ttl").exists()

            {

                "dataset_name": "ds2",    def test_void_only(self, tmp_path):

                "void_iri": "",        """fmt='void' should only produce .ttl files."""

                "graph_uri": "",        csv_path = str(tmp_path / "sources.csv")

                "endpoint_url": "http://example.org/sparql",        _write_sources_csv(csv_path, [

                "use_graph": "False",            {

            },                "dataset_name": "ds2",

        ])                "void_iri": "",

                "graph_uri": "",

        schema = _make_schema("ds2")                "endpoint_url": "http://example.org/sparql",

        out_dir = str(tmp_path / "out")                "use_graph": "False",

            },

        with patch(_MINE, return_value=schema):        ])

            result = mine_all_sources(

                sources_csv=csv_path,        schema = _make_schema("ds2")

                output_dir=out_dir,        out_dir = str(tmp_path / "out")

                fmt="void",

            )        with patch(

            "rdfsolve.api._mine", return_value=schema,

        assert "ds2" in result["succeeded"]        ):

        assert not (tmp_path / "out" / "ds2_schema.jsonld").exists()            result = mine_all_sources(

        assert (tmp_path / "out" / "ds2_void.ttl").exists()                sources_csv=csv_path,

                output_dir=out_dir,

    def test_handles_mining_failure(self, tmp_path):                fmt="void",

        """If the miner raises, the dataset goes to 'failed'."""            )

        csv_path = str(tmp_path / "sources.csv")

        _write_sources_csv(csv_path, [        assert "ds2" in result["succeeded"]

            {        assert not (tmp_path / "out" / "ds2_schema.jsonld").exists()

                "dataset_name": "bad",        assert (tmp_path / "out" / "ds2_void.ttl").exists()

                "void_iri": "",

                "graph_uri": "",    def test_handles_mining_failure(self, tmp_path):

                "endpoint_url": "http://bad.example.org/sparql",        """If the miner raises, the dataset goes to 'failed'."""

                "use_graph": "False",        csv_path = str(tmp_path / "sources.csv")

            },        _write_sources_csv(csv_path, [

        ])            {

                "dataset_name": "bad",

        with patch(                "void_iri": "",

            _MINE,                "graph_uri": "",

            side_effect=RuntimeError("connection refused"),                "endpoint_url": "http://bad.example.org/sparql",

        ):                "use_graph": "False",

            result = mine_all_sources(            },

                sources_csv=csv_path,        ])

                output_dir=str(tmp_path / "out"),

            )        with patch(

            "rdfsolve.api._mine",

        assert result["succeeded"] == []            side_effect=RuntimeError("connection refused"),

        assert len(result["failed"]) == 1        ):

        assert result["failed"][0]["dataset"] == "bad"            result = mine_all_sources(

        assert "connection refused" in result["failed"][0]["error"]                sources_csv=csv_path,

                output_dir=str(tmp_path / "out"),

    def test_multiple_sources_mixed(self, tmp_path):            )

        """Mix of skipped, succeeded, and failed sources."""

        csv_path = str(tmp_path / "sources.csv")        assert result["succeeded"] == []

        _write_sources_csv(csv_path, [        assert len(result["failed"]) == 1

            {        assert result["failed"][0]["dataset"] == "bad"

                "dataset_name": "skipped_one",        assert "connection refused" in result["failed"][0]["error"]

                "void_iri": "",

                "graph_uri": "",    def test_multiple_sources_mixed(self, tmp_path):

                "endpoint_url": "",        """Mix of skipped, succeeded, and failed sources."""

                "use_graph": "False",        csv_path = str(tmp_path / "sources.csv")

            },        _write_sources_csv(csv_path, [

            {            {

                "dataset_name": "good_one",                "dataset_name": "skipped_one",

                "void_iri": "",                "void_iri": "",

                "graph_uri": "",                "graph_uri": "",

                "endpoint_url": "http://good.example.org/sparql",                "endpoint_url": "",

                "use_graph": "False",                "use_graph": "False",

            },            },

            {            {

                "dataset_name": "bad_one",                "dataset_name": "good_one",

                "void_iri": "",                "void_iri": "",

                "graph_uri": "",                "graph_uri": "",

                "endpoint_url": "http://bad.example.org/sparql",                "endpoint_url": "http://good.example.org/sparql",

                "use_graph": "False",                "use_graph": "False",

            },            },

        ])            {

                "dataset_name": "bad_one",

        schema = _make_schema("good_one")                "void_iri": "",

                "graph_uri": "",

        def _side_effect(**kwargs):                "endpoint_url": "http://bad.example.org/sparql",

            if "bad" in kwargs.get("endpoint_url", ""):                "use_graph": "False",

                raise RuntimeError("timeout")            },

            return schema        ])



        with patch(_MINE, side_effect=_side_effect):        schema = _make_schema("good_one")

            result = mine_all_sources(

                sources_csv=csv_path,        def _side_effect(**kwargs):

                output_dir=str(tmp_path / "out"),            if "bad" in kwargs.get("endpoint_url", ""):

            )                raise RuntimeError("timeout")

            return schema

        assert result["skipped"] == ["skipped_one"]

        assert result["succeeded"] == ["good_one"]        with patch("rdfsolve.api._mine", side_effect=_side_effect):

        assert len(result["failed"]) == 1            result = mine_all_sources(

        assert result["failed"][0]["dataset"] == "bad_one"                sources_csv=csv_path,

                output_dir=str(tmp_path / "out"),

    def test_graph_uri_passed_when_use_graph(self, tmp_path):            )

        """When use_graph=True, graph_uri should be passed."""

        csv_path = str(tmp_path / "sources.csv")        assert result["skipped"] == ["skipped_one"]

        _write_sources_csv(csv_path, [        assert result["succeeded"] == ["good_one"]

            {        assert len(result["failed"]) == 1

                "dataset_name": "withgraph",        assert result["failed"][0]["dataset"] == "bad_one"

                "void_iri": "",

                "graph_uri": "http://example.org/mygraph",    def test_graph_uri_passed_when_use_graph(self, tmp_path):

                "endpoint_url": "http://example.org/sparql",        """When use_graph=True, graph_uri should be passed."""

                "use_graph": "True",        csv_path = str(tmp_path / "sources.csv")

            },        _write_sources_csv(csv_path, [

        ])            {

                "dataset_name": "withgraph",

        schema = _make_schema("withgraph")                "void_iri": "",

                "graph_uri": "http://example.org/mygraph",

        with patch(                "endpoint_url": "http://example.org/sparql",

            _MINE, return_value=schema,                "use_graph": "True",

        ) as mock_mine:            },

            mine_all_sources(        ])

                sources_csv=csv_path,

                output_dir=str(tmp_path / "out"),        schema = _make_schema("withgraph")

            )

        with patch(

        # Verify that graph_uris was passed            "rdfsolve.api._mine", return_value=schema,

        call_kwargs = mock_mine.call_args.kwargs        ) as mock_mine:

        assert call_kwargs["graph_uris"] == [            mine_all_sources(

            "http://example.org/mygraph"                sources_csv=csv_path,

        ]                output_dir=str(tmp_path / "out"),

            )

    def test_no_graph_uri_when_use_graph_false(self, tmp_path):

        """When use_graph=False, graph_uris should be None."""        # Verify that graph_uris was passed

        csv_path = str(tmp_path / "sources.csv")        call_kwargs = mock_mine.call_args.kwargs

        _write_sources_csv(csv_path, [        assert call_kwargs["graph_uris"] == [

            {            "http://example.org/mygraph"

                "dataset_name": "nograph",        ]

                "void_iri": "",

                "graph_uri": "http://example.org/g",    def test_no_graph_uri_when_use_graph_false(self, tmp_path):

                "endpoint_url": "http://example.org/sparql",        """When use_graph=False, graph_uris should be None."""

                "use_graph": "False",        csv_path = str(tmp_path / "sources.csv")

            },        _write_sources_csv(csv_path, [

        ])            {

                "dataset_name": "nograph",

        schema = _make_schema("nograph")                "void_iri": "",

                "graph_uri": "http://example.org/g",

        with patch(                "endpoint_url": "http://example.org/sparql",

            _MINE, return_value=schema,                "use_graph": "False",

        ) as mock_mine:            },

            mine_all_sources(        ])

                sources_csv=csv_path,

                output_dir=str(tmp_path / "out"),        schema = _make_schema("nograph")

            )

        with patch(

        call_kwargs = mock_mine.call_args.kwargs            "rdfsolve.api._mine", return_value=schema,

        assert call_kwargs["graph_uris"] is None        ) as mock_mine:

            mine_all_sources(

    def test_on_progress_callback(self, tmp_path):                sources_csv=csv_path,

        """The on_progress callback should be invoked."""                output_dir=str(tmp_path / "out"),

        csv_path = str(tmp_path / "sources.csv")            )

        _write_sources_csv(csv_path, [

            {        call_kwargs = mock_mine.call_args.kwargs

                "dataset_name": "cb_test",        assert call_kwargs["graph_uris"] is None

                "void_iri": "",

                "graph_uri": "",    def test_on_progress_callback(self, tmp_path):

                "endpoint_url": "http://example.org/sparql",        """The on_progress callback should be invoked."""

                "use_graph": "False",        csv_path = str(tmp_path / "sources.csv")

            },        _write_sources_csv(csv_path, [

        ])            {

                "dataset_name": "cb_test",

        schema = _make_schema("cb_test")                "void_iri": "",

        progress_calls: list[tuple] = []                "graph_uri": "",

                "endpoint_url": "http://example.org/sparql",

        def _cb(name, idx, total, err):                "use_graph": "False",

            progress_calls.append((name, idx, total, err))            },

        ])

        with patch(_MINE, return_value=schema):

            mine_all_sources(        schema = _make_schema("cb_test")

                sources_csv=csv_path,        progress_calls: list[tuple] = []

                output_dir=str(tmp_path / "out"),

                on_progress=_cb,        def _cb(name, idx, total, err):

            )            progress_calls.append((name, idx, total, err))



        assert len(progress_calls) == 1        with patch(

        assert progress_calls[0] == ("cb_test", 1, 1, None)            "rdfsolve.api._mine", return_value=schema,

        ):

    def test_creates_output_dir(self, tmp_path):            mine_all_sources(

        """Output directory should be created if it doesn't exist."""                sources_csv=csv_path,

        csv_path = str(tmp_path / "sources.csv")                output_dir=str(tmp_path / "out"),

        _write_sources_csv(csv_path, [                on_progress=_cb,

            {            )

                "dataset_name": "mkd",

                "void_iri": "",        assert len(progress_calls) == 1

                "graph_uri": "",        assert progress_calls[0] == ("cb_test", 1, 1, None)

                "endpoint_url": "http://example.org/sparql",

                "use_graph": "False",    def test_creates_output_dir(self, tmp_path):

            },        """Output directory should be created if it doesn't exist."""

        ])        csv_path = str(tmp_path / "sources.csv")

        _write_sources_csv(csv_path, [

        schema = _make_schema("mkd")            {

        deep_dir = str(tmp_path / "a" / "b" / "c")                "dataset_name": "mkd",

        assert not os.path.exists(deep_dir)                "void_iri": "",

                "graph_uri": "",

        with patch(_MINE, return_value=schema):                "endpoint_url": "http://example.org/sparql",

            mine_all_sources(                "use_graph": "False",

                sources_csv=csv_path,            },

                output_dir=deep_dir,        ])

            )

        schema = _make_schema("mkd")

        assert os.path.isdir(deep_dir)        deep_dir = str(tmp_path / "a" / "b" / "c")

        assert os.path.exists(        assert not os.path.exists(deep_dir)

            os.path.join(deep_dir, "mkd_schema.jsonld")

        )        with patch(

            "rdfsolve.api._mine", return_value=schema,
        ):
            mine_all_sources(
                sources_csv=csv_path,
                output_dir=deep_dir,
            )

        assert os.path.isdir(deep_dir)
        assert os.path.exists(
            os.path.join(deep_dir, "mkd_schema.jsonld")
        )
