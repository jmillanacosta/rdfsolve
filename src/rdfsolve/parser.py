"""VoID (Vocabulary of Interlinked Datasets) Parser.

Parses an in-memory VoID RDF graph and converts the embedded schema
to various downstream formats (JSON-LD, LinkML, SHACL, RDF-config,
DataFrame).

It also serves as the entry-point for *discovering* VoID catalogs at
live SPARQL endpoints (``discover_void_graphs``) and for turning raw
partition records back into an RDF graph
(``build_void_graph_from_partitions``).
"""

import logging
from hashlib import md5
from typing import Any, cast

import pandas as pd
from linkml_runtime.linkml_model import SchemaDefinition
from rdflib import Graph, Literal, URIRef

# Create logger with NullHandler by default , no output unless user configures
logger = logging.getLogger(__name__)
if not logger.handlers:
    logger.addHandler(logging.NullHandler())


class VoidParser:
    """Parser for VoID (Vocabulary of Interlinked Datasets) files."""

    def __init__(
        self,
        void_source: str | Graph | None = None,
        graph_uris: str | list[str] | None = None,
        exclude_graphs: bool = True,
    ):
        """Initialize the VoID parser.

        Args:
            void_source: File path (str) or RDF Graph object
            graph_uris: Graph URI(s) to analyze, or None for all non-system graphs
            exclude_graphs: Exclude Virtuoso system graphs
        """
        self.void_file_path: str | None = None
        self.graph: Graph = Graph()
        self.schema_triples: list[Any] = []
        self.classes: dict[str, Any] = {}
        self.properties: dict[str, Any] = {}
        self.graph_uris = self._normalize_graph_uris(graph_uris)
        self.exclude_graphs = exclude_graphs
        self.exclude_graph_patterns: list[str] | None = None

        # VoID namespace URIs
        self.void_class = URIRef("http://rdfs.org/ns/void#class")
        self.void_property = URIRef("http://rdfs.org/ns/void#property")
        self.void_propertyPartition = URIRef("http://rdfs.org/ns/void#propertyPartition")
        self.void_classPartition = URIRef("http://rdfs.org/ns/void#classPartition")
        self.void_datatypePartition = URIRef("http://ldf.fi/void-ext#datatypePartition")

        # Bind common namespace prefixes
        self.void_ns = "http://rdfs.org/ns/void#"
        self.void_ext_ns = "http://ldf.fi/void-ext#"
        # Extended VoID properties for schema
        self.void_subjectClass = URIRef("http://ldf.fi/void-ext#subjectClass")
        self.void_objectClass = URIRef("http://ldf.fi/void-ext#objectClass")

        if void_source:
            if isinstance(void_source, str):
                self.void_file_path = void_source
                self._load_graph()
            elif isinstance(void_source, Graph):
                self.graph = void_source

    def _normalize_graph_uris(self, graph_uris: str | list[str] | None) -> list[str] | None:
        """Normalize graph URIs input to a list."""
        if graph_uris is None:
            return None
        elif isinstance(graph_uris, str):
            return [graph_uris]
        elif isinstance(graph_uris, list):
            return graph_uris
        else:
            raise ValueError("graph_uris must be str, list of str, or None")

    def _load_graph(self) -> None:
        """Load the VoID file into an RDF graph."""
        self.graph.parse(self.void_file_path, format="turtle")

    def _extract_classes(self) -> None:
        """Extract class information from VoID description."""
        self.classes = {}
        for s, _p, o in self.graph.triples((None, self.void_class, None)):
            self.classes[s] = o

    def _extract_properties(self) -> None:
        """Extract property information from VoID description."""
        self.properties = {}
        for s, _p, o in self.graph.triples((None, self.void_property, None)):
            self.properties[s] = o

    def _extract_schema_triples(self) -> None:
        """Extract schema triples by analyzing property partitions."""
        self.schema_triples = []

        # Try new ty extraction first (with subjectClass/objectClass)
        triples = self._extract_schema()
        if triples:
            self.schema_triples = triples
            return

    def _extract_schema(self) -> list[Any]:
        """Extract schema from property partitions with type info."""
        triples: list[Any] = []

        # Find all property partitions with subject/object class info
        for partition, _, property_uri in self.graph.triples((None, self.void_property, None)):
            # Get subject class
            subject_classes = list(self.graph.triples((partition, self.void_subjectClass, None)))
            # Get object class
            object_classes = list(self.graph.triples((partition, self.void_objectClass, None)))

            if subject_classes and object_classes:
                for _, _, subject_class in subject_classes:
                    for _, _, object_class in object_classes:
                        triples.append((subject_class, property_uri, object_class))
            elif subject_classes:
                # Check for datatype partitions (literal objects)
                datatype_partitions = list(
                    self.graph.triples((partition, self.void_datatypePartition, None))
                )
                if datatype_partitions:
                    for _, _, subject_class in subject_classes:
                        triples.append((subject_class, property_uri, "Literal"))
                else:
                    # No explicit datatype or object class - assume Resource
                    for _, _, subject_class in subject_classes:
                        triples.append((subject_class, property_uri, "Resource"))

        return triples

    def _filter_void_admin_nodes(self, df: pd.DataFrame) -> pd.DataFrame:
        """Filter out VoID-related triples."""
        mask = (
            ~df["subject_uri"].str.contains("void", case=False, na=False)
            & ~df["property_uri"].str.contains("void", case=False, na=False)
            & ~df["object_uri"].str.contains("void", case=False, na=False)
            & ~df["subject_uri"].str.contains("well-known", case=False, na=False)
            & ~df["property_uri"].str.contains("well-known", case=False, na=False)
            & ~df["object_uri"].str.contains("well-known", case=False, na=False)
            & ~df["subject_uri"].str.contains("openlink", case=False, na=False)
            & ~df["property_uri"].str.contains("openlink", case=False, na=False)
            & ~df["object_uri"].str.contains("openlink", case=False, na=False)
        )
        return df[mask].copy()

    def _extract_about_metadata(
        self,
        endpoint_url: str | None = None,
        dataset_name: str | None = None,
        graph_uris: list[str] | None = None,
    ) -> dict[str, Any]:
        """Extract metadata from the VoID graph for the @about section.

        Pulls metadata from the VoID graph (endpoint, title, graph URIs)
        and merges with any explicitly provided values.

        Args:
            endpoint_url: SPARQL endpoint URL (overrides graph value)
            dataset_name: Dataset name (overrides graph value)
            graph_uris: Graph URIs (overrides graph value)

        Returns:
            Dictionary with metadata for the @about section
        """
        from datetime import datetime, timezone

        from rdfsolve.version import VERSION

        about: dict[str, Any] = {
            "generatedBy": f"rdfsolve {VERSION}",
            "generatedAt": datetime.now(timezone.utc).isoformat(),
        }

        # Try to extract metadata from the VoID graph
        void_dataset_type = URIRef("http://rdfs.org/ns/void#Dataset")
        void_sparql_endpoint = URIRef("http://rdfs.org/ns/void#sparqlEndpoint")
        dcterms_title = URIRef("http://purl.org/dc/terms/title")

        graph_endpoint = None
        graph_title = None
        graph_graph_uris: list[str] = []

        for s, p, o in self.graph:
            if (
                p == URIRef("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")
                and o == void_dataset_type
            ):
                # Found a void:Dataset - extract its properties
                for _, pred, obj in self.graph.triples((s, None, None)):
                    if pred == void_sparql_endpoint:
                        graph_endpoint = str(obj)
                    elif pred == dcterms_title:
                        graph_title = str(obj)

        # Collect graph URIs from the parser
        if self.graph_uris:
            graph_graph_uris = list(self.graph_uris)

        # Use explicit values, fall back to graph values
        if endpoint_url:
            about["endpoint"] = endpoint_url
        elif graph_endpoint:
            about["endpoint"] = graph_endpoint

        if dataset_name:
            about["datasetName"] = dataset_name
        elif graph_title:
            about["datasetName"] = graph_title

        effective_graph_uris = graph_uris if graph_uris else graph_graph_uris
        if effective_graph_uris:
            about["graphURIs"] = effective_graph_uris

        if self.void_file_path:
            about["voidFile"] = self.void_file_path

        about["tripleCount"] = len(self.schema_triples) if self.schema_triples else 0

        return about

    def to_jsonld(
        self,
        filter_void_admin_nodes: bool = True,
        endpoint_url: str | None = None,
        dataset_name: str | None = None,
        graph_uris: list[str] | None = None,
    ) -> dict[str, Any]:
        """
        Parse VoID file and return simple JSON-LD with the schema triples.

        Args:
            filter_void_admin_nodes: Whether to filter out VoID-specific nodes
            endpoint_url: SPARQL endpoint URL for the @about section
            dataset_name: Dataset name for the @about section
            graph_uris: Graph URIs for the @about section

        Returns:
            Simple JSON-LD with @context, @graph, and @about sections
        """
        # Extract schema triples
        self._extract_schema_triples()

        if not self.schema_triples:
            about = self._extract_about_metadata(
                endpoint_url=endpoint_url,
                dataset_name=dataset_name,
                graph_uris=graph_uris,
            )
            return {"@context": {}, "@graph": [], "@about": about}

        # Create minimal context for the namespaces we find
        context: dict[str, str] = {}
        triples: list[dict[str, Any]] = []

        for s, p, o in self.schema_triples:
            # Convert to CURIEs and collect namespaces
            s_curie, s_prefix, s_namespace = self._get_curie_and_namespace(str(s))
            p_curie, p_prefix, p_namespace = self._get_curie_and_namespace(str(p))

            # Add prefixes to context
            if s_prefix and s_namespace:
                context[s_prefix] = s_namespace
            if p_prefix and p_namespace:
                context[p_prefix] = p_namespace

            # Handle object
            o_value: str | dict[str, str]
            if isinstance(o, Literal):
                # It's a literal value
                if o.datatype:
                    o_value = {"@value": str(o), "@type": str(o.datatype)}
                else:
                    o_value = str(o)
            else:
                # It's a URI/Resource
                o_curie, o_prefix, o_namespace = self._get_curie_and_namespace(str(o))
                if o_prefix and o_namespace:
                    context[o_prefix] = o_namespace
                o_value = {"@id": o_curie if o_curie else str(o)}

            # Create simple triple as JSON-LD
            triple = {
                "@id": s_curie if s_curie else str(s),
                p_curie if p_curie else str(p): o_value,
            }
            triples.append(triple)

        # Group triples by subject
        grouped: dict[str, dict[str, Any]] = {}
        for triple in triples:
            subject_id: str = cast(str, triple["@id"])
            if subject_id not in grouped:
                grouped[subject_id] = {"@id": subject_id}

            # Merge properties
            for key, value in triple.items():
                if key != "@id":
                    if key in grouped[subject_id]:
                        # Convert to array if not already
                        if not isinstance(grouped[subject_id][key], list):
                            grouped[subject_id][key] = [grouped[subject_id][key]]
                        # Add new value if not duplicate
                        if value not in grouped[subject_id][key]:
                            grouped[subject_id][key].append(value)
                    else:
                        grouped[subject_id][key] = value

        # Build @about metadata section
        about = self._extract_about_metadata(
            endpoint_url=endpoint_url,
            dataset_name=dataset_name,
            graph_uris=graph_uris,
        )

        # Return simple JSON-LD
        return {"@context": context, "@graph": list(grouped.values()), "@about": about}

    def _create_context(self) -> dict[str, str]:
        """Create JSON-LD @context."""
        # Start with standard W3C vocabularies (should not be needed anymore)
        context = {
            # Core RDF vocabularies
            "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
            "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
            "owl": "http://www.w3.org/2002/07/owl#",
            "xsd": "http://www.w3.org/2001/XMLSchema#",
            # Metadata vocabularies
            "dcterms": "http://purl.org/dc/terms/",
            "dc": "http://purl.org/dc/elements/1.1/",
            "prov": "http://www.w3.org/ns/prov#",
            "foaf": "http://xmlns.com/foaf/0.1/",
            "skos": "http://www.w3.org/2004/02/skos/core#",
            "schema": "https://schema.org/",
            # VoID and SHACL for schema description
            "void": "http://rdfs.org/ns/void#",
            "sh": "http://www.w3.org/ns/shacl#",
            # Common biological/chemical ontologies (clean URIs)
            "go": "http://purl.obolibrary.org/obo/GO_",
            "chebi": "http://purl.obolibrary.org/obo/CHEBI_",
            "pato": "http://purl.obolibrary.org/obo/PATO_",
            "ncit": "http://ncicb.nci.nih.gov/xml/owl/EVS/Thesaurus.owl#",
            "cheminf": "http://semanticscience.org/resource/CHEMINF_",
        }

        # Add prefixes from VoID graph namespace manager
        if self.graph and hasattr(self.graph, "namespace_manager"):
            for prefix, namespace in self.graph.namespace_manager.namespaces():
                if prefix and namespace and str(prefix) not in context:
                    # Only add if it's a valid URI and not already present
                    ns_str = str(namespace)
                    if ns_str.startswith(("http://", "https://", "urn:")):
                        context[str(prefix)] = ns_str

        return context

    def _extract_context(self) -> dict[str, str]:
        """Extract @context from VoID graph and common namespaces."""
        return self._create_context()

    def _filter_jsonld_void_admin_nodes(self, jsonld: dict[str, Any]) -> dict[str, Any]:
        """Filter out VoID administrative nodes from JSON-LD structure."""
        void_patterns = [
            "void",
            "rdfs",
            "rdf",
            "owl",
            "skos",
            "foaf",
            "dcterms",
            "dc",
            "prov",
            "schema",
        ]

        # Handle @graph structure
        if "@graph" in jsonld:
            filtered_graph = []

            for item in jsonld["@graph"]:
                # Keep dataset description (first item)
                if item.get("@type") == "void:Dataset":
                    filtered_graph.append(item)
                    continue

                # Keep schema pattern statements, S-P-O relationships
                if "void:SchemaPattern" in item.get("@type", []):
                    filtered_graph.append(item)
                    continue

                # Filter other items based on @id patterns
                item_id = item.get("@id", "").lower()
                if not any(void_pat in item_id for void_pat in void_patterns):
                    filtered_graph.append(item)

            jsonld_filtered = jsonld.copy()
            jsonld_filtered["@graph"] = filtered_graph
            return jsonld_filtered

        # Return as-is if no recognized structure
        return jsonld

    def _get_curie_and_namespace(self, uri: str) -> tuple[str, str, str]:
        """Get CURIE representation and extract prefix/namespace info.

        Args:
            uri: The URI to convert

        Returns:
            Tuple of (curie, prefix, namespace_uri).
        """
        import re

        curie = None
        prefix = None
        namespace_uri = None

        # First try bioregistry conversion
        if uri.startswith(("http://", "https://")):
            try:
                from bioregistry import curie_from_iri, parse_iri

                parsed = parse_iri(uri)
                if parsed:
                    prefix, local_id = parsed
                    if local_id in uri:
                        idx = uri.rfind(local_id)
                        namespace_uri = uri[:idx]
                    elif "#" in uri:
                        namespace_uri = uri.rsplit("#", 1)[0] + "#"
                    else:
                        namespace_uri = uri.rsplit("/", 1)[0] + "/"

                    curie = curie_from_iri(uri)
                    if not curie and prefix and local_id:
                        curie = f"{prefix}:{local_id}"

            except Exception as e:
                logger.debug("Bioregistry failed for %s: %s", uri, e)

        # Fallback to string manipulation
        if not curie:
            if "#" in uri:
                namespace_part, local_part = uri.rsplit("#", 1)
                namespace_uri = namespace_part + "#"
            elif "/" in uri:
                namespace_part, local_part = uri.rsplit("/", 1)
                namespace_uri = namespace_part + "/"
            else:
                local_part = uri

            if not prefix and namespace_uri:
                clean_uri = namespace_uri.replace(
                    "http://",
                    "",
                ).replace("https://", "")
                clean_uri = (
                    clean_uri.replace(
                        "www.",
                        "",
                    )
                    .strip("/")
                    .strip("#")
                )
                if "/" in clean_uri:
                    parts = clean_uri.split("/")
                    prefix = parts[-1] if parts[-1] else parts[-2] if len(parts) > 1 else "ns"
                else:
                    prefix = clean_uri.split(".")[0] if "." in clean_uri else clean_uri
                prefix = re.sub(r"[^a-zA-Z0-9_]", "", prefix)[:10]

            curie = f"{prefix}:{local_part}" if prefix and local_part else uri

        return curie or uri, prefix or "", namespace_uri or ""

    def _extract_schema_patterns_from_triples(self) -> list[dict[str, str]]:
        """
        Extract schema patterns from the internal schema triples.
        This creates the schema_patterns structure expected by other methods.

        Returns:
            List of schema pattern dictionaries
        """
        if not hasattr(self, "schema_triples") or not self.schema_triples:
            return []

        patterns = []
        for subject_uri, property_uri, object_uri in self.schema_triples:
            # Convert URIs to CURIEs for display
            subject_curie, _, _ = self._get_curie_and_namespace(str(subject_uri))
            property_curie, _, _ = self._get_curie_and_namespace(str(property_uri))
            object_curie, _, _ = self._get_curie_and_namespace(str(object_uri))

            patterns.append(
                {
                    "subject_class": subject_curie,
                    "subject_uri": str(subject_uri),
                    "property": property_curie,
                    "property_uri": str(property_uri),
                    "object_class": object_curie,
                    "object_uri": str(object_uri),
                }
            )

        return patterns

    def to_schema(self, filter_void_admin_nodes: bool = True) -> pd.DataFrame:
        """
        Parse VoID file and return schema as pandas DataFrame.
        This method now uses the JSON-LD generation as the source of truth.

        Args:
            filter_void_admin_nodes: Whether to filter out VoID-specific nodes

        Returns:
            DataFrame with schema information including CURIEs
        """
        # Ensure schema is extracted (populates self.schema_triples)
        self._extract_schema_triples()

        # Get schema patterns from the internal triples
        schema_patterns = self._extract_schema_patterns_from_triples()

        if not schema_patterns:
            return pd.DataFrame()

        # Convert to DataFrame
        df = pd.DataFrame(schema_patterns)

        # Apply filtering if requested
        if filter_void_admin_nodes:
            df = self._filter_void_admin_nodes(df)

        return df

    def to_linkml(
        self,
        filter_void_nodes: bool = True,
        schema_name: str | None = None,
        schema_description: str | None = None,
        schema_base_uri: str | None = None,
        jsonld_override: dict[str, Any] | None = None,
    ) -> SchemaDefinition:
        """Generate LinkML schema from JSON-LD representation.

        See :func:`rdfsolve.schema_models.linkml.to_linkml` for full
        documentation.
        """
        from rdfsolve.schema_models.linkml import (
            to_linkml as _to_linkml,
        )

        jsonld = (
            jsonld_override if jsonld_override is not None else self.to_jsonld(filter_void_nodes)
        )
        return _to_linkml(
            jsonld,
            schema_name=schema_name,
            schema_description=schema_description,
            schema_base_uri=schema_base_uri,
        )

    def to_linkml_yaml(
        self,
        filter_void_nodes: bool = True,
        schema_name: str | None = None,
        schema_description: str | None = None,
        schema_base_uri: str | None = None,
    ) -> str:
        """Return LinkML schema as YAML string.

        See :func:`rdfsolve.schema_models.linkml.to_linkml_yaml`.
        """
        from rdfsolve.schema_models.linkml import (
            to_linkml_yaml as _to_linkml_yaml,
        )

        jsonld = self.to_jsonld(filter_void_nodes)
        return _to_linkml_yaml(
            jsonld,
            schema_name=schema_name,
            schema_description=schema_description,
            schema_base_uri=schema_base_uri,
        )

    def to_shacl(
        self,
        filter_void_nodes: bool = True,
        schema_name: str | None = None,
        schema_description: str | None = None,
        schema_base_uri: str | None = None,
        closed: bool = True,
        suffix: str | None = None,
        include_annotations: bool = False,
    ) -> str:
        """Generate SHACL shapes from VoID schema.

        See :func:`rdfsolve.schema_models.shacl.to_shacl`.
        """
        from rdfsolve.schema_models.shacl import (
            to_shacl as _to_shacl,
        )

        jsonld = self.to_jsonld(filter_void_nodes)
        return _to_shacl(
            jsonld,
            schema_name=schema_name,
            schema_description=schema_description,
            schema_base_uri=schema_base_uri,
            closed=closed,
            suffix=suffix,
            include_annotations=include_annotations,
        )

    def to_rdfconfig(
        self,
        filter_void_nodes: bool = True,
        endpoint_url: str | None = None,
        endpoint_name: str | None = None,
        graph_uri: str | None = None,
    ) -> dict[str, str]:
        """Generate RDF-config YAML files.

        See :func:`rdfsolve.schema_models.rdfconfig.to_rdfconfig`.
        """
        from rdfsolve.schema_models.rdfconfig import (
            to_rdfconfig as _to_rdfconfig,
        )

        jsonld = self.to_jsonld(filter_void_nodes)
        return _to_rdfconfig(
            jsonld,
            endpoint_url=endpoint_url,
            endpoint_name=endpoint_name,
            graph_uri=graph_uri,
        )

    # ------------------------------------------------------------------
    # VoID catalog discovery
    # ------------------------------------------------------------------

    def discover_void_graphs(self, endpoint_url: str) -> dict[str, Any]:
        """Discover VoID graphs at *endpoint_url* via a SELECT query.

        Queries for VoID partitions across all named graphs.  Returns a
        dict describing found graphs and their raw partition records,
        which can be passed directly to
        :meth:`build_void_graph_from_partitions`.

        Args:
            endpoint_url: SPARQL endpoint URL.

        Returns:
            Dict with keys ``has_void_descriptions``, ``found_graphs``,
            ``total_graphs``, ``void_content``, ``partitions``.
            On failure an ``error`` key is added and counts are zero.
        """
        from rdfsolve.sparql_helper import SparqlHelper

        query = """
        PREFIX void: <http://rdfs.org/ns/void#>
        PREFIX void-ext: <http://ldf.fi/void-ext#>
        SELECT DISTINCT ?subjectClass ?prop ?objectClass ?objectDatatype ?g
        WHERE {
          GRAPH ?g {
            {
              ?cp void:class ?subjectClass ;
                  void:propertyPartition ?pp .
              ?pp void:property ?prop .
              OPTIONAL {
                {
                  ?pp void:classPartition [ void:class ?objectClass ] .
                } UNION {
                  ?pp void-ext:datatypePartition
                      [ void-ext:datatype ?objectDatatype ] .
                }
              }
            } UNION {
              ?ls void:subjectsTarget [ void:class ?subjectClass ] ;
                  void:linkPredicate ?prop ;
                  void:objectsTarget [ void:class ?objectClass ] .
            }
          }
        }
        """
        try:
            helper = SparqlHelper(endpoint_url)
            results = helper.select(query, purpose="void/partition-discovery")

            found_graphs: list[str] = []
            void_content: dict[str, dict[str, Any]] = {}
            partitions: list[dict[str, str]] = []

            for row in results["results"]["bindings"]:
                g = row.get("g", {}).get("value")
                if not g:
                    continue
                if g not in void_content:
                    void_content[g] = {
                        "partition_count": 0,
                        "has_any_partitions": True,
                    }
                    found_graphs.append(g)
                void_content[g]["partition_count"] += 1

                p: dict[str, str] = {
                    "graph": g,
                    "subjectClass": row.get("subjectClass", {}).get("value", ""),
                    "prop": row.get("prop", {}).get("value", ""),
                }
                if row.get("objectClass", {}).get("value"):
                    p["objectClass"] = row["objectClass"]["value"]
                if row.get("objectDatatype", {}).get("value"):
                    p["objectDatatype"] = row["objectDatatype"]["value"]
                partitions.append(p)

            return {
                "has_void_descriptions": bool(found_graphs),
                "found_graphs": found_graphs,
                "total_graphs": len(found_graphs),
                "void_content": void_content,
                "partitions": partitions,
            }
        except Exception as exc:
            logger.info("VoID discovery failed: %s", exc)
            return {
                "has_void_descriptions": False,
                "found_graphs": [],
                "total_graphs": 0,
                "void_content": {},
                "partitions": [],
                "error": str(exc),
            }

    def build_void_graph_from_partitions(
        self,
        partitions: list[dict[str, str]],
        base_uri: str | None = None,
    ) -> Graph:
        """Convert raw partition records into an RDF VoID graph.

        Partition records are the dicts produced by
        :meth:`discover_void_graphs` (keys: ``subjectClass``, ``prop``,
        optionally ``objectClass`` / ``objectDatatype``).

        Args:
            partitions: Partition records from :meth:`discover_void_graphs`.
            base_uri: Base URI for generated blank-node-replacement IRIs.

        Returns:
            RDF :class:`~rdflib.Graph` with VoID partition triples.
        """
        VOID = "http://rdfs.org/ns/void#"
        VOID_EXT = "http://ldf.fi/void-ext#"
        base = base_uri or "urn:void:partition:"

        void_graph = Graph()
        void_graph.bind("void", URIRef(VOID))
        void_graph.bind("void-ext", URIRef(VOID_EXT))

        class_partitions: dict[str, URIRef] = {}

        for part in partitions:
            sc = part.get("subjectClass", "")
            prop = part.get("prop", "")
            if not sc or not prop:
                continue

            if sc not in class_partitions:
                cp_uri = URIRef(f"{base}class_{md5(sc.encode()).hexdigest()[:12]}")
                class_partitions[sc] = cp_uri
                void_graph.add((cp_uri, URIRef(f"{VOID}class"), URIRef(sc)))

            cp_uri = class_partitions[sc]
            oc = part.get("objectClass", "")
            dt = part.get("objectDatatype", "")
            pp_key = f"{sc}_{prop}_{oc or dt}"
            pp_uri = URIRef(f"{base}prop_{md5(pp_key.encode()).hexdigest()[:12]}")

            void_graph.add((cp_uri, URIRef(f"{VOID}propertyPartition"), pp_uri))
            void_graph.add((pp_uri, URIRef(f"{VOID}property"), URIRef(prop)))
            void_graph.add(
                (
                    pp_uri,
                    URIRef(f"{VOID_EXT}subjectClass"),
                    URIRef(sc),
                )
            )

            if oc:
                if oc not in class_partitions:
                    oc_uri = URIRef(f"{base}class_{md5(oc.encode()).hexdigest()[:12]}")
                    class_partitions[oc] = oc_uri
                    void_graph.add((oc_uri, URIRef(f"{VOID}class"), URIRef(oc)))
                oc_uri = class_partitions[oc]
                void_graph.add(
                    (
                        pp_uri,
                        URIRef(f"{VOID}classPartition"),
                        oc_uri,
                    )
                )
                void_graph.add(
                    (
                        pp_uri,
                        URIRef(f"{VOID_EXT}objectClass"),
                        URIRef(oc),
                    )
                )
            elif dt:
                dt_uri = URIRef(f"{base}dtype_{md5(dt.encode()).hexdigest()[:12]}")
                void_graph.add(
                    (
                        pp_uri,
                        URIRef(f"{VOID_EXT}datatypePartition"),
                        dt_uri,
                    )
                )
                void_graph.add((dt_uri, URIRef(f"{VOID_EXT}datatype"), URIRef(dt)))

        logger.debug(
            "Built VoID graph: %d triples from %d partitions",
            len(void_graph),
            len(partitions),
        )
        return void_graph
