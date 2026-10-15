"""Build LinkML classes and slots directly from observed schema patterns."""

from __future__ import annotations

import re
from collections import defaultdict
from collections.abc import Iterable
from hashlib import sha256
from importlib.resources import files
from typing import TYPE_CHECKING, Any, cast

from bioregistry import curie_from_iri
from linkml_runtime.linkml_model import (
    ClassDefinition,
    SchemaDefinition,
    SlotDefinition,
    TypeDefinition,
)
from linkml_runtime.linkml_model.meta import AnonymousSlotExpression, Example

if TYPE_CHECKING:
    from rdfsolve.schema_models.core import MinedSchema
    from rdfsolve.schema_models.pattern import SchemaPattern


def _clean_local_part(local: str) -> str:
    """Clean the local part of a name while preserving structure.

    Examples::

        "KeyEvent"    -> "KeyEvent"
        "data1025"    -> "data_1025"
        "edam.data1025" -> "edam_data_1025"
        "C123456"     -> "C_123456"
    """
    local = local.replace(".", "_")
    local = re.sub(r"([a-zA-Z])(\d)", r"\1_\2", local)
    local = re.sub(r"([a-z])([A-Z])", r"\1_\2", local)
    local = re.sub(r"[^a-zA-Z0-9_]", "_", local)
    return local


def _finalize_linkml_name(name: str) -> str:
    """Apply final cleanup rules to ensure valid LinkML identifier."""
    name = re.sub(r"_+", "_", name)
    name = name.strip("_")
    if name and name[0].isdigit():
        name = f"item_{name}"
    elif not name or not name[0].isalpha():
        name = f"item_{name}" if name else "unknown_item"
    if not name:
        name = "unknown_item"
    return name


def make_valid_linkml_name(uri_or_curie: str) -> str:
    """Convert a URI or CURIE to a valid LinkML identifier.

    LinkML identifiers must start with a letter and contain only
    letters, digits, and underscores.

    Examples::

        "aopo:KeyEvent"           -> "aopo_KeyEvent"
        "edam.data1025"           -> "edam_data_1025"
        "http://example.org/Cls"  -> prefix_Cls  (via bioregistry)
        "40:Something"            -> "item_40_Something"
    """
    if uri_or_curie.startswith(("http://", "https://")):
        curie = curie_from_iri(uri_or_curie)
        if curie:
            uri_or_curie = curie

    if ":" in uri_or_curie:
        prefix, local = uri_or_curie.split(":", 1)
        # Clean prefix and ensure it starts with letter/underscore
        prefix = re.sub(r"[^a-zA-Z0-9_]", "_", prefix)
        prefix = _finalize_linkml_name(prefix)  # Fix invalid prefix names
        local = _clean_local_part(local)
        local = _finalize_linkml_name(local)  # Fix invalid local parts
        name = f"{prefix}_{local}"
    else:
        name = _clean_local_part(uri_or_curie)

    return _finalize_linkml_name(name)


# Core conversion

SH = "http://www.w3.org/ns/shacl#"
DASH = "http://datashapes.org/dash#"
RDF_LANG = "http://www.w3.org/1999/02/22-rdf-syntax-ns#langString"
RDFS_LABEL = "http://www.w3.org/2000/01/rdf-schema#label"
XSD = "http://www.w3.org/2001/XMLSchema#"
# XSD datatypes that a LinkML type of linkml:types has as its uri.
LINKML_TYPES = {
    XSD + "string": "string",
    XSD + "integer": "integer",
    XSD + "boolean": "boolean",
    XSD + "float": "float",
    XSD + "double": "double",
    XSD + "decimal": "decimal",
    XSD + "time": "time",
    XSD + "date": "date",
    XSD + "dateTime": "datetime",
    XSD + "anyURI": "uri",
    # LinkML has no type of its own for xsd:int, and asks for integer (no_xsd_int_type).
    XSD + "int": "integer",
}
# The LinkML type that other XSD datatypes restrict, for the types made for them.
XSD_BASES = dict.fromkeys(
    (
        "long",
        "short",
        "byte",
        "nonNegativeInteger",
        "nonPositiveInteger",
        "positiveInteger",
        "negativeInteger",
        "unsignedLong",
        "unsignedInt",
        "unsignedShort",
        "unsignedByte",
    ),
    "integer",
)


def _curie_name(iri: str, prefixes: dict[str, str], *, slot: bool) -> str:
    """Return the LinkML name of a term from its CURIE alone (``schema_name``,
    ``dct_conforms_to``; a class in UpperCamelCase, ``SchemaSoftwareApplication``):
    the longest namespace of *prefixes* that the IRI starts with, else Bioregistry's CURIE, else
    the IRI's last part after a hash of its namespace. A term keeps its name whatever else the
    schema holds.
    """
    namespace = max(
        (ns for ns in prefixes.values() if iri.startswith(ns) and ns != iri), key=len, default=None
    )
    if namespace is not None:
        prefix = min(p for p, ns in prefixes.items() if ns == namespace)
        local = iri[len(namespace) :]
    else:
        curie = curie_from_iri(iri) if iri.startswith(("http://", "https://")) else None
        if curie:
            prefix, local = curie.split(":", 1)
        else:
            local = re.split(r"[/#:]", iri)[-1]
            prefix = "ns" + sha256(iri[: len(iri) - len(local)].encode()).hexdigest()[:6]
    if slot:
        from rdfsolve.schema_models.exporters.pydantic import _identifier

        prefix = re.sub(r"[^A-Za-z0-9_]", "_", prefix).lower()
        return _finalize_linkml_name(f"{prefix}_{_identifier(local)}" if local else prefix)
    return _camel(f"{prefix} {local}")


def _camel(text: str) -> str:
    """Return *text* in UpperCamelCase, keeping the inner capitals of each word."""
    name = "".join(w[:1].upper() + w[1:] for w in re.findall(r"[A-Za-z0-9]+", text))
    return name if name[:1].isalpha() else "C" + name


def _names(
    iris: list[str], prefixes: dict[str, str], reserved: set[str], *, slot: bool
) -> dict[str, str]:
    """Return the name of each term; terms whose names coincide each take a hash of their IRI."""
    first = {iri: _curie_name(iri, prefixes, slot=slot) for iri in sorted(set(iris))}
    counts: dict[str, int] = {}
    for name in first.values():
        counts[name] = counts.get(name, 0) + 1
    return {
        iri: name + "_" + sha256(iri.encode()).hexdigest()[:8]
        if counts[name] > 1 or name in reserved
        else name
        for iri, name in first.items()
    }


# Examples written for each class and slot: they show values, the schema JSON keeps them all.
EXAMPLES = 5


def _first(values: Iterable[str]) -> list[str]:
    """Return the first EXAMPLES distinct values, in order."""
    return list(dict.fromkeys(values))[:EXAMPLES]


def _profile_rules(schema: MinedSchema) -> tuple[Any, dict[str, dict[str, dict[str, Any]]]]:
    """Return the declared shapes (the shape profile and retained source shapes) as a graph, and
    their rules by class IRI and path IRI: what each property shape of a node shape with a
    sh:targetClass states.
    """
    from rdflib import Graph, URIRef
    from rdflib.collection import Collection

    graph = Graph(bind_namespaces="none")
    if schema.shape_profile:
        graph.parse(data=schema.shape_profile, format="turtle")
    if schema.shapes is not None:
        graph += schema.shapes.to_rdf()
    sh = {
        name: URIRef(SH + name)
        for name in (
            "targetClass",
            "property",
            "path",
            "minCount",
            "maxCount",
            "class",
            "datatype",
            "nodeKind",
            "pattern",
            "in",
            "hasValue",
            "or",
            "severity",
            "message",
            "order",
            "group",
            "name",
            "description",
            "Violation",
            "deactivated",
        )
    }
    rules: dict[str, dict[str, dict[str, Any]]] = {}
    for node, cls in graph.subject_objects(sh["targetClass"]):
        if not isinstance(cls, URIRef):
            continue
        for shape in graph.objects(node, sh["property"]):
            path = graph.value(shape, sh["path"])
            if not isinstance(path, URIRef) or graph.value(shape, sh["deactivated"]) is not None:
                continue
            rule = rules.setdefault(str(cls), {}).setdefault(str(path), {})

            def one(name: str, shape: Any = shape) -> Any:
                """Return the one value of a SHACL predicate on the property shape."""
                return graph.value(shape, sh[name])

            for name in ("minCount", "maxCount", "order"):
                value = one(name)
                if value is not None:
                    rule[name] = int(value)
            for name in ("class", "datatype", "nodeKind", "pattern", "group", "message"):
                value = one(name)
                if value is not None:
                    rule[name] = str(value)
            for name in ("name", "description"):
                value = one(name)
                if value is not None:
                    rule[name] = str(value)
            severity = one("severity")
            if severity is not None and severity != sh["Violation"]:
                rule["severity"] = str(severity)
            listed = one("in")
            if listed is not None:
                rule["in"] = list(Collection(graph, listed))
            fixed = one("hasValue")
            if fixed is not None:
                rule["hasValue"] = fixed
            alternatives = one("or")
            if alternatives is not None:
                rule["or"] = [
                    str(graph.value(member, sh["class"]))
                    for member in Collection(graph, alternatives)
                    if graph.value(member, sh["class"]) is not None
                ]
            dash = {
                str(p)[len(DASH) :]: str(v)
                for p, v in graph.predicate_objects(shape)
                if str(p).startswith(DASH)
            }
            if dash:
                rule["dash"] = dash
    return graph, rules


def to_linkml(
    schema: MinedSchema,
    *,
    schema_name: str | None = None,
    schema_description: str | None = None,
    schema_base_uri: str | None = None,
) -> SchemaDefinition:
    """Return a LinkML schema of a mined schema.

    Classes and slots are named from their CURIEs, so a term keeps its name from one release
    to the next; class_uri and slot_uri are full IRIs. The ranges are the observed ones: a
    class of the schema, a LinkML type for an XSD datatype (others, rdf:langString included,
    get a type of their own named from their CURIE), uriorcurie for IRIs without a type. Each
    class's slot_usage takes what the shapes of the schema state (the shape profile and the
    retained source shapes): required (sh:minCount; recommended when the shape's severity is
    not sh:Violation), single-valued (sh:maxCount 1) or maximum_cardinality, the class of links
    (sh:class, sh:or of classes), pattern, enums (sh:in, an IRI sh:hasValue), equals_string,
    rank (sh:order), slot_group (sh:group), title (sh:name), description, and sh:message and
    DASH as annotations. Where the shapes state no count, the counts of the schema that prove
    one (exporters.shacl.counted_cardinalities) give required and single-valued. A class's
    nearest superclass among the schema's classes is its is_a.
    """
    from linkml_runtime.linkml_model.meta import EnumDefinition, PermissibleValue
    from linkml_runtime.utils.schemaview import SchemaView
    from rdflib import Literal as RdfLiteral
    from rdflib import URIRef

    from rdfsolve.schema_models.exporters.shacl import counted_cardinalities

    name = make_valid_linkml_name(schema_name or schema.about.dataset_name or "rdf_schema")
    base = schema_base_uri or schema.about.schema_uri or f"https://w3id.org/{name}/"
    declared, rules = _profile_rules(schema)
    types_view = SchemaView(
        str(files("linkml_runtime").joinpath("linkml_model/model/schema/types.yaml"))
    )
    builtin_types = set(types_view.all_types())
    builtin_prefixes = {
        p.prefix_prefix: p.prefix_reference for p in types_view.schema.prefixes.values()
    }
    prefixes = {
        **{str(p): str(ns) for p, ns in declared.namespaces()},
        **schema.get_prefixes(),
    }
    # The schema's own namespaces; a prefix that linkml:types gives another namespace (its
    # schema: is http://schema.org/) is declared as "<prefix>_ns".
    curie_prefixes = {
        (p if builtin_prefixes.get(p, ns) == ns else f"{p}_ns"): ns
        for p, ns in sorted(prefixes.items())
        if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_.-]*", p) and p not in (name, "linkml")
    }

    def as_curie(iri: str) -> str:
        """Return an IRI as a CURIE whose local part holds no colon, as LinkML splits CURIEs at
        their colon: the longest namespace that gives one, else a prefix added for the IRI up
        to its last "#", "/" or ":". The prefixes stay the same for the same IRIs.
        """
        found = [
            (len(ns), key)
            for key, ns in curie_prefixes.items()
            if iri.startswith(ns) and ":" not in iri[len(ns) :] and iri != ns
        ]
        if found:
            key = max(found)[1]
            return f"{key}:{iri[len(curie_prefixes[key]) :]}"
        cut = max(iri.rfind(c) for c in "#/:")
        namespace, local = iri[: cut + 1], iri[cut + 1 :]
        if not local or ":" not in namespace:
            return iri
        base_key = re.sub(r"[^A-Za-z0-9_]", "_", namespace.rstrip("#/:").rsplit("/", 1)[-1])
        key = base_key if re.match(r"[A-Za-z]", base_key) else f"ns_{base_key}"
        if curie_prefixes.get(key, namespace) != namespace or key in (name, "linkml"):
            key = f"{key}_{sha256(namespace.encode()).hexdigest()[:6]}"
        curie_prefixes[key] = namespace
        return f"{key}:{local}"

    profile_classes = set(rules)
    linked = {
        target
        for paths in rules.values()
        for rule in paths.values()
        for target in ([rule["class"]] if "class" in rule else []) + rule.get("or", [])
    }
    class_iris = sorted(set(schema.get_classes()) | profile_classes | linked)
    property_iris = sorted(
        set(schema.get_properties()) | {p for paths in rules.values() for p in paths}
    )
    class_names = _names(class_iris, prefixes, builtin_types, slot=False)
    slot_names = _names(
        property_iris, prefixes, builtin_types | set(class_names.values()), slot=True
    )
    types: dict[str, TypeDefinition] = {}
    enums: dict[str, EnumDefinition] = {}

    def label(iri: str) -> str | None:
        """Return the title of a term: its label in the schema's enrichment."""
        return schema.enrichment.label(iri)

    # A class's nearest superclasses among the schema's classes: the first is its is_a.
    parents = {
        child: [p for p in found if p in class_names]
        for child, found in schema.class_hierarchy.items()
        if child in class_names
    }
    classes = {
        iri: ClassDefinition(
            name=class_name,
            class_uri=as_curie(iri),
            title=label(iri),
            is_a=class_names[parents[iri][0]] if parents.get(iri) else None,
            annotations=(
                {"rdfsolve_superclasses": " ".join(parents[iri])}
                if len(parents.get(iri, [])) > 1
                else {}
            ),
            slots=[],
            description=schema.enrichment.description(iri),
            examples=[
                Example(value=v)
                for v in _first(t.value for t in schema.enrichment.class_examples.get(iri, []))
            ],
        )
        for iri, class_name in class_names.items()
    }

    def datatype_range(datatype: str) -> str:
        """Return the LinkML type of a literal datatype."""
        if datatype in LINKML_TYPES:
            return LINKML_TYPES[datatype]
        type_name = _curie_name(datatype, prefixes, slot=True)
        if type_name in builtin_types or type_name in class_names.values():
            type_name += "_type"
        base_type = XSD_BASES.get(datatype.removeprefix(XSD), "string")
        types[type_name] = TypeDefinition(name=type_name, uri=as_curie(datatype), typeof=base_type)
        return type_name

    def value_range(pattern: SchemaPattern) -> str:
        """Choose a LinkML range for one observed pattern."""
        if pattern.object_class in class_names:
            return class_names[pattern.object_class]
        if pattern.object_class == "Resource":
            return "uriorcurie"
        if pattern.object_class == "BlankNode" or not pattern.datatype:
            return "string"
        return datatype_range(pattern.datatype)

    ranges: dict[str, set[str]] = {}
    class_ranges: dict[tuple[str, str], set[str]] = {}
    untyped_ranges: dict[str, set[str]] = {}
    for pattern in schema.patterns:
        value = value_range(pattern)
        ranges.setdefault(pattern.property_uri, set()).add(value)
        # Untyped subjects have no class: their properties are slots of the UntypedSubject mixin.
        if pattern.untyped_subject:
            untyped_ranges.setdefault(pattern.property_uri, set()).add(value)
        else:
            class_ranges.setdefault((pattern.subject_class, pattern.property_uri), set()).add(value)

    def constraints(values: set[str]) -> dict[str, Any]:
        """Represent one range or a set of alternative ranges."""
        ordered = sorted(values)
        return (
            {"range": ordered[0]}
            if len(ordered) == 1
            else {"any_of": [AnonymousSlotExpression(range=value) for value in ordered]}
        )

    groups: dict[str, str] = {}

    def apply(usage: dict[str, Any], rule: dict[str, Any], enum_name: str) -> None:
        """Add to the fields of a slot usage what the declared shapes state."""
        notes: dict[str, str] = {}
        advisory = "severity" in rule
        if rule.get("minCount", 0) >= 1:
            if advisory:
                usage["recommended"] = True
            else:
                usage["required"] = True
        if "maxCount" in rule:
            if rule["maxCount"] <= 1:
                usage["multivalued"] = False
            else:
                usage["multivalued"] = True
                usage["maximum_cardinality"] = rule["maxCount"]
        if "minCount" in rule and rule["minCount"] > 1:
            usage["minimum_cardinality"] = rule["minCount"]
        value_type: dict[str, Any] = {}
        if "class" in rule:
            value_type = {"range": class_names[rule["class"]]}
        elif rule.get("or"):
            value_type = constraints({class_names[c] for c in rule["or"]})
        elif "datatype" in rule:
            value_type = {"range": datatype_range(rule["datatype"])}
        elif rule.get("nodeKind") == SH + "IRI":
            value_type = {"range": "uriorcurie"}
        if value_type:
            usage["range"], usage["any_of"] = value_type.get("range"), value_type.get("any_of", [])
        if "pattern" in rule:
            usage["pattern"] = rule["pattern"]
        listed = rule.get("in") or (
            [rule["hasValue"]] if isinstance(rule.get("hasValue"), URIRef) else None
        )
        if listed:
            values = {
                (_curie_name(str(v), prefixes, slot=True) if isinstance(v, URIRef) else str(v)): v
                for v in listed
            }
            enums[enum_name] = EnumDefinition(
                name=enum_name,
                permissible_values={
                    text: PermissibleValue(
                        text=text, meaning=as_curie(str(v)) if isinstance(v, URIRef) else None
                    )
                    for text, v in values.items()
                },
            )
            usage["range"], usage["any_of"] = enum_name, []
        elif isinstance(rule.get("hasValue"), RdfLiteral):
            usage["equals_string"] = str(rule["hasValue"])
        if "order" in rule:
            usage["rank"] = rule["order"]
        if "name" in rule:
            usage["title"] = rule["name"]
        if "description" in rule:
            usage["description"] = rule["description"]
        if "group" in rule:
            group = rule["group"]
            if group not in groups:
                groups[group] = _curie_name(group, prefixes, slot=True) + "_group"
            usage["slot_group"] = groups[group]
        if "message" in rule:
            notes["sh_message"] = rule["message"]
        if advisory:
            notes["sh_severity"] = rule["severity"]
        for key, value in sorted(rule.get("dash", {}).items()):
            notes[f"dash_{key}"] = value
        if notes:
            usage["annotations"] = notes

    slot_examples: dict[str, list[str]] = defaultdict(list)
    for example in schema.enrichment.examples:
        slot_examples[example.property_uri].append(example.value.value)
    slots = {
        slot_names[iri]: SlotDefinition(
            name=slot_names[iri],
            slot_uri=as_curie(iri),
            title=label(iri),
            multivalued=True,
            required=False,
            **(constraints(ranges[iri]) if iri in ranges else {}),
            description=schema.enrichment.description(iri),
            examples=[Example(value=v) for v in _first(slot_examples.get(iri, []))],
        )
        for iri in property_iris
    }
    counted = counted_cardinalities(schema)
    pairs = set(class_ranges) | {(c, p) for c, paths in rules.items() for p in paths}
    for class_iri, property_iri in sorted(pairs):
        slot_name = slot_names[property_iri]
        if class_iri not in classes:
            continue
        classes[class_iri].slots.append(slot_name)
        usage: dict[str, Any] = dict(
            constraints(class_ranges[(class_iri, property_iri)])
            if (class_iri, property_iri) in class_ranges
            else {}
        )
        least, most = counted.get((class_iri, property_iri), (None, None))
        if least == 1:
            usage["required"] = True
        if most == 1:
            usage["multivalued"] = False
        rule = rules.get(class_iri, {}).get(property_iri)
        if rule:
            apply(usage, rule, _camel(f"{classes[class_iri].name} {slot_name} values"))
        if not usage.get("any_of"):
            usage.pop("any_of", None)
        classes[class_iri].slot_usage[slot_name] = SlotDefinition(name=slot_name, **usage)
    for group, group_name in sorted(groups.items()):
        title = declared.value(URIRef(group), URIRef(RDFS_LABEL))
        slots[group_name] = SlotDefinition(
            name=group_name,
            is_grouping_slot=True,
            title=str(title) if title is not None else None,
            annotations={"sh_group": group},
        )
    if untyped_ranges:
        # A mixin, not a class: it is never instantiated, so it states no rdf:type (no
        # class_uri, never rdfs:Resource); its slots are the properties of the IRI subjects
        # without a type, with their observed ranges.
        taken = {c.name for c in classes.values()} | set(slots) | builtin_types
        untyped_name = "UntypedSubject"
        while untyped_name in taken:
            untyped_name += "_"
        untyped = ClassDefinition(
            name=untyped_name,
            mixin=True,
            description=(
                "IRI subjects without a type in the data (rdfsolve patterns with "
                "subject_binding 'untyped'). Not a class of the data: it has no class_uri."
            ),
            annotations={"rdfsolve_subject_binding": "untyped"},
            slots=[],
        )
        for property_iri, values in sorted(untyped_ranges.items()):
            slot_name = slot_names[property_iri]
            untyped.slots.append(slot_name)
            untyped.slot_usage[slot_name] = SlotDefinition(name=slot_name, **constraints(values))
        classes[untyped_name] = untyped

    about = schema.about
    annotations = (
        {"source_version_iri": about.source_version_iri} if about.source_version_iri else {}
    )
    if schema.navigation is not None:
        import json

        annotations["rdfsolve_navigation"] = json.dumps(
            schema.navigation.model_dump(mode="json"), sort_keys=True
        )
    return SchemaDefinition(
        id=base,
        name=name,
        title=about.title or about.dataset_name,
        description=schema_description or about.description,
        version=about.schema_version or None,
        source=about.endpoint,
        license=about.source_license,
        created_on=about.generated_at or None,
        default_prefix=name,
        default_range="string",
        imports=["linkml:types"],
        prefixes={**curie_prefixes, name: base, "linkml": "https://w3id.org/linkml/"},
        annotations=annotations,
        types=types,
        enums=enums,
        classes={item.name: item for item in classes.values()},
        slots=slots,
    )


def to_linkml_yaml(
    schema: MinedSchema,
    *,
    schema_name: str | None = None,
    schema_description: str | None = None,
    schema_base_uri: str | None = None,
) -> str:
    """Serialize a LinkML schema as YAML: the schema itself, which the LinkML generators read."""
    result = to_linkml(
        schema,
        schema_name=schema_name,
        schema_description=schema_description,
        schema_base_uri=schema_base_uri,
    )
    from linkml_runtime.dumpers import yaml_dumper

    # The source schema as written, not a generator's derived copy (which inlines the imported
    # types and the slot usages of each class, and stamps a generation date).
    return cast(str, yaml_dumper.dumps(result))
