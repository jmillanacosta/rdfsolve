#!/usr/bin/env python3
"""Extract cross-references from local QLever indices via SPARQL."""
import logging
import sys
from pathlib import Path
import pandas as pd
import curies

from rdfsolve.sparql_helper import SparqlHelper
from mappings_qlever_utils import get_qlever_workdirs, start_qlever_server, stop_qlever_server

logging.basicConfig(level=logging.INFO, format='%(message)s')
log = logging.getLogger(__name__)

OUTPUT_DIR = Path(__file__).parent.parent / "output" / "mappings"
OUTPUT_FILE = OUTPUT_DIR / "02_crossrefs.sssom.tsv"
DATA_DIR = Path(__file__).parent.parent.parent / "data"
BASE_PORT = 7000

LINKING_PREDS = [
    # SKOS mapping properties
    "http://www.w3.org/2004/02/skos/core#exactMatch",
    "http://www.w3.org/2004/02/skos/core#closeMatch",
    "http://www.w3.org/2004/02/skos/core#broadMatch",
    "http://www.w3.org/2004/02/skos/core#narrowMatch",
    "http://www.w3.org/2004/02/skos/core#relatedMatch",
    "http://www.w3.org/2004/02/skos/core#mappingRelation",

    # OWL equivalence
    "http://www.w3.org/2002/07/owl#sameAs",
    "http://www.w3.org/2002/07/owl#equivalentClass",
    "http://www.w3.org/2002/07/owl#equivalentProperty",

    # RDFS
    "http://www.w3.org/2000/01/rdf-schema#seeAlso",

    # Schema.org
    "http://schema.org/sameAs",
    "https://schema.org/sameAs",

    # Dublin Core
    "http://purl.org/dc/terms/replaces",
    "http://purl.org/dc/terms/isReplacedBy",

    # OBO ontology cross-references (very common in bio data)
    "http://www.geneontology.org/formats/oboInOwl#hasDbXref",
    "http://purl.obolibrary.org/obo/IAO_0000115",  # definition (sometimes contains xrefs)

    # Bio2RDF specific
    "http://bio2rdf.org/bio2rdf_vocabulary:xRef",
    "http://bio2rdf.org/bio2rdf_vocabulary:x-uniprot",
    "http://bio2rdf.org/bio2rdf_vocabulary:x-pubmed",
    "http://bio2rdf.org/bio2rdf_vocabulary:x-kegg",
    "http://bio2rdf.org/bio2rdf_vocabulary:x-chembl",
    "http://bio2rdf.org/bio2rdf_vocabulary:x-drugbank",

    # UniProt specific
    "http://purl.uniprot.org/core/database",

    # Identifiers.org
    "http://identifiers.org/idot/preferredPrefix",
    "http://identifiers.org/idot/alternatePrefix",

    # VOID linksets
    "http://rdfs.org/ns/void#linkPredicate",

    # Other common xref predicates
    "http://www.w3.org/2004/02/skos/core#related",
    "http://purl.org/dc/terms/relation",
]

# Load converter once at module level
try:
    CONVERTER = curies.get_bioregistry_converter()
except:
    CONVERTER = None
    log.warning("Could not load bioregistry converter")


def normalize_iri(iri):
    """Normalize IRI to CURIE if possible."""
    if not CONVERTER:
        return iri
    try:
        curie = CONVERTER.compress(iri)
        return curie if curie else iri
    except:
        return iri


def query_crossrefs(source_name, port):
    """Query cross-references WITH classes using pagination."""
    results = []
    endpoint = f"http://localhost:{port}"
    helper = SparqlHelper(endpoint, timeout=60.0)
    CHUNK_SIZE = 50000

    for i, pred in enumerate(LINKING_PREDS, 1):
        try:
            pred_short = pred.split('#')[-1].split('/')[-1]
            sys.stderr.write(f"  [{i}/{len(LINKING_PREDS)}] {pred_short}...\n")
            sys.stderr.flush()

            offset = 0
            total = 0
            while True:
                query = f"""
                SELECT ?s ?s_class ?o ?o_class WHERE {{
                    ?s <{pred}> ?o .
                    FILTER(isIRI(?o))
                    OPTIONAL {{ ?s a ?s_class . FILTER(isIRI(?s_class)) }}
                    OPTIONAL {{ ?o a ?o_class . FILTER(isIRI(?o_class)) }}
                }} LIMIT {CHUNK_SIZE} OFFSET {offset}
                """

                res = helper.select(query)
                bindings = res.get('results', {}).get('bindings', [])

                if not bindings:
                    break

                for binding in bindings:
                    subj = binding['s']['value']
                    obj = binding['o']['value']
                    subj_class = binding.get('s_class', {}).get('value')
                    obj_class = binding.get('o_class', {}).get('value')

                    results.append({
                        'subject_id': normalize_iri(subj),
                        'subject_class': normalize_iri(subj_class) if subj_class else None,
                        'object_id': normalize_iri(obj),
                        'object_class': normalize_iri(obj_class) if obj_class else None,
                        'predicate_id': pred,
                        'source': source_name
                    })

                total += len(bindings)
                offset += CHUNK_SIZE

                if len(bindings) < CHUNK_SIZE:
                    break

            if total > 0:
                sys.stderr.write(f"    -> {total} results\n")
                sys.stderr.flush()

        except Exception as e:
            sys.stderr.write(f"    -> Error: {e}\n")
            sys.stderr.flush()
            continue

    helper.close()
    return results


def process_source(source_name, port):
    """Start QLever server, query, then stop."""
    workdir = DATA_DIR / "qlever_workdirs" / source_name

    sys.stderr.write(f"Starting {source_name} on port {port}...\n")
    sys.stderr.flush()
    pid = start_qlever_server(workdir, source_name, port, DATA_DIR)

    if not pid:
        sys.stderr.write(f"  {source_name}: failed to start server\n")
        sys.stderr.flush()
        return []

    try:
        results = query_crossrefs(source_name, port)
        sys.stderr.write(f"  {source_name}: {len(results)} cross-refs\n")
        sys.stderr.flush()
        return results
    finally:
        sys.stderr.write(f"  Stopping server (PID {pid})...\n")
        sys.stderr.flush()
        stop_qlever_server(pid)
        sys.stderr.write(f"  Server stopped\n")
        sys.stderr.flush()


def main():
    if OUTPUT_FILE.exists():
        log.info(f"Output exists: {OUTPUT_FILE}")
        return

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    sources = get_qlever_workdirs(DATA_DIR)
    sys.stderr.write(f"Processing {len(sources)} sources with local QLever indices\n")
    sys.stderr.flush()

    all_results = []
    port = BASE_PORT

    # Process sequentially to avoid port conflicts and resource limits
    for idx, source_name in enumerate(sources, 1):
        sys.stderr.write(f"\n[{idx}/{len(sources)}] {source_name}\n")
        sys.stderr.flush()
        results = process_source(source_name, port)
        all_results.extend(results)
        port += 1

    if not all_results:
        log.warning("No cross-references found")
        OUTPUT_FILE.write_text("# No cross-references\n")
        return

    # Convert to DataFrame
    df = pd.DataFrame(all_results)

    # Map predicates to SSSOM standard
    pred_map = {
        "http://www.w3.org/2004/02/skos/core#exactMatch": "skos:exactMatch",
        "http://www.w3.org/2004/02/skos/core#closeMatch": "skos:closeMatch",
        "http://www.w3.org/2004/02/skos/core#broadMatch": "skos:broadMatch",
        "http://www.w3.org/2004/02/skos/core#narrowMatch": "skos:narrowMatch",
        "http://www.w3.org/2004/02/skos/core#relatedMatch": "skos:relatedMatch",
        "http://www.w3.org/2004/02/skos/core#mappingRelation": "skos:mappingRelation",
        "http://www.w3.org/2002/07/owl#sameAs": "owl:sameAs",
        "http://www.w3.org/2002/07/owl#equivalentClass": "owl:equivalentClass",
        "http://www.w3.org/2002/07/owl#equivalentProperty": "owl:equivalentProperty",
        "http://www.w3.org/2000/01/rdf-schema#seeAlso": "rdfs:seeAlso",
        "http://schema.org/sameAs": "schema:sameAs",
        "https://schema.org/sameAs": "schema:sameAs",
        "http://purl.org/dc/terms/replaces": "dct:replaces",
        "http://purl.org/dc/terms/isReplacedBy": "dct:isReplacedBy",
        "http://www.geneontology.org/formats/oboInOwl#hasDbXref": "oboInOwl:hasDbXref",
        "http://purl.obolibrary.org/obo/IAO_0000115": "IAO:0000115",
        "http://bio2rdf.org/bio2rdf_vocabulary:xRef": "bio2rdf:xRef",
        "http://bio2rdf.org/bio2rdf_vocabulary:x-uniprot": "bio2rdf:x-uniprot",
        "http://bio2rdf.org/bio2rdf_vocabulary:x-pubmed": "bio2rdf:x-pubmed",
        "http://bio2rdf.org/bio2rdf_vocabulary:x-kegg": "bio2rdf:x-kegg",
        "http://bio2rdf.org/bio2rdf_vocabulary:x-chembl": "bio2rdf:x-chembl",
        "http://bio2rdf.org/bio2rdf_vocabulary:x-drugbank": "bio2rdf:x-drugbank",
        "http://purl.uniprot.org/core/database": "uniprot:database",
        "http://identifiers.org/idot/preferredPrefix": "idot:preferredPrefix",
        "http://identifiers.org/idot/alternatePrefix": "idot:alternatePrefix",
        "http://rdfs.org/ns/void#linkPredicate": "void:linkPredicate",
        "http://www.w3.org/2004/02/skos/core#related": "skos:related",
        "http://purl.org/dc/terms/relation": "dct:relation",
    }
    df['predicate_id'] = df['predicate_id'].map(pred_map).fillna(df['predicate_id'])

    # Deduplicate on entity pairs (keep class info)
    df = df.drop_duplicates()

    sys.stderr.write(f"\nTotal cross-references with classes: {len(df)}\n")
    sys.stderr.flush()

    df.to_csv(OUTPUT_FILE, sep='\t', index=False)
    sys.stderr.write(f"Wrote: {OUTPUT_FILE}\n")
    sys.stderr.flush()


if __name__ == "__main__":
    main()
