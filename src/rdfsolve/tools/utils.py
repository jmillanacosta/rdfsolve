import urllib.parse
import yaml
import subprocess
import requests
from rdflib import Graph, ConjunctiveGraph
from rdflib.plugins.sparql.processor import prepareQuery
import os
import pkgutil
import pandas as pd


def nice_prefixed_iri(iri, prefixes):
    for namespace, prefix in prefixes.items():
        if iri.startswith(namespace) and iri != namespace:
            return f"{prefix}:{iri[len(namespace):]}"
    return iri


def retrieve_endpoint(endpoint):
    graph_query = pkgutil.get_data(__name__, "sparql/graph.rq").decode("utf-8")
    graph_request_url = f"{endpoint}?query={urllib.parse.quote(graph_query)}&ac=1"
    headers = {"Accept": "application/sparql-results+json"}

    response = requests.get(graph_request_url, headers=headers)
    response.raise_for_status()
    results = response.json()

    if not results.get("results", {}).get("bindings"):
        return {"endpoint": {endpoint: {"graph": []}}}
    graphs = [row["graphIri"]["value"] for row in results["results"]["bindings"]]
    return {"endpoint": {endpoint: {"graph": graphs}}}


def retrieve_prefixes(endpoint):
    prefix_query = pkgutil.get_data(__name__, "sparql/prefix.rq").decode("utf-8")
    request_url = f"{endpoint}?query={urllib.parse.quote(prefix_query)}&ac=1"
    headers = {"Accept": "application/sparql-results+json"}

    response = requests.get(request_url, headers=headers)
    response.raise_for_status()
    results = response.json()

    prefixes = {
        row["namespace"]["value"]: row["prefix"]["value"]
        for row in results["results"]["bindings"]
    }
    return {"prefixes": prefixes}


def process_datatypes(results_df):
    node_datatypes_map = {}
    for _, row in results_df.iterrows():
        if row["datatypeTo"]:
            from_id = row["classFrom"]
            datatype = {
                "id": row["datatypeTo"],
                "predIri": row["propIri"],
                "count": int(row["sumTriples"]),
            }
            node_datatypes_map.setdefault(from_id, []).append(datatype)
    return node_datatypes_map


def process_rdf_types(results_df, nodes_map, edges_map, node_datatypes_map):
    ignores = {
        "http://www.w3.org/1999/02/22-rdf-syntax-ns#Statement",
        "http://www.w3.org/1999/02/22-rdf-syntax-ns#object",
        "http://www.w3.org/1999/02/22-rdf-syntax-ns#subject",
        "http://www.w3.org/1999/02/22-rdf-syntax-ns#predicate",
    }
    for _, row in results_df.iterrows():
        if row["classFrom"] in ignores or row["datatypeTo"] in row:
            continue
        from_id = row["classFrom"]
        to_id = row.get("classTo", {})
        prop_id = row["propIri"]
        count = int(row["sumTriples"])

        nodes_map.setdefault(from_id, {"count": 0})
        nodes_map[from_id]["count"] += count
        if to_id:
            nodes_map.setdefault(to_id, {"count": 0})
            nodes_map[to_id]["count"] += count

        edges_map[from_id + prop_id] = {
            "from": from_id,
            "to": prop_id,
            "count": count,
        }
        if to_id:
            edges_map[prop_id + to_id] = {
                "from": prop_id,
                "to": to_id,
                "count": count,
            }
    return nodes_map, edges_map


def make_rdf_config(graph_file):
    print("Loading graph from file:", graph_file)
    graph = load_graph(graph_file)
    print("Graph loaded successfully")

    print("Retrieving prefixes from graph")
    prefixes_data = retrieve_prefixes_from_graph(graph)
    prefixes = prefixes_data["prefixes"]
    print("Prefixes retrieved:", prefixes)

    print("Retrieving data from graph")
    results = retrieve_data_from_graph(graph)

    nodes_map = {}
    edges_map = {}
    df = pd.DataFrame(results).applymap(str)
    print(f"Data retrieved: {len(df)} rows")
    df.columns = ["classFrom", "datatypeTo", "classTo", "propIri", "sumTriples"]
    print("Processing datatypes")
    node_datatypes_map = process_datatypes(df)
    print("Datatypes processed:", node_datatypes_map)

    print("Processing RDF types")
    nodes_map, edges_map = process_rdf_types(
        df, nodes_map, edges_map, node_datatypes_map
    )
    print("RDF types processed")
    print("Nodes map:", nodes_map)
    print("Edges map:", edges_map)

    model_yaml = {"nodes": nodes_map, "edges": edges_map}
    combined_yaml = {**prefixes_data, "model": model_yaml}

    print("Generating YAML output")
    yaml_output = yaml.dump(combined_yaml, default_flow_style=False, sort_keys=False)
    print("YAML output generated")
    return yaml_output


def retrieve_prefixes_from_graph(graph):
    prefix_query = pkgutil.get_data(__name__, "sparql/prefix.rq").decode("utf-8")
    prepared_query = prepareQuery(prefix_query)
    results = graph.query(prepared_query)
    if not results:
        print("Warning: No prefixes found in the graph.")

    prefixes = {
        str(row["namespace"]): str(row["prefix"])
        for row in results
    }
    return {"prefixes": prefixes}


def retrieve_data_from_graph(graph):
    query = pkgutil.get_data(__name__, "sparql/model.rq").decode("utf-8")
    prepared_query = prepareQuery(query)
    results = graph.query(prepared_query)
    if results:
        return results
    else:
        raise ValueError("No results for data model.")


# VoID Generator stuff below


VOID_GENERATOR_URL = "https://github.com/JervenBolleman/void-generator/releases/download/v0.6/void-generator-0.6-uber.jar"


def get_void_jar(path):
    """Retrieve VoID generator"""
    url = VOID_GENERATOR_URL
    path = os.getcwd() + path + "/" + url.rsplit('/', maxsplit=1)[-1]
    try:
        response = requests.get(url, stream=True, timeout=10)
        response.raise_for_status()  # Ensure the request was successful
    except requests.exceptions.HTTPError as http_err:
        if 400 <= response.status_code < 500:
            print(f"HTTP error occurred: {http_err}")
        else:
            print(f"HTTP error occurred: {http_err}. Please try again later.")
        return None
    except Exception as err:
        print(f"An error occurred: {err}")
        return None

    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "wb") as file:
        for chunk in response.iter_content(chunk_size=8192):
            file.write(chunk)
    return path


def run_void_generator_endpoint(
    jar_path, endpoint, void_iri, void_file, graph_iris
):
    """Run VoID generator on an endpoint"""
    print("Running VoID generator")
    if "/.well-known/void#" not in void_iri:
        void_iri = void_iri + "/.well-known/void#"
    if isinstance(graph_iris, list):
        graph_iris = ",".join(graph_iris)
    if ".ttl" not in void_file:
        void_file += "_void.ttl"

    command = [
        "java", "-jar", jar_path,
        "-r", endpoint,
        "--void-file", void_file,
        "--iri-of-void", void_iri,
        "-p", "https:",
        "-g", graph_iris

    ]

    print(" ".join(command), "\n")

    # Start the process
    process = subprocess.Popen(
        command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True
    )

    try:
        # Read and print output in real-time
        for line in iter(process.stdout.readline, ""):
            if "INFO" in line:
                print(line, end="")  # Print output line by line as it appears

        process.wait()  # Wait for the process to complete
        if process.returncode != 0:
            raise subprocess.CalledProcessError(
                process.returncode, command, output="Subprocess failed"
            )
    finally:
        if process.stdout:
            process.stdout.close()

    return void_file


def load_graph(graph_file):
    """Load RDF data into a ConjunctiveGraph"""
    graph = ConjunctiveGraph()
    graph.parse(graph_file, format="turtle")
    return graph


def execute_query(graph, query_file):
    """Execute a SPARQL query on the given graph"""
    query = pkgutil.get_data(__name__, query_file).decode("utf-8")
    prepared_query = prepareQuery(query)
    results = graph.query(prepared_query)
    return [
        {str(k): str(v) for k, v in row.items()}
        for row in results
    ]


import requests


def get_graph_iris(endpoint_url):
    query = """
    SELECT DISTINCT ?graph
    WHERE {
      GRAPH ?graph {
        ?s ?p ?o.
      }
    }
    """
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Accept": "application/json",
    }
    data = {"query": query}

    response = requests.post(endpoint_url, data=data, headers=headers)

    if response.status_code != 200:
        raise Exception(f"SPARQL query failed: {response.status_code} {response.text}")

    results = response.json()
    graph_list = [result["graph"]["value"] for result in results["results"]["bindings"]]
    return graph_list


IGNORE_GRAPH_IRIS = [
    " http://www.openlinksw.com/schemas/virtrdf#",
    ]
