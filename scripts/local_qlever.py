"""Run a small QLever fixture for a notebook experiment."""

from contextlib import contextmanager
from pathlib import Path
import socket
import subprocess

from rdfsolve.qlever.lifecycle import start_server, stop_server


@contextmanager
def local_qlever(turtle, workdir, image):
    workdir = Path(workdir).resolve()
    workdir.mkdir(parents=True, exist_ok=True)
    image = Path(image).resolve()
    data = workdir / "data.ttl"
    data.write_bytes(Path(turtle).read_bytes())
    settings = workdir / "settings.json"
    settings.write_text('{"ascii-prefixes-only": false}')
    (workdir / "Qleverfile").write_text("[data]\nNAME = sample\n[server]\nMEMORY_FOR_QUERIES = 2G\nTIMEOUT = 30s\n")
    with (workdir / "index.log").open("w") as log:
        subprocess.run(["singularity", "exec", "--bind", f"{workdir}:{workdir}", str(image),
                        "qlever-index", "-i", "sample", "-s", str(settings), "-F", "ttl",
                        "-f", str(data), "-p", "false", "-b", "32MB", "-m", "1GB"],
                       cwd=workdir, check=True, stdout=log, stderr=subprocess.STDOUT)
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    server = start_server(image, workdir, "sample", port, startup_timeout=60)
    try:
        yield f"http://127.0.0.1:{port}"
    finally:
        stop_server(server)


def path_pairs(graph, route):
    """Evaluate a retained forward route by RDF traversal, independently of SPARQL."""
    from rdflib import RDF, XSD, URIRef, Literal, BNode

    pairs = {(s, s) for s in graph.subjects(RDF.type, URIRef(route.steps[0].subject_class))}
    for step in route.steps:
        found = set()
        for start, current in pairs:
            for target in graph.objects(current, URIRef(step.property_uri)):
                if step.object_class == "Literal":
                    valid = isinstance(target, Literal) and (not step.datatype or str(target.datatype or (RDF.langString if target.language else XSD.string)) == step.datatype)
                elif step.object_class in {"Resource", "BlankNode"}:
                    valid = isinstance(target, URIRef if step.object_class == "Resource" else BNode)
                else:
                    valid = (target, RDF.type, URIRef(step.object_class)) in graph
                if valid:
                    found.add((start, target))
        pairs = found
    return pairs


def term_key(term):
    """Compare RDF 1.1 terms across a Turtle fixture and SPARQL JSON."""
    from rdflib import URIRef, Literal, XSD

    if isinstance(term, URIRef):
        return ("uri", str(term))
    if isinstance(term, Literal):
        return ("literal", str(term), str(term.datatype or (RDF_LANG if term.language else XSD.string)), term.language)
    raise ValueError("Blank-node labels cannot be compared across source responses")


RDF_LANG = "http://www.w3.org/1999/02/22-rdf-syntax-ns#langString"
