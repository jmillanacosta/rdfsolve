"""The recipe of a local index, written into the run beside the outputs of its source.

The work folder of a source (data/qlever_workdirs/<source>) holds how its index was built: the
Qleverfile, the index command and its feed, the settings of qlever-index, the census of literal
datatypes, the download record and the input lines that were repaired. A release that holds
only the outputs of mining cannot say how to build the index again; write_index_recipe copies
these files into a folder of the run (<source>_local_index_recipe), made portable:

- every absolute path under the work folder is written relative to it; a path under the data
  directory is written ${RDFSOLVE_DATA_DIR}/..., and one under the home folder ${HOME}/...;
- the Qleverfile names the QLever image that built the index (its SHA-256 and QLever build,
  from the engine record of the run) instead of the moving tag docker.io/adfreiburg/qlever:latest;
- the values of the job that served the index (PORT, ACCESS_TOKEN, MEMORY_FOR_QUERIES) are
  left out, with a comment;
- the export manifest of a source made from an endpoint export (export_inputs.json names it),
  and its notes, are copied unchanged: their SHA-256 is the one that the export pin records.

The RDF files, the index, the server logs and the scan store are never copied. recipe.json
lists the files of the recipe, how the image was pinned and the steps that rebuild the index.
"""

from __future__ import annotations

import json
import re
import shutil
import subprocess
from pathlib import Path
from typing import Any

from rdfsolve.qlever.datatypes import CENSUS_FILE
from rdfsolve.qlever.downloads import RECORD
from rdfsolve.qlever.inputs import FEED, FEED_PIPES
from rdfsolve.qlever.repair import REPAIRS_FILE

RECIPE_DIR = "{name}{suffix}_index_recipe"
RECIPE_FILE = "recipe.json"
EXPORT_PINS = "export_inputs.json"
EXPORT_MANIFEST = "export-manifest.json"
EXPORT_NOTES = "export-manifest-notes.json"
DATA_DIR_VARIABLE = "RDFSOLVE_DATA_DIR"
# The values of the job that served the index: not part of how it is built.
JOB_VALUES = ("PORT", "ACCESS_TOKEN", "MEMORY_FOR_QUERIES")
_LATEST = "docker.io/adfreiburg/qlever:latest"
# How to build the index again from the recipe, written into recipe.json.
REBUILD_STEPS = (
    (
        "Copy this folder; in it, run the GET_DATA_CMD of the Qleverfile with bash, with "
        "RDFSOLVE_DATA_DIR and RDFSOLVE_PYTHON set (it downloads the URLs of downloads.json "
        "and converts RDF/XML and HDT to N-Triples)."
    ),
    (
        "Convert TriG inputs to N-Quads as rdfsolve does (rdfsolve.qlever.inputs."
        "convert_trig): GET_DATA_CMD does not."
    ),
    "Check the files against their SHA-256 in <source>_local_inputs.json.",
    (
        "When input-repairs.json is here, write each listed line of its file as its "
        "'written' form (rdfsolve.qlever.repair.repair_inputs does it)."
    ),
    (
        'Run index-command.sh in the pinned image: singularity exec --bind "$PWD" '
        "<image> bash index-command.sh."
    ),
)
# An absolute path, or the path of a file:// URL; not the path part of an http(s) URL.
_PATH = re.compile(r"(?:(?<=file://)|(?<![\w:/.~$}!]))/[^\s'\"<>;|&(){}\[\],]+")


def _relocate(text: str, workdir: Path, data_dir: Path | None) -> tuple[str, list[str]]:
    """Write the absolute paths of *text* relative to the work folder or as variables.

    Return the text and the absolute paths left as they are (under none of these folders).
    """
    places = [(workdir.resolve(), "")]
    if data_dir is not None:
        places.append((data_dir.resolve(), f"${{{DATA_DIR_VARIABLE}}}"))
    places.append((Path.home().resolve(), "${HOME}"))
    left: list[str] = []

    def replace(match: re.Match[str]) -> str:
        """Return the path of *match* relative to the first folder that holds it."""
        path = match.group(0)
        resolved = Path(path).resolve()
        for folder, variable in places:
            if resolved == folder or folder in resolved.parents:
                relative = resolved.relative_to(folder).as_posix()
                if not variable:
                    return relative
                return variable if relative == "." else f"{variable}/{relative}"
        if path != "/" and not path.startswith("/dev/"):
            left.append(path)
        return path

    return _PATH.sub(replace, text), left


def _image_comment(engine: dict[str, Any]) -> list[str]:
    """Say which image built the index, and how the recipe pins it."""
    lines = [
        "# The image is pinned by rdfsolve from the engine record of the run",
        (
            "# (<source>_local_engine.json): the index was built by QLever build "
            f"{engine.get('index_build') or 'unknown'}"
        ),
    ]
    if engine.get("image_revision"):
        lines.append(f"# (revision {engine['image_revision']}, from the labels of the image)")
    lines.append(
        f"# with the image {engine.get('image_name') or 'unknown'}, SHA-256 "
        f"{engine.get('image_sha256') or 'unknown'}."
    )
    if engine.get("image_from"):
        lines.append(
            f"# That image was built from {engine['image_from']}"
            + (f" (created {engine['image_created']})" if engine.get("image_created") else "")
            + ";"
        )
        lines.append("# that tag moves with each QLever build, so use an image of the build above:")
        lines.append("# an image of another build may not read the index.")
    return lines


def portable_qleverfile(
    text: str, workdir: Path, engine: dict[str, Any], *, data_dir: Path | None = None
) -> tuple[str, list[str]]:
    """Return the Qleverfile *text* made portable, and the absolute paths left in it.

    The paths are relocated (_relocate), the image is pinned to the engine record, and the
    server values of the job (JOB_VALUES) are left out.
    """
    text, left = _relocate(text, workdir, data_dir)
    out: list[str] = []
    section = ""
    for line in text.splitlines():
        stripped = line.strip()
        if re.fullmatch(r"#\s+cd\s+'?\.'?", stripped):
            out.append("#  cd <the folder of this Qleverfile>")
            continue
        header = re.fullmatch(r"\[(\w+)\]", stripped)
        if header:
            section = header.group(1)
            out.append(line)
            if section == "data":
                out += [
                    "# GET_DATA_CMD downloads the URLs of downloads.json into this folder; the files",
                    "# that were indexed, and their SHA-256, are pinned in <source>_local_inputs.json",
                    "# beside this folder. Check them before building the index.",
                ]
            if section == "server":
                out += [
                    "# PORT, ACCESS_TOKEN and MEMORY_FOR_QUERIES were values of the job that served",
                    "# the index; they are left out. Set a free PORT, a token of your own and the",
                    "# query memory of your machine to serve it.",
                ]
            continue
        key = re.match(r"([A-Z_]+)\s*=", stripped)
        if section == "server" and key and key.group(1) in JOB_VALUES:
            continue
        if section == "runtime" and key and key.group(1) == "IMAGE":
            out += _image_comment(engine)
            out.append(f"IMAGE  = {engine.get('image_name') or _LATEST}")
            if engine.get("image_sha256"):
                out.append(f"IMAGE_SHA256 = {engine['image_sha256']}")
            if engine.get("index_build"):
                out.append(f"INDEX_BUILD = {engine['index_build']}")
            continue
        out.append(line)
    return "\n".join(out) + "\n", left


def portable_script(
    text: str, workdir: Path, *, data_dir: Path | None = None
) -> tuple[str, list[str]]:
    """Return an index script made portable: it runs in its own folder, wherever that is."""
    text, left = _relocate(text, workdir, data_dir)
    text = re.sub(r"^cd '?\.'?$", 'cd "$(dirname "${BASH_SOURCE[0]}")"', text, flags=re.MULTILINE)
    return text, left


def image_labels(image: Path) -> dict[str, str]:
    """Return the labels of a Singularity image (singularity inspect), or none without it."""
    try:
        result = subprocess.run(
            ["singularity", "inspect", str(image)],
            capture_output=True,
            text=True,
            timeout=120,
            check=True,
        )
    except (OSError, subprocess.SubprocessError):
        return {}
    labels: dict[str, str] = {}
    for line in result.stdout.splitlines():
        key, sep, value = line.partition(": ")
        if sep:
            labels[key.strip()] = value.strip()
    return labels


def engine_details(engine: dict[str, Any], labels: dict[str, str]) -> dict[str, Any]:
    """Return the engine record of a run with the image's file name and origin, for the recipe.

    The image path of the record is a path of the machine that ran it; its file name is kept.
    """
    details = {
        "image_name": Path(str(engine["image"])).name if engine.get("image") else None,
        "image_sha256": engine.get("image_sha256"),
        "index_build": engine.get("index_build"),
        "image_from": labels.get("org.label-schema.usage.singularity.deffile.from"),
        "image_revision": labels.get("org.opencontainers.image.revision"),
        "image_created": labels.get("org.opencontainers.image.created"),
    }
    return {key: value for key, value in details.items() if value}


def _export_manifest(workdir: Path) -> Path | None:
    """Return the manifest of the endpoint export that a work folder was made from, if any."""
    pins = workdir / EXPORT_PINS
    if not pins.is_file():
        return None
    manifest = (
        json.loads(pins.read_text(encoding="utf-8")).get("endpoint_export", {}).get("manifest")
    )
    return Path(manifest) if manifest else None


def write_index_recipe(
    workdir: Path,
    target: Path,
    *,
    name: str,
    engine: dict[str, Any],
    data_dir: Path | None = None,
) -> dict[str, Any] | None:
    """Write the recipe of the index in *workdir* into the folder *target*; return recipe.json.

    *engine* is the engine record of the run (engine_details). Nothing is written for a work
    folder without a Qleverfile. A folder written before is replaced, so that no file of an
    older recipe stays.
    """
    qleverfile = workdir / "Qleverfile"
    if not qleverfile.is_file():
        return None
    if target.exists():
        shutil.rmtree(target)
    target.mkdir(parents=True)
    files: dict[str, dict[str, Any]] = {}
    left: dict[str, list[str]] = {}

    def keep(file: str, how: str, origin: str, paths: list[str] | None = None) -> None:
        """Record a file of the recipe, and the absolute paths left in it."""
        files[file] = {"from": origin, "written": how}
        if paths:
            left[file] = sorted(set(paths))

    text, paths = portable_qleverfile(
        qleverfile.read_text(encoding="utf-8"), workdir, engine, data_dir=data_dir
    )
    (target / "Qleverfile").write_text(text, encoding="utf-8")
    keep("Qleverfile", "portable", "Qleverfile", paths)
    for script in ("index-command.sh", FEED, FEED_PIPES):
        if (workdir / script).is_file():
            text, paths = portable_script(
                (workdir / script).read_text(encoding="utf-8"), workdir, data_dir=data_dir
            )
            (target / script).write_text(text, encoding="utf-8")
            keep(script, "portable", script, paths)
    for record in (RECORD, EXPORT_PINS):
        # The URLs of GET_DATA_CMD, and the export files they are with their SHA-256; a
        # file:// URL of an export and the path of its copy are relocated.
        if (workdir / record).is_file():
            text, paths = _relocate(
                (workdir / record).read_text(encoding="utf-8"), workdir, data_dir
            )
            (target / record).write_text(text, encoding="utf-8")
            keep(record, "paths relocated", record, paths)
    for file in (f"{name}.settings.json", CENSUS_FILE, REPAIRS_FILE):
        if (workdir / file).is_file():
            shutil.copyfile(workdir / file, target / file)
            keep(file, "unchanged", file)
    manifest = _export_manifest(workdir)
    if manifest is not None:
        if not manifest.is_file():
            raise FileNotFoundError(f"The export manifest of {name} is not there: {manifest}")
        shutil.copyfile(manifest, target / EXPORT_MANIFEST)
        keep(EXPORT_MANIFEST, "unchanged", _relocate(str(manifest), workdir, data_dir)[0])
        notes = manifest.with_name("manifest-notes.json")
        if notes.is_file():
            shutil.copyfile(notes, target / EXPORT_NOTES)
            keep(EXPORT_NOTES, "unchanged", _relocate(str(notes), workdir, data_dir)[0])
    home = str(Path.home())
    for file in files:
        content = (target / file).read_text(encoding="utf-8", errors="replace")
        if home in content or str(Path.home().resolve()) in content:
            left.setdefault(file, []).append("(a path under the home folder)")
    recipe: dict[str, Any] = {
        "format": "rdfsolve-index-recipe/1",
        "source": name,
        "engine": engine,
        "image_pinned_by": "the SHA-256 of the image file and the QLever build of the index, "
        "from the engine record of the run (not a registry tag)",
        "left_out": {
            "server_values": list(JOB_VALUES),
            "files": "the RDF inputs, the index, the server logs and the scan store",
        },
        "variables": {
            DATA_DIR_VARIABLE: "the data directory of rdfsolve (the folder that holds "
            "qlever_workdirs and exports)",
            "HOME": "the home folder",
            "RDFSOLVE_PYTHON": "a Python with rdfsolve installed, which converts RDF/XML in "
            "GET_DATA_CMD (python -m rdfsolve.qlever.rdfxml)",
        },
        "files": files,
        "absolute_paths_left": left,
        "rebuild": list(REBUILD_STEPS),
    }
    (target / RECIPE_FILE).write_text(json.dumps(recipe, indent=1), encoding="utf-8")
    return recipe
