"""Each index is served by the QLever build that made it: the index records its build, and the
image catalogue of the data directory names the image of each build."""

import json

import pytest

from rdfsolve.qlever.lifecycle import image_for_index, index_build


def workdir(tmp_path, build):
    path = tmp_path / "workdirs" / "src"
    path.mkdir(parents=True)
    (path / "src.meta-data.json").write_text(json.dumps({"git-hash": build}))
    return path


def test_the_image_of_the_index_build_is_chosen(tmp_path):
    (tmp_path / "qlever.sif").write_text("old")
    (tmp_path / "fixed.sif").write_text("new")
    index = workdir(tmp_path, "388f365")
    assert index_build(index, "src") == "388f365"
    assert image_for_index(tmp_path, index, "src") == tmp_path / "qlever.sif", "No catalogue"
    (tmp_path / "qlever_images.yaml").write_text(
        "- image: qlever.sif\n  git_hash: 9ec88a0\n- image: fixed.sif\n  git_hash: 388f365\n"
    )
    assert image_for_index(tmp_path, index, "src") == tmp_path / "fixed.sif"
    old = workdir(tmp_path / "other", "9ec88a")
    assert image_for_index(tmp_path, old, "src") == tmp_path / "qlever.sif", "Short hashes match"
    unknown = workdir(tmp_path / "third", "abc1234")
    with pytest.raises(ValueError, match="abc1234"):
        image_for_index(tmp_path, unknown, "src")


def test_an_index_not_yet_built_gets_the_default_image(tmp_path):
    (tmp_path / "qlever_images.yaml").write_text("- image: fixed.sif\n  git_hash: 388f365\n")
    new = tmp_path / "workdirs" / "src"
    new.mkdir(parents=True)
    assert image_for_index(tmp_path, new, "src") == tmp_path / "qlever.sif"
