"""Versioned registry of ontology identities and retrieved ontology artifacts."""

from __future__ import annotations

from pathlib import Path

from pydantic import BaseModel, Field

from rdfsolve.evidence.ontology import OntologyArtifact


class OntologyRecord(BaseModel):
    """Stable ontology identity independent of any particular retrieved release."""

    ontology_id: str
    preferred_iri: str | None = None
    prefixes: list[str] = Field(default_factory=list)
    aliases: list[str] = Field(default_factory=list)
    artifact_ids: list[str] = Field(default_factory=list)


class OntologyRegistry(BaseModel):
    """Registry of stable ontology identities and content-addressed artifacts.

    No ``latest`` release is inferred because provider version strings are not
    necessarily sortable and retrieval order is not semantic version order.
    """

    ontologies: dict[str, OntologyRecord] = Field(default_factory=dict)
    artifacts: dict[str, OntologyArtifact] = Field(default_factory=dict)

    def register_artifact(
        self,
        ontology_id: str,
        artifact: OntologyArtifact,
        *,
        preferred_iri: str | None = None,
        prefixes: list[str] | None = None,
        aliases: list[str] | None = None,
    ) -> OntologyRecord:
        """Attach one immutable retrieved artifact to a stable ontology identity."""
        existing = self.artifacts.get(artifact.artifact_id)
        if existing is not None and existing.sha256 != artifact.sha256:
            raise ValueError(f"Artifact id collision: {artifact.artifact_id}")
        self.artifacts[artifact.artifact_id] = artifact
        record = self.ontologies.setdefault(
            ontology_id,
            OntologyRecord(ontology_id=ontology_id, preferred_iri=preferred_iri),
        )
        if preferred_iri and record.preferred_iri and record.preferred_iri != preferred_iri:
            if preferred_iri not in record.aliases:
                record.aliases.append(preferred_iri)
        elif preferred_iri and not record.preferred_iri:
            record.preferred_iri = preferred_iri
        for prefix in prefixes or []:
            if prefix not in record.prefixes:
                record.prefixes.append(prefix)
        for alias in aliases or []:
            if alias not in record.aliases and alias != record.preferred_iri:
                record.aliases.append(alias)
        if artifact.artifact_id not in record.artifact_ids:
            record.artifact_ids.append(artifact.artifact_id)
        record.prefixes.sort()
        record.aliases.sort()
        record.artifact_ids.sort()
        return record

    def save(self, path: str | Path) -> Path:
        """Write the registry as JSON and return its path."""
        target = Path(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(self.model_dump_json(indent=2), encoding="utf-8")
        return target

    @classmethod
    def load(cls, path: str | Path) -> OntologyRegistry:
        """Read a registry written by save."""
        return cls.model_validate_json(Path(path).read_text(encoding="utf-8"))

    def releases(self, ontology_id: str) -> list[OntologyArtifact]:
        """Return all archived artifacts for an ontology without choosing a latest release."""
        record = self.ontologies.get(ontology_id)
        if record is None:
            return []
        return [self.artifacts[artifact_id] for artifact_id in record.artifact_ids]


__all__ = ["OntologyRecord", "OntologyRegistry"]
