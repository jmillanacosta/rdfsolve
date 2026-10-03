"""Structured SPARQL query execution with Pydantic result models."""

from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel, Field

from rdfsolve.sparql_helper import SparqlHelper

if TYPE_CHECKING:
    import pandas as pd

# Result models


class ResultCell(BaseModel):
    """One cell in a SPARQL result row."""

    value: str
    type: str  # "uri" | "literal" | "bnode"
    lang: str | None = None
    datatype: str | None = None


class VariableProfile(BaseModel):
    """How one result column is bound across rows."""

    name: str
    bound: int = Field(description="Rows with a value.")
    distinct: int = Field(description="Distinct RDF terms.")
    kinds: dict[str, int] = Field(default_factory=dict, description="uri, literal or bnode.")
    datatypes: dict[str, int] = Field(default_factory=dict)
    languages: dict[str, int] = Field(default_factory=dict)


class Multiplicity(BaseModel):
    """Values of one record column that occur with several values of another column."""

    key: str
    other: str
    keys: int = Field(description="Distinct key values bound together with the other column.")
    multiple: int = Field(description="Key values with more than one other value.")
    max: int = Field(description="Largest number of other values for one key value.")
    note: str | None = Field(
        None, description="Set when only a minority of keys repeat: rows then overcount keys."
    )


class ResultProfile(BaseModel):
    """What the rows of a result represent, computed without further queries."""

    rows: int
    variables: list[VariableProfile]
    multiplicities: list[Multiplicity] = Field(default_factory=list)
    notes: list[str] = Field(default_factory=list)

    def variable(self, name: str) -> VariableProfile:
        """Return one column profile by name."""
        return next(v for v in self.variables if v.name == name)


class QueryResult(BaseModel):
    """Structured result from a SPARQL query execution."""

    query: str
    endpoint: str
    variables: list[str]
    rows: list[dict[str, ResultCell]]
    variable_map: dict[str, str] = Field(default_factory=dict)
    row_count: int
    duration_ms: int
    error: str | None = None

    def table(self) -> pd.DataFrame:
        """Display values; exact RDF terms remain in rows."""
        import pandas as pd

        return pd.DataFrame(
            [{name: term.value for name, term in row.items()} for row in self.rows],
            columns=self.variables,
        )

    def profile(self) -> ResultProfile:
        """Report coverage, term kinds and one-to-many joins behind these rows.

        A row is one solution, not one record: a record column (IRI or blank
        node values) that occurs with several values of another column repeats
        in several rows. Notes concern columns that nearly identify rows
        (distinct values at least half the rows): when only a minority of their
        values repeat, rows usually multiply through a shared context rather
        than the question's intended one-to-many relation.
        """
        from collections import Counter, defaultdict

        def term(cell: ResultCell) -> tuple[str, str, str | None, str | None]:
            """Identify an exact RDF term."""
            return cell.type, cell.value, cell.datatype, cell.lang

        total = len(self.rows)
        profiles, notes, found = [], [], []
        for name in self.variables:
            cells = [row[name] for row in self.rows if name in row]
            kinds = Counter(c.type for c in cells)
            profile = VariableProfile(
                name=name,
                bound=len(cells),
                distinct=len({term(c) for c in cells}),
                kinds=dict(kinds),
                datatypes=dict(Counter(c.datatype for c in cells if c.datatype)),
                languages=dict(Counter(c.lang for c in cells if c.lang)),
            )
            profiles.append(profile)
            if 0 < profile.bound < total:
                notes.append(f"{name} is unbound in {total - profile.bound} of {total} rows.")
            if len(profile.datatypes) > 1 or len(kinds) > 1:
                shown = profile.datatypes or profile.kinds
                notes.append(f"{name} has several datatypes or term kinds: {shown}.")
            if not cells or kinds.get("literal", 0) * 2 >= len(cells):
                continue
            for other in self.variables:
                if other == name:
                    continue
                values: dict[Any, set[Any]] = defaultdict(set)
                for row in self.rows:
                    if name in row and other in row:
                        values[term(row[name])].add(term(row[other]))
                sizes = [len(v) for v in values.values()]
                multiple = sum(size > 1 for size in sizes)
                if not multiple:
                    continue
                item = Multiplicity(
                    key=name, other=other, keys=len(sizes), multiple=multiple, max=max(sizes)
                )
                if multiple * 2 < len(sizes) and profile.distinct * 2 >= total:
                    item.note = (
                        f"{multiple} of {len(sizes)} {name} values have several {other} values "
                        f"(up to {item.max}); count distinct {name} values, not rows."
                    )
                    notes.append(item.note)
                found.append(item)
        return ResultProfile(rows=total, variables=profiles, multiplicities=found, notes=notes)


# Public helper


def execute_sparql(
    query: str,
    endpoint: str,
    *,
    method: str = "GET",
    timeout: int = 30,
    variable_map: dict[str, str] | None = None,
) -> QueryResult:
    """Execute a SPARQL SELECT query and return a :class:`QueryResult`.

    Parameters
    ----------
    query:
        Full SPARQL query string.
    endpoint:
        URL of the SPARQL endpoint.
    method:
        HTTP method (``"GET"`` or ``"POST"``).  If ``"GET"`` fails the
        underlying :class:`SparqlHelper` will automatically retry with
        POST.
    timeout:
        Request timeout in seconds.
    variable_map:
        Optional mapping of SPARQL ``?variable`` names to schema URIs.

    Returns
    -------
    QueryResult
        Pydantic model with ``query``, ``endpoint``, ``variables``,
        ``rows``, ``variable_map``, ``row_count``, ``duration_ms``, and
        optionally ``error``.
    """
    t0 = time.monotonic()

    try:
        helper = SparqlHelper(
            endpoint,
            use_post=(method.upper() == "POST"),
            timeout=float(timeout),
        )
        json_result = helper.select(query, purpose="user-query")
    except Exception as exc:
        return QueryResult(
            query=query,
            endpoint=endpoint,
            variables=[],
            rows=[],
            variable_map=variable_map or {},
            row_count=0,
            duration_ms=int((time.monotonic() - t0) * 1000),
            error=str(exc),
        )

    variables: list[str] = json_result.get("head", {}).get("vars", [])
    bindings: list[dict[str, Any]] = json_result.get("results", {}).get("bindings", [])

    rows: list[dict[str, ResultCell]] = []
    for binding in bindings:
        row: dict[str, ResultCell] = {}
        for var in variables:
            cell_data = binding.get(var)
            if cell_data:
                cell_type = cell_data.get("type", "literal")
                if cell_type == "uri":
                    rtype = "uri"
                elif cell_type == "bnode":
                    rtype = "bnode"
                else:
                    rtype = "literal"
                row[var] = ResultCell(
                    value=cell_data["value"],
                    type=rtype,
                    lang=cell_data.get("xml:lang"),
                    datatype=cell_data.get("datatype"),
                )
        rows.append(row)

    duration_ms = int((time.monotonic() - t0) * 1000)

    return QueryResult(
        query=query,
        endpoint=endpoint,
        variables=variables,
        rows=rows,
        variable_map=variable_map or {},
        row_count=len(rows),
        duration_ms=duration_ms,
    )
