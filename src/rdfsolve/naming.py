"""Words of a name written in camel case, without breaking names that are mixed case on purpose.

A local name such as ``hasValue`` is two words; a name that a registry writes in mixed case
(a database or a standard, such as an identifier scheme with internal capitals) is one word,
however it is cased inside a longer name. The mixed-case names are read from Bioregistry (the
names and synonyms of its resources), so no name is listed here.
"""

from __future__ import annotations

import functools
import re

__all__ = ["label", "words"]


def _camel(text: str) -> list[str]:
    """Split text at separators and case changes (a lower letter then a capital, or the last
    capital of a run before a lower letter), in any script; digits stay with their word.
    """
    out: list[str] = []
    for part in re.findall(r"[^\W_]+", text):
        start = 0
        for i in range(1, len(part)):
            before, here, after = part[i - 1], part[i], part[i + 1 : i + 2]
            if here.isupper() and (
                before.islower() or before.isdigit() or (before.isupper() and after.islower())
            ):
                out.append(part[start:i])
                start = i
        out.append(part[start:])
    return out


@functools.cache
def _protected() -> re.Pattern[str] | None:
    """Return a pattern of the mixed-case names that Bioregistry writes (longest first)."""
    try:
        import bioregistry
    except ImportError:  # pragma: no cover
        return None
    found: set[str] = set()
    for resource in bioregistry.resources():
        for text in [resource.get_name() or "", *(resource.get_synonyms() or [])]:
            for word in re.findall(r"[A-Za-z][A-Za-z0-9]*", text):
                if re.search(r"[a-z]", word) and re.search(r"[A-Z]", word[1:]) and len(word) > 3:
                    found.add(word)
    if not found:
        return None
    alternatives = "|".join(re.escape(w) for w in sorted(found, key=len, reverse=True))
    # A protected name starts the text, or follows a lowercase letter, a digit or a separator,
    # and is not followed by a lowercase letter (so it is a whole word of the name).
    return re.compile(rf"(?:(?<=^)|(?<=[a-z0-9_\W]))({alternatives})(?![a-z])")


def words(text: str) -> list[str]:
    """Return the words of a name: camel case and separators split it, registered names do not."""
    pattern = _protected()
    out: list[str] = []
    rest = 0
    for match in pattern.finditer(text) if pattern else ():
        out += _camel(text[rest : match.start()])
        out.append(match.group(1))
        rest = match.end()
    out += _camel(text[rest:])
    return out


def label(text: str) -> str:
    """Return a readable label for a name: its words, the first capitalized, registered names kept."""
    found = words(text.replace("_", " "))
    if not found:
        return text
    first = found[0]
    keep = first != first.lower() and first != first.capitalize()
    return " ".join(
        [
            first if keep else first.capitalize(),
            *(w if w != w.lower() and w != w.capitalize() else w.lower() for w in found[1:]),
        ]
    )
