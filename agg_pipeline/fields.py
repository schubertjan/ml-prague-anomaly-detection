"""Field extraction from event documents."""

from __future__ import annotations

UNKNOWN = "unknown"


def extract(doc: dict, field: str, default: str = UNKNOWN) -> str:
    """Extract a field from an event doc.

    Field names are flat: a dotted name such as 'process.program' looks up the
    literal key doc["process.program"] first. As a fallback it also resolves
    through nested dicts (doc["process"]["program"]).

    Missing fields or nulls resolve to `default`.
    """
    if field in doc:
        value = doc[field]
    else:
        cur = doc
        for part in field.split("."):
            if isinstance(cur, dict) and part in cur:
                cur = cur[part]
            else:
                return default
        value = cur
    if value is None:
        return default
    return str(value)
