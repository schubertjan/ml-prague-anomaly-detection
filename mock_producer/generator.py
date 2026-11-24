"""Dummy document generator driven by a YAML field spec."""

from __future__ import annotations

import random
import re
import string
from datetime import datetime, timezone

_INT_TMPL = re.compile(r"\{int:(\d+)-(\d+)\}")


def _one(spec: dict) -> object:
    t = spec.get("type", "static")
    if t == "now":
        return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
    if t == "choice":
        return random.choice(spec["values"])
    if t == "int":
        return random.randint(spec.get("min", 0), spec.get("max", 100))
    if t == "static":
        return spec["value"]
    if t == "template":
        s = spec["pattern"]
        s = _INT_TMPL.sub(lambda m: str(random.randint(int(m[1]), int(m[2]))), s)
        return s
    raise ValueError(f"unknown generator type: {t!r}")


def generate(spec: dict | None) -> dict:
    """Build a random document from a flat field spec (field name -> generator).

    Field names are used verbatim as output keys, so a dotted name such as
    'process.program' yields the flat key 'process.program', not a nested
    object.
    """
    if spec is None:
        spec = {}
    if not isinstance(spec, dict) or "type" in spec:
        raise ValueError("field spec must be a mapping of field name -> generator")

    out: dict = {}
    for key, sub in spec.items():
        if isinstance(sub, dict):
            if "type" not in sub:
                raise ValueError(
                    f"nested object for field {key!r} is not supported; "
                    f"use a dotted field name (e.g. 'process.program')"
                )
            out[key] = _one(sub)
        else:
            out[key] = sub  # plain scalar -> static
    return out


def random_string(n: int = 12) -> str:
    return "".join(random.choices(string.ascii_lowercase, k=n))
