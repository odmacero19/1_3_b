"""Loads the SQLDelight schema (.sq / .sqm files) into an in-memory SQLite database for trigger tests."""
import re
import sqlite3
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2] / "main" / "sqldelight" / "tachiyomi"
TABLE_FILES = [
    "categories", "mangas", "chapters", "mangas_categories", "history",
    "excluded_scanlators", "manga_sync", "sources", "extension_store", "sync_delta_queue",
]
LABEL = re.compile(r"^\w+:\s*$", re.M)


def _strip(sql: str) -> str:
    sql = re.sub(r"^import .*$", "", sql, flags=re.M)
    sql = re.sub(r"\bAS (?:List<String>|Boolean|UpdateStrategy|JsonObject|Date)", "", sql)
    return sql


def schema_sql(name: str) -> str:
    s = _strip((ROOT / "data" / f"{name}.sq").read_text())
    m = LABEL.search(s)
    return s[: m.start()] if m else s


def queries(name: str) -> dict[str, str]:
    s = _strip((ROOT / "data" / f"{name}.sq").read_text())
    out: dict[str, str] = {}
    matches = list(LABEL.finditer(s))
    for i, m in enumerate(matches):
        end = matches[i + 1].start() if i + 1 < len(matches) else len(s)
        out[m.group(0).strip().rstrip(":")] = s[m.end():end].strip().rstrip(";")
    return out


def fresh_db() -> sqlite3.Connection:
    c = sqlite3.connect(":memory:", isolation_level=None)
    c.execute("PRAGMA foreign_keys = ON")
    for f in TABLE_FILES:
        c.executescript(schema_sql(f))
    return c


def migration_sql(n: int) -> str:
    return (ROOT / "migrations" / f"{n}.sqm").read_text()
