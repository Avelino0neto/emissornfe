from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from sqlalchemy.orm import Session

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import db
import graph_mail


def _read_streamlit_secret(path: list[str]) -> str | None:
    try:
        import streamlit as st

        value = st.secrets
        for key in path:
            value = value[key]
        return str(value) if value else None
    except Exception:
        return None


def get_database_url() -> str | None:
    return (
        _read_streamlit_secret(["connections", "neon", "url"])
        or os.getenv("DATABASE_URL")
    )


def get_encryption_key() -> str | None:
    return (
        _read_streamlit_secret(["graph", "cache_encryption_key"])
        or os.getenv("GRAPH_CACHE_ENCRYPTION_KEY")
    )


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Importa token_cache.bin do MSAL para o banco, criptografado."
    )
    parser.add_argument(
        "--cache-file",
        default="token_cache.bin",
        help="Caminho do token_cache.bin local.",
    )
    args = parser.parse_args()

    cache_path = Path(args.cache_file)
    if not cache_path.exists():
        print(f"Arquivo nao encontrado: {cache_path}")
        return 1

    database_url = get_database_url()
    if not database_url:
        print("DATABASE_URL nao encontrado em st.secrets ou variavel de ambiente.")
        return 1

    encryption_key = get_encryption_key()
    if not encryption_key:
        print("graph.cache_encryption_key nao encontrado em st.secrets ou GRAPH_CACHE_ENCRYPTION_KEY.")
        return 1

    serialized_cache = cache_path.read_text(encoding="utf-8")
    engine = db.make_engine(database_url)
    db.init_db(engine)
    with Session(engine) as session:
        with session.begin():
            graph_mail.save_serialized_token_cache(session, serialized_cache, encryption_key)

    print("Cache Microsoft Graph importado para o banco com sucesso.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
