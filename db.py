# db.py
from __future__ import annotations
import re, unicodedata
from typing import Iterable, Optional, Tuple, List

from sqlalchemy import (
    create_engine, text, String, Boolean, Integer, Text,
    UniqueConstraint, Index, ForeignKey, Numeric
)
from sqlalchemy.orm import (
    DeclarativeBase, Mapped, mapped_column, relationship, Session
)

# -------- Base ORM --------
class Base(DeclarativeBase):
    pass

# -------- Modelos --------
class Product(Base):
    __tablename__ = "products"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    code: Mapped[str] = mapped_column(String(64), unique=True, nullable=False)
    name: Mapped[str] = mapped_column(Text, nullable=False)
    name_norm: Mapped[str] = mapped_column(Text, nullable=False)
    ncm: Mapped[Optional[str]] = mapped_column(String(16))
    unit: Mapped[Optional[str]] = mapped_column(String(16))
    cst_icms: Mapped[Optional[str]] = mapped_column(String(16))
    active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)

    aliases: Mapped[List["ProductAlias"]] = relationship(
        back_populates="product", cascade="all, delete-orphan"
    )

    __table_args__ = (
        Index("ix_products_name_norm", "name_norm"),
    )

class ProductAlias(Base):
    __tablename__ = "product_aliases"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    product_id: Mapped[int] = mapped_column(ForeignKey("products.id", ondelete="CASCADE"), nullable=False)
    store_id: Mapped[str] = mapped_column(String(64), nullable=False)
    alias: Mapped[str] = mapped_column(Text, nullable=False)
    alias_norm: Mapped[str] = mapped_column(Text, nullable=False)

    product: Mapped[Product] = relationship(back_populates="aliases")

    __table_args__ = (
        UniqueConstraint("store_id", "alias_norm", name="uq_alias_per_store"),
        Index("ix_alias_norm", "alias_norm"),
    )

class ProductInbox(Base):
    __tablename__ = "product_inbox"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    store_id: Mapped[str] = mapped_column(String(64), nullable=False)
    raw_name: Mapped[str] = mapped_column(Text, nullable=False)
    raw_code: Mapped[Optional[str]] = mapped_column(String(64))
    raw_ncm: Mapped[Optional[str]] = mapped_column(String(16))
    raw_unit: Mapped[Optional[str]] = mapped_column(String(16))
    reason: Mapped[Optional[str]] = mapped_column(String(32))
    suggested_product_id: Mapped[Optional[int]] = mapped_column(ForeignKey("products.id"))
    score: Mapped[Optional[float]] = mapped_column(Numeric(5, 2))

    __table_args__ = (
        Index("ix_inbox_store", "store_id"),
    )

class Client(Base):
    __tablename__ = "clients"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    documento: Mapped[str] = mapped_column(String(32), nullable=False, unique=True)
    nome: Mapped[str] = mapped_column(Text, nullable=False)
    nome_fantasia: Mapped[Optional[str]] = mapped_column(Text)
    logradouro: Mapped[Optional[str]] = mapped_column(Text)
    numero: Mapped[Optional[str]] = mapped_column(String(32))
    bairro: Mapped[Optional[str]] = mapped_column(Text)
    inscricao_estadual: Mapped[Optional[str]] = mapped_column(String(32))
    cidade: Mapped[Optional[str]] = mapped_column(Text)
    uf: Mapped[Optional[str]] = mapped_column(String(8))
    cep: Mapped[Optional[str]] = mapped_column(String(16))
    endereco_complemento: Mapped[Optional[str]] = mapped_column(Text)
    endereco_pais: Mapped[Optional[str]] = mapped_column(Text)
    ibge_id: Mapped[Optional[str]] = mapped_column(String(16))
    telefone: Mapped[Optional[str]] = mapped_column(String(32))
    email: Mapped[Optional[str]] = mapped_column(String(128))

class NfeXml(Base):
    __tablename__ = "nfe_xmls"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    client_id: Mapped[int] = mapped_column(ForeignKey("clients.id"), nullable=False, index=True)
    numero: Mapped[str] = mapped_column(String(64), nullable=False)
    valor_total: Mapped[Optional[float]] = mapped_column(Numeric(14, 2))
    emitida_em: Mapped[Optional[str]] = mapped_column(String(32))
    xml_text: Mapped[str] = mapped_column(Text, nullable=False)
    hash: Mapped[str] = mapped_column(String(64), nullable=False, unique=True)

    client: Mapped[Client] = relationship("Client")

# -------- Engine / init --------
def make_engine(database_url: str):
    # psycopg2-binary aceita ?sslmode=require; deixe como veio do Neon
    return create_engine(database_url, pool_pre_ping=True, future=True)

def init_db(engine) -> None:
    """
    Cria tabelas e, se possível, habilita extensões úteis.
    """
    with engine.begin() as conn:
        # tenta habilitar pg_trgm (ok no Neon). Se não der, segue sem.
        try:
            conn.execute(text("CREATE EXTENSION IF NOT EXISTS pg_trgm"))
        except Exception:
            pass
        Base.metadata.create_all(conn)

# -------- Normalização de nome --------
ABBREV = {
    r"\bPCT\b": "PACOTE",
    r"\bPCTE\b": "PACOTE",
    r"\bPCT\.?\b": "PACOTE",
    r"\bPTA\b": "PAULISTA",
    r"\bEMB\.?\b": "EMBALADA",
}

def normalize_name(name: str) -> str:
    s = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode("ascii")
    s = s.upper()
    for pat, repl in ABBREV.items():
        s = re.sub(pat, repl, s)
    s = re.sub(r"[^A-Z0-9\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

# -------- Operações principais --------
def upsert_product_by_code(
    s: Session,
    *,
    code: str,
    name: str,
    ncm: str | None = None,
    unit: str | None = None,
    cst_icms: str | None = None,
) -> Product:
    """
    Cria/atualiza produto pela chave de negócio `code`.
    """
    name_norm = normalize_name(name)
    p = s.execute(
        text("SELECT * FROM products WHERE code=:code FOR UPDATE"),
        {"code": code},
    ).mappings().first()

    if p:
        s.execute(text("""
            UPDATE products
               SET name=:name, name_norm=:name_norm, ncm=:ncm, unit=:unit, cst_icms=:cst
             WHERE code=:code
        """), {"name": name, "name_norm": name_norm, "ncm": ncm, "unit": unit, "cst": cst_icms, "code": code})
        s.flush()
        prod_id = p["id"]
    else:
        s.execute(text("""
            INSERT INTO products (code, name, name_norm, ncm, unit, cst_icms, active)
            VALUES (:code, :name, :name_norm, :ncm, :unit, :cst, TRUE)
        """), {"code": code, "name": name, "name_norm": name_norm, "ncm": ncm, "unit": unit, "cst": cst_icms})
        s.flush()
        prod_id = s.execute(text("SELECT id FROM products WHERE code=:code"), {"code": code}).scalar_one()

    return s.get(Product, prod_id)

def ensure_alias(s: Session, *, product_id: int, store_id: str, alias: str) -> None:
    alias_norm = normalize_name(alias)
    s.execute(text("""
        INSERT INTO product_aliases (product_id, store_id, alias, alias_norm)
        VALUES (:pid, :store, :alias, :alias_norm)
        ON CONFLICT (store_id, alias_norm) DO NOTHING
    """), {"pid": product_id, "store": store_id, "alias": alias, "alias_norm": alias_norm})

# -------- Sugestão (fuzzy) sem gravar automático --------
def best_suggestion(s: Session, name: str, min_score: int = 85) -> Tuple[Optional[int], float]:
    """
    Retorna (product_id_sugerido, score) usando fuzzy token_sort_ratio (RapidFuzz).
    Não grava nada; apenas sugere.
    """
    try:
        from rapidfuzz import process, fuzz
    except Exception:
        return None, 0.0

    name_norm = normalize_name(name)
    rows = s.execute(text("SELECT id, name_norm FROM products WHERE active")).all()
    if not rows:
        return None, 0.0

    ids = [r[0] for r in rows]
    names = [r[1] for r in rows]
    match, score, idx = process.extractOne(name_norm, names, scorer=fuzz.token_sort_ratio)
    if score >= min_score:
        return ids[idx], float(score)
    return None, float(score)

# -------- Inbox (pendências para revisão) --------
def enqueue_inbox(
    s: Session,
    *,
    store_id: str,
    raw_name: str,
    raw_code: str | None,
    raw_ncm: str | None,
    raw_unit: str | None,
    reason: str,
    suggested_product_id: int | None = None,
    score: float | None = None,
) -> None:
    s.execute(text("""
        INSERT INTO product_inbox
            (store_id, raw_name, raw_code, raw_ncm, raw_unit, reason, suggested_product_id, score)
        VALUES (:store, :name, :code, :ncm, :unit, :reason, :spid, :score)
    """), {"store": store_id, "name": raw_name, "code": raw_code, "ncm": raw_ncm,
           "unit": raw_unit, "reason": reason, "spid": suggested_product_id, "score": score})

def approve_inbox_link_alias(
    s: Session, *, inbox_id: int, product_id: int, store_id: str, alias: str
) -> None:
    """
    Aprova um item da inbox vinculando como alias.
    """
    ensure_alias(s, product_id=product_id, store_id=store_id, alias=alias)
    s.execute(text("DELETE FROM product_inbox WHERE id=:id"), {"id": inbox_id})

def approve_inbox_create_product(
    s: Session,
    *,
    inbox_id: int,
    store_id: str,
    code: str,
    name: str,
    ncm: str | None = None,
    unit: str | None = None,
    cst_icms: str | None = None,
) -> int:
    """
    Cria um novo produto canônico a partir do item da inbox e gera o alias.
    Retorna o product_id criado.
    """
    p = upsert_product_by_code(s, code=code, name=name, ncm=ncm, unit=unit, cst_icms=cst_icms)
    ensure_alias(s, product_id=p.id, store_id=store_id, alias=name)
    s.execute(text("DELETE FROM product_inbox WHERE id=:id"), {"id": inbox_id})
    return p.id

# -------- Pipeline de importação (linha a linha) --------
def import_row(
    s: Session,
    *,
    store_id: str,
    name: str,
    code: str | None,
    ncm: str | None,
    unit: str | None,
    cst_icms: str | None,
    min_fuzzy_score: int = 90,
) -> dict:
    """
    Regra:
    1) Se vier code -> upsert em products + cria/garante alias.
    2) Sem code -> tenta achar alias exato (normalize) dessa loja.
    3) Não achou -> calcula sugestão fuzzy e manda para inbox (com score).
    """
    name_norm = normalize_name(name)

    if code:
        prod = upsert_product_by_code(s, code=code, name=name, ncm=ncm, unit=unit, cst_icms=cst_icms)
        ensure_alias(s, product_id=prod.id, store_id=store_id, alias=name)
        return {"status": "upsert_by_code", "product_id": prod.id}

    # tenta alias exato (normalize) por loja
    pid = s.execute(text("""
        SELECT p.id
          FROM product_aliases a
          JOIN products p ON p.id = a.product_id
         WHERE a.store_id=:store AND a.alias_norm=:an
         LIMIT 1
    """), {"store": store_id, "an": name_norm}).scalar()

    if pid:
        ensure_alias(s, product_id=pid, store_id=store_id, alias=name)
        return {"status": "matched_by_alias", "product_id": pid}

    # sugestão fuzzy (não grava)
    spid, score = best_suggestion(s, name, min_score=min_fuzzy_score)
    enqueue_inbox(
        s, store_id=store_id, raw_name=name, raw_code=code, raw_ncm=ncm,
        raw_unit=unit, reason="no_match", suggested_product_id=spid, score=score or None
    )
    return {"status": "queued_inbox", "suggested_product_id": spid, "score": score}
