# db.py
import re
from typing import List
from sqlalchemy import String, text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.ext.asyncio import (
    create_async_engine,
    AsyncEngine,
    async_sessionmaker,
)
from sqlalchemy.types import TIMESTAMP


# Base ORM
class Base(DeclarativeBase):
    pass


# Modelo simples
class Message(Base):
    __tablename__ = "messages"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    content: Mapped[str] = mapped_column(String(500), nullable=False)
    # timestamp no lado do servidor
    created_at: Mapped[str] = mapped_column(
        TIMESTAMP(timezone=True), server_default=text("now()"), nullable=False
    )


def make_async_url(sync_url: str) -> str:
    cleaned = re.sub(r'\?.*', '', sync_url)
    return re.sub(r"^postgresql:", "postgresql+asyncpg:", cleaned)



def make_engine(sync_database_url: str) -> AsyncEngine:
    async_url = make_async_url(sync_database_url)
    return create_async_engine(async_url, echo=False, pool_pre_ping=True)


async def init_db(engine: AsyncEngine) -> None:
    """Cria a tabela se não existir."""
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


async def insert_message(engine: AsyncEngine, content: str) -> int:
    """Insere uma mensagem e retorna o id."""
    Session = async_sessionmaker(engine, expire_on_commit=False)
    async with Session() as session:
        msg = Message(content=content)
        session.add(msg)
        await session.commit()
        await session.refresh(msg)
        return msg.id


async def fetch_recent(engine: AsyncEngine, limit: int = 5) -> List[Message]:
    """Busca as últimas mensagens para mostrar na UI."""
    Session = async_sessionmaker(engine, expire_on_commit=False)
    async with Session() as session:
        result = await session.execute(
            text(
                "SELECT id, content, created_at "
                "FROM messages ORDER BY id DESC LIMIT :lim"
            ),
            {"lim": limit},
        )
        # retorna como dicionários
        rows = [
            {"id": r[0], "content": r[1], "created_at": r[2]} for r in result.fetchall()
        ]
        return rows
