# db.py (versão síncrona)
from sqlalchemy import String, text, TIMESTAMP, create_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, Session

class Base(DeclarativeBase):
    pass

class Message(Base):
    __tablename__ = "messages"
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    content: Mapped[str] = mapped_column(String(500), nullable=False)
    created_at: Mapped[str] = mapped_column(TIMESTAMP(timezone=True),
                                            server_default=text("now()"), nullable=False)

def make_engine(sync_url: str):
    # psycopg2 aceita ?sslmode=require normalmente
    return create_engine(sync_url, pool_pre_ping=True, future=True)

def init_db(engine) -> None:
    Base.metadata.create_all(engine)

def insert_message(engine, content: str) -> int:
    with Session(engine) as s:
        msg = Message(content=content)
        s.add(msg)
        s.commit()
        s.refresh(msg)
        return msg.id

def fetch_recent(engine, limit: int = 5):
    with Session(engine) as s:
        rows = s.execute(
            text("SELECT id, content, created_at FROM messages ORDER BY id DESC LIMIT :lim"),
            {"lim": limit},
        ).all()
        return [{"id": r[0], "content": r[1], "created_at": r[2]} for r in rows]
