# app.py
import os
import asyncio
import streamlit as st

import db  # nosso ORM

st.set_page_config(page_title="Streamlit + Neon (Postgres)", page_icon="🟢")

st.title("Streamlit + Neon")
st.caption("Um campo de texto → envia para o banco (Neon/Postgres)")

# 1) Pegue a URL do banco
# - Em produção (Streamlit Cloud): coloque em Settings → Secrets
#     [connections.neon]
#     url = "postgresql://usuario:senha@host.neon.tech/db?sslmode=require"
# - Local: você pode usar .env e os.getenv("DATABASE_URL")
DATABASE_URL = (
    st.secrets.get("connections", {})
    .get("neon", {})
    .get("url")
    or os.getenv("DATABASE_URL")
)

if not DATABASE_URL:
    st.error(
        "Configure a variável DATABASE_URL ou o secret [connections.neon].url "
        "para conectar ao banco."
    )
    st.stop()

# 2) Engine assíncrono (criamos uma vez só)
@st.cache_resource
def get_engine():
    return db.make_engine(DATABASE_URL)

engine = get_engine()

# 3) Cria a tabela na primeira execução
@st.cache_resource
def init_once():
    asyncio.run(db.init_db(engine))
    return True

init_once()

# 4) UI simples
with st.form("form"):
    content = st.text_input("Mensagem")
    submitted = st.form_submit_button("Enviar")

if submitted:
    if not content.strip():
        st.warning("Digite alguma coisa antes de enviar.")
    else:
        try:
            new_id = asyncio.run(db.insert_message(engine, content.strip()))
            st.success(f"Mensagem salva com id {new_id}.")
        except Exception as e:
            st.error(f"Falha ao salvar: {e}")

# 5) Mostrar últimas mensagens
st.subheader("Últimas mensagens")
try:
    rows = asyncio.run(db.fetch_recent(engine, limit=5))
    if rows:
        for r in rows:
            st.write(f"• #{r['id']} — {r['content']}  _(em {r['created_at']})_")
    else:
        st.write("Ainda não há mensagens.")
except Exception as e:
    st.error(f"Falha ao listar mensagens: {e}")
