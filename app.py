# app.py
import os
import streamlit as st
import db

st.set_page_config(page_title="Streamlit + Neon", page_icon="🟢")
st.title("Streamlit + Neon")

def get_database_url():
    try:
        url = st.secrets.get("connections", {}).get("neon", {}).get("url")  # type: ignore
        if url: return url
    except Exception:
        pass
    return os.getenv("DATABASE_URL")

DATABASE_URL = get_database_url()
if not DATABASE_URL:
    st.error("Defina [connections.neon].url nos Secrets ou a variável DATABASE_URL.")
    st.stop()

@st.cache_resource
def get_engine():
    return db.make_engine(DATABASE_URL)

engine = get_engine()
db.init_db(engine)

with st.form("f"):
    content = st.text_input("Mensagem")
    submitted = st.form_submit_button("Enviar")

if submitted:
    if not content.strip():
        st.warning("Digite algo.")
    else:
        try:
            new_id = db.insert_message(engine, content.strip())
            st.success(f"Mensagem salva com id {new_id}.")
        except Exception as e:
            st.error(f"Falha ao salvar: {e}")

st.subheader("Últimas mensagens")
try:
    rows = db.fetch_recent(engine, 5)
    if rows:
        for r in rows:
            st.write(f"• #{r['id']} — {r['content']}  _(em {r['created_at']})_")
    else:
        st.write("Ainda não há mensagens.")
except Exception as e:
    st.error(f"Falha ao listar: {e}")
