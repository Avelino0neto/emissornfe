# app.py
import os
import streamlit as st
import db  # seu db.py síncrono

# -------------------- LOGIN GATE --------------------
def require_login():
    if "auth_ok" not in st.session_state:
        st.session_state.auth_ok = False

    if st.session_state.auth_ok:
        with st.sidebar:
            st.caption("🔐 Sessão autenticada")
            if st.button("Sair"):
                st.session_state.auth_ok = False
                st.rerun()
        return  # segue o app normalmente

    st.set_page_config(page_title="NFe App", page_icon="🧾")
    st.title("🔐 Login")
    with st.form("login_form", clear_on_submit=False):
        user = st.text_input("Usuário")
        pwd = st.text_input("Senha", type="password")
        ok = st.form_submit_button("Entrar")

    if ok:
        try:
            u_ok = st.secrets["auth"].get("username")
            p_ok = st.secrets["auth"].get("password")
        except Exception:
            st.error("Secrets de autenticação não encontrados. Defina [auth] em Settings → Secrets.")
            st.stop()

        if user == u_ok and pwd == p_ok:
            st.session_state.auth_ok = True
            st.success("Autenticado!")
            st.rerun()
        else:
            st.error("Usuário ou senha inválidos.")
    st.stop()  # bloqueia o restante do app até logar

require_login()
# ------------------ FIM LOGIN GATE ------------------

st.set_page_config(page_title="NFe App", page_icon="🧾")
st.title("Emissor NFe (demo)")
st.caption("Acesso restrito • Streamlit + Neon")

# --------- Conexão com o banco ----------
def get_database_url():
    # tenta secrets primeiro; se não houver, usa variável de ambiente
    try:
        url = st.secrets.get("connections", {}).get("neon", {}).get("url")  # type: ignore
        if url:
            return url
    except Exception:
        pass
    return os.getenv("DATABASE_URL")

DATABASE_URL = get_database_url()
if not DATABASE_URL:
    st.error(
        "Configure a URL do banco em Settings → Secrets ( [connections.neon].url ) "
        "ou defina a variável de ambiente DATABASE_URL."
    )
    st.stop()

@st.cache_resource
def get_engine():
    return db.make_engine(DATABASE_URL)

engine = get_engine()
db.init_db(engine)

# --------- UI mínima (campo + botão) ----------
st.subheader("Cadastrar mensagem de teste")
with st.form("f_msg"):
    content = st.text_input("Mensagem")
    s1 = st.form_submit_button("Enviar")
if s1:
    if not content.strip():
        st.warning("Digite algo.")
    else:
        try:
            new_id = db.insert_message(engine, content.strip())
            st.success(f"Salvo! id={new_id}")
        except Exception as e:
            st.error(f"Falha ao salvar: {e}")

# --------- (Opcional) Upload de XML NFe ----------
st.subheader("Upload de XML de NFe")
xml_file = st.file_uploader("Selecione um arquivo .xml", type=["xml"])
if xml_file is not None:
    # Aqui você pode ler o conteúdo e salvar no banco.
    # Exemplo simples: armazenar o XML cru numa tabela (você pode criar outra tabela no db.py)
    xml_bytes = xml_file.read()
    st.info(f"Arquivo {xml_file.name} com {len(xml_bytes)} bytes carregado (exemplo).")
    # TODO: parsear, validar e inserir no banco conforme seu modelo

# --------- Últimos registros ----------
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
