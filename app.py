# app.py
import os
import streamlit as st
import db  # seu db.py síncrono
import os, base64, tempfile, stat
from sqlalchemy.orm import Session

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


def materialize_cert_from_secrets() -> tuple[str, str]:
    """Cria um .pfx temporário a partir dos Secrets e retorna (path, senha)."""
    try:
        pfx_b64 = st.secrets["cert"]["pfx_b64"]
        pwd = st.secrets["cert"]["password"]
    except Exception:
        st.error("Secrets do certificado não encontrados. Defina [cert].")
        st.stop()

    # arquivo temporário exclusivo
    fd, pfx_path = tempfile.mkstemp(suffix=".pfx")
    os.close(fd)
    with open(pfx_path, "wb") as f:
        f.write(base64.b64decode(pfx_b64))

    # restringe permissões (Linux no Streamlit Cloud). No Windows local, ignora silenciosamente.
    try:
        os.chmod(pfx_path, stat.S_IRUSR | stat.S_IWUSR)
    except Exception:
        pass

    return pfx_path, pwd


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


# importação de uma linha (exemplo)
if st.button("Importar linha de exemplo"):
    with Session(engine) as s, s.begin():
        res = db.import_row(
            s,
            store_id="loja_a",
            name="PIMENTAO VERDE PCT",
            code="",             # sem código -> vai tentar alias/fuzzy
            ncm="07096000",
            unit="KG",
            cst_icms="Sim",
            min_fuzzy_score=90   # só sugere se >= 90
        )
    st.write(res)