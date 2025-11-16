from __future__ import annotations

# app.py
import base64
import os
import stat
import tempfile
import unicodedata

import pandas as pd
import streamlit as st
from sqlalchemy import select
from sqlalchemy.orm import Session
from typing import Any

import db  # seu db.py sincrono
import nfe_business

st.set_page_config(page_title="NFe App", page_icon=":scroll:")


# -------------------- LOGIN GATE --------------------
def require_login() -> None:
    if "auth_ok" not in st.session_state:
        st.session_state.auth_ok = False

    if st.session_state.auth_ok:
        with st.sidebar:
            st.caption("Sessao autenticada")
            if st.button("Sair", type="primary"):
                st.session_state.auth_ok = False
                st.rerun()
        return

    st.title("Login")
    with st.form("login_form", clear_on_submit=False):
        user = st.text_input("Usuario")
        pwd = st.text_input("Senha", type="password")
        ok = st.form_submit_button("Entrar", type="primary")

    if ok:
        try:
            u_ok = st.secrets["auth"].get("username")
            p_ok = st.secrets["auth"].get("password")
        except Exception:
            st.error("Secrets de autenticacao nao encontrados. Defina [auth] em Settings > Secrets.")
            st.stop()

        if user == u_ok and pwd == p_ok:
            st.session_state.auth_ok = True
            st.success("Autenticado! Recarregando aplicacao...")
            st.rerun()
        else:
            st.error("Usuario ou senha invalidos.")
    st.stop()


require_login()
# ------------------ FIM LOGIN GATE ------------------

EXPECTED_COLUMNS = [
    "produto",
    "quantidade",
    "preco uni",
    "total",
    "codigo",
    "ncm",
    "cfop",
    "unid",
]


def materialize_cert_from_secrets() -> tuple[str, str]:
    """Cria um .pfx temporario a partir dos Secrets e retorna (path, senha)."""
    try:
        pfx_b64 = st.secrets["cert"]["pfx_b64"]
        pwd = st.secrets["cert"]["password"]
    except Exception:
        st.error("Secrets do certificado nao encontrados. Defina [cert].")
        st.stop()

    fd, pfx_path = tempfile.mkstemp(suffix=".pfx")
    os.close(fd)
    with open(pfx_path, "wb") as f:
        f.write(base64.b64decode(pfx_b64))

    try:
        os.chmod(pfx_path, stat.S_IRUSR | stat.S_IWUSR)
    except Exception:
        pass

    return pfx_path, pwd


def ensure_certificate_loaded() -> None:
    if "certificado_path" in st.session_state:
        return
    cert_path, cert_pwd = materialize_cert_from_secrets()
    st.session_state.certificado_path = cert_path
    st.session_state.senha_certificado = cert_pwd


# --------- Conexao com o banco ----------
def get_database_url():
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
        "Configure a URL do banco em Settings > Secrets ( [connections.neon].url ) "
        "ou defina a variavel de ambiente DATABASE_URL."
    )
    st.stop()


@st.cache_resource
def get_engine():
    return db.make_engine(DATABASE_URL)


def fetch_clients(engine) -> list[db.Client]:
    with Session(engine) as session:
        stmt = select(db.Client).order_by(db.Client.nome)
        return list(session.scalars(stmt).all())


def buscar_produto(engine, codigo: str | None, nome: str | None):
    if not codigo and not nome:
        return None
    with Session(engine) as session:
        stmt = select(db.Product)
        if codigo:
            stmt = stmt.where(db.Product.code == codigo.strip())
        elif nome:
            termo = db.normalize_name(nome)
            stmt = stmt.where(db.Product.name_norm.contains(termo))
        stmt = stmt.limit(1)
        return session.scalars(stmt).first()


def safe_float(value, default: float = 0.0) -> float:
    if value in (None, ""):
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def normalize_header(value) -> str:
    normalized = unicodedata.normalize("NFKD", str(value)).encode("ascii", "ignore").decode("ascii")
    return normalized.strip().lower()


def read_products_dataframe(uploaded_file):
    uploaded_file.seek(0)
    name = uploaded_file.name.lower()
    if name.endswith(".csv"):
        df = pd.read_csv(uploaded_file)
    else:
        df = pd.read_excel(uploaded_file)
    df.columns = [normalize_header(col) for col in df.columns]
    return df


def parse_products_file(uploaded_file) -> list[dict]:
    df = read_products_dataframe(uploaded_file)
    missing = [col for col in EXPECTED_COLUMNS if col not in df.columns]
    if missing:
        st.error("Colunas faltando na planilha: " + ", ".join(missing))
        return []

    registros: list[dict] = []
    for _, row in df.iterrows():
        registro = {
            "codigo": str(row.get("codigo", "")).strip(),
            "nome": str(row.get("produto", "")).strip(),
            "ncm": str(row.get("ncm", "")).strip(),
            "cfop": str(row.get("cfop", "")).strip(),
            "unidade": str(row.get("unid", "")).strip() or "UN",
            "quantidade": safe_float(row.get("quantidade"), 1.0),
            "valor_unitario": safe_float(row.get("preco uni"), 0.0),
            "valor_total": safe_float(row.get("total"), 0.0),
            "cst_pis": "99",
            "cst_cofins": "99",
            "cst_icms": "40",
        }
        if not registro["valor_total"]:
            registro["valor_total"] = registro["quantidade"] * registro["valor_unitario"]
        registros.append(registro)
    return registros


engine = get_engine()
db.init_db(engine)
ensure_certificate_loaded()

st.session_state.setdefault("produtos", [])
st.session_state.setdefault("produto_preselecionado", {})

st.title("Emissor NFe")
st.caption("Acesso restrito - Streamlit + Neon")

with st.sidebar:
    st.success("Banco conectado")
    st.caption(
        f"URL banco: {DATABASE_URL.split('@')[-1] if DATABASE_URL else 'indefinida'}"
    )
    st.caption("Certificado carregado a partir dos secrets.")
    st.metric("Produtos na sessao", len(st.session_state.produtos))

aba_planilha, aba_manual, aba_xml = st.tabs(["Importar planilha", "Montar manualmente", "Importar XMLs"])

with aba_planilha:
    st.subheader("Importar produtos via planilha")
    st.write(
        "Envie um arquivo CSV ou XLSX contendo ao menos as colunas: "
        + ", ".join(EXPECTED_COLUMNS)
        + "."
    )
    arquivo = st.file_uploader("Selecione a planilha", type=["csv", "xlsx", "xls"])
    if arquivo is not None:
        try:
            df_preview = read_products_dataframe(arquivo)
            st.dataframe(df_preview.head(20))
            arquivo.seek(0)
        except Exception as exc:
            st.error(f"Erro ao ler arquivo: {exc}")
            arquivo = None

    if st.button("Processar planilha", type="primary", disabled=arquivo is None):
        if arquivo is None:
            st.warning("Envie um arquivo antes de processar.")
        else:
            produtos_importados = parse_products_file(arquivo)
            if produtos_importados:
                st.session_state.produtos = produtos_importados
                st.success(f"{len(produtos_importados)} produtos foram carregados na sessao.")
            else:
                st.warning("Nenhum produto foi importado. Verifique a planilha.")

with aba_manual:
    st.subheader("Selecionar cliente")
    clientes = fetch_clients(engine)
    if not clientes:
        st.info("Nenhum cliente cadastrado ainda.")
    else:
        opcoes = {f"{c.nome} ({c.documento})": c.id for c in clientes}
        nomes = list(opcoes.keys())
        cliente_default = 0
        cliente_id_atual = st.session_state.get("cliente_id")
        if cliente_id_atual:
            for idx, label in enumerate(nomes):
                if opcoes[label] == cliente_id_atual:
                    cliente_default = idx
                    break
        escolha = st.selectbox("Cliente para emissao", nomes, index=cliente_default)
        st.session_state.cliente_id = opcoes[escolha]
        cliente_obj = next(c for c in clientes if c.id == st.session_state.cliente_id)
        with st.expander("Dados do cliente"):
            st.write(
                {
                    "Documento": cliente_obj.documento,
                    "Nome": cliente_obj.nome,
                    "Cidade": f"{cliente_obj.cidade}/{cliente_obj.uf}",
                    "Endereco": f"{cliente_obj.logradouro}, {cliente_obj.numero}",
                    "Telefone": cliente_obj.telefone,
                    "Email": cliente_obj.email,
                }
            )

    st.subheader("Buscar produto no banco")
    with st.form("buscar_produto_form"):
        codigo_busca = st.text_input("Codigo do produto")
        nome_busca = st.text_input("Nome (opcional)")
        buscar = st.form_submit_button("Buscar", type="secondary")
    if buscar:
        produto_banco = buscar_produto(engine, codigo_busca, nome_busca)
        if produto_banco:
            st.session_state.produto_preselecionado = {
                "codigo": produto_banco.code,
                "nome": produto_banco.name,
                "ncm": produto_banco.ncm or "",
                "unidade": produto_banco.unit or "UN",
                "cfop": "5102",
                "cst_icms": "40",
                "cst_pis": "99",
                "cst_cofins": "99",
            }
            st.success(f"Produto '{produto_banco.name}' carregado no formulario.")
        else:
            st.warning("Produto nao encontrado.")

    st.subheader("Adicionar produto manualmente")
    pre = st.session_state.get("produto_preselecionado", {})
    quantidade_default = float(pre.get("quantidade", 1.0) or 1.0)
    valor_unitario_default = float(pre.get("valor_unitario", 0.0) or 0.0)
    with st.form("form_produto_manual"):
        codigo = st.text_input("Codigo", value=pre.get("codigo", ""))
        nome = st.text_input("Descricao", value=pre.get("nome", ""))
        ncm = st.text_input("NCM", value=pre.get("ncm", ""))
        cfop = st.text_input("CFOP", value=pre.get("cfop", "5102"))
        unidade = st.text_input("Unidade", value=pre.get("unidade", "UN"))
        quantidade = st.number_input("Quantidade", min_value=0.0, value=quantidade_default, step=1.0)
        valor_unitario = st.number_input(
            "Valor unitario", min_value=0.0, value=valor_unitario_default, step=0.01, format="%.2f"
        )
        cst_icms = st.text_input("CST ICMS", value=pre.get("cst_icms", "40"))
        adicionar = st.form_submit_button("Adicionar produto", type="primary")

    if adicionar:
        if not codigo or not nome:
            st.error("Preencha ao menos codigo e descricao do produto.")
        else:
            novo_produto = {
                "codigo": codigo.strip(),
                "nome": nome.strip(),
                "ncm": ncm.strip(),
                "cfop": cfop.strip(),
                "unidade": unidade.strip() or "UN",
                "quantidade": float(quantidade),
                "valor_unitario": float(valor_unitario),
                "valor_total": float(quantidade) * float(valor_unitario),
                "cst_pis": "99",
                "cst_cofins": "99",
                "cst_icms": cst_icms.strip() or "40",
            }
            st.session_state.produtos.append(novo_produto)
            st.session_state.produto_preselecionado = {}
            st.success(f"Produto {novo_produto['codigo']} adicionado a lista.")

    st.subheader("Produtos selecionados")
    if st.session_state.produtos:
        df_produtos = pd.DataFrame(st.session_state.produtos)
        st.dataframe(df_produtos)
        for idx, produto in enumerate(list(st.session_state.produtos)):
            if st.button(f"Remover {produto['codigo']}", key=f"rm_{idx}"):
                st.session_state.produtos.pop(idx)
                st.rerun()
    else:
        st.info("Nenhum produto na lista ainda.")

if st.session_state.produtos and st.session_state.get("cliente_id"):
    st.success(
        "Produtos e cliente selecionados! Quando a tela de emissao estiver pronta "
        "bastara confirmar para gerar e enviar a NFe."
    )
else:
    st.warning("Selecione um cliente e adicione produtos para habilitar a emissao da nota.")

with aba_xml:
    st.subheader("Importar XMLs emitidos")
    st.write("Faça upload de um ou mais arquivos XML para armazenar e extrair clientes/produtos automaticamente.")
    arquivos_xml = st.file_uploader("Selecione os XMLs", type=["xml"], accept_multiple_files=True)

    if st.button("Processar XMLs", type="primary", disabled=not arquivos_xml):
        resultados: list[dict[str, Any]] = []
        for arquivo in arquivos_xml or []:
            dados = arquivo.read()
            if not dados:
                resultados.append(
                    {"status": "erro", "arquivo": arquivo.name, "mensagem": "Arquivo vazio ou inválido."}
                )
                continue
            try:
                with Session(engine) as session:
                    with session.begin():
                        resultado = nfe_business.importar_xml_document(session, dados, filename=arquivo.name)
            except Exception as exc:
                resultados.append({"status": "erro", "arquivo": arquivo.name, "mensagem": str(exc)})
            else:
                resultados.append(resultado)

        for info in resultados:
            status = info.get("status")
            arquivo = info.get("arquivo")
            if status == "ok":
                produtos_count = len(info.get("produtos_status") or [])
                st.success(
                    f"{arquivo}: NFe {info.get('numero')} vinculada ao cliente {info.get('cliente')} "
                    f"({produtos_count} produtos processados)."
                )
            elif status == "duplicated":
                st.info(f"{arquivo}: XML já importado anteriormente (nota {info.get('numero')}).")
            else:
                st.error(f"{arquivo}: Falha ao importar XML - {info.get('mensagem')}.")
