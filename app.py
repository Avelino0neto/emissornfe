from __future__ import annotations

# app.py
import base64
import os
import stat
import tempfile
import unicodedata
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Any, Optional, Iterable

import pandas as pd
import streamlit as st
from lxml import etree
from pynfe.processamento.comunicacao import ComunicacaoSefaz
from sqlalchemy import select
from sqlalchemy.orm import Session

import db  # seu db.py sincrono
import nfe_business

try:
    from brazilfiscalreport.danfe import Danfe
except Exception:  # pragma: no cover
    Danfe = None

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
REQUIRED_COLUMNS = [
    col for col in EXPECTED_COLUMNS if col != "codigo"
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


def ensure_comunicacao() -> None:
    if st.session_state.get("comunicacao"):
        return
    cert_path = st.session_state.get("certificado_path")
    cert_pwd = st.session_state.get("senha_certificado")
    if not cert_path or not cert_pwd:
        return
    try:
        uf = nfe_business.get_emitente_data()["uf"]
    except Exception:
        return
    st.session_state.comunicacao = ComunicacaoSefaz(
        uf=uf,
        certificado=cert_path,
        certificado_senha=cert_pwd,
        homologacao=False,
    )


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


def buscar_produtos_por_nome(engine, nome: str, limite: int = 20):
    termo = db.normalize_name(nome)
    with Session(engine) as session:
        stmt = (
            select(db.Product)
            .where(db.Product.name_norm.contains(termo))
            .order_by(db.Product.name)
            .limit(limite)
        )
        return list(session.scalars(stmt).all())


def preencher_codigos_por_alias(engine, cliente_id: int | None, produtos: list[dict]):
    if not cliente_id or not produtos:
        return
    with Session(engine) as session:
        cliente = session.get(db.Client, cliente_id)
        if not cliente:
            return
        store_id = cliente.documento or f"cliente_{cliente.id}"
        for produto in produtos:
            if produto.get("codigo"):
                continue
            nome = produto.get("nome", "")
            alias_norm = db.normalize_name(nome)
            if not alias_norm:
                continue
            stmt_alias = (
                select(db.Product.code)
                .join(db.ProductAlias, db.ProductAlias.product_id == db.Product.id)
                .where(
                    db.ProductAlias.store_id == store_id,
                    db.ProductAlias.alias_norm == alias_norm,
                )
                .limit(1)
            )
            code = session.execute(stmt_alias).scalar()
            if not code:
                stmt_prod = (
                    select(db.Product.code)
                    .where(db.Product.name_norm == alias_norm)
                    .limit(1)
                )
                code = session.execute(stmt_prod).scalar()
            if code:
                produto["codigo"] = code


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
        df = pd.read_csv(uploaded_file, dtype=str)
    else:
        df = pd.read_excel(uploaded_file, dtype=str)
    df.columns = [normalize_header(col) for col in df.columns]
    return df


def parse_products_file(uploaded_file) -> list[dict]:
    df = read_products_dataframe(uploaded_file)
    missing = [col for col in REQUIRED_COLUMNS if col not in df.columns]
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


def parse_emitida_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    txt = str(value).strip()
    if not txt:
        return None
    txt = txt.replace("Z", "")
    try:
        return datetime.fromisoformat(txt)
    except ValueError:
        pass
    try:
        return datetime.strptime(txt[:19], "%Y-%m-%dT%H:%M:%S")
    except ValueError:
        pass
    try:
        return datetime.strptime(txt[:10], "%Y-%m-%d")
    except ValueError:
        return None


def consultar_notas(engine, inicio: date, fim: date, incluir_canceladas: bool = False) -> list[dict[str, Any]]:
    with Session(engine) as session:
        stmt = (
            select(db.NfeXml, db.Client)
            .join(db.Client, db.NfeXml.client_id == db.Client.id)
            .order_by(db.NfeXml.numero.desc())
        )
        if not incluir_canceladas:
            stmt = stmt.where(db.NfeXml.cancelada.is_(False))
        rows = session.execute(stmt).all()

    notas: list[dict[str, Any]] = []
    for nfe, cliente in rows:
        dt = parse_emitida_datetime(nfe.emitida_em)
        dt_date = dt.date() if dt else None
        if dt_date:
            if inicio and dt_date < inicio:
                continue
            if fim and dt_date > fim:
                continue
        notas.append(
            {
                "data": dt.strftime("%Y-%m-%d %H:%M") if dt else (nfe.emitida_em or ""),
                "numero": nfe.numero,
                "cliente": (cliente.nome_fantasia or cliente.nome) if cliente else "",
                "documento": cliente.documento if cliente else "",
                "valor_total": float(nfe.valor_total or 0),
                "cancelada": nfe.cancelada,
            }
        )
    return notas


def listar_notas_emitidas(engine, limite: int = 20) -> list[tuple[int, str, Optional[str], str]]:
    with Session(engine) as session:
        stmt = (
            select(db.NfeXml.id, db.NfeXml.numero, db.NfeXml.emitida_em, db.NfeXml.xml_text)
            .where(db.NfeXml.cancelada.is_(False))
            .order_by(db.NfeXml.numero.desc())
            .limit(limite)
        )
        return session.execute(stmt).all()


def obter_xml_por_chave(engine, chave: str) -> Optional[str]:
    if not chave:
        return None
    try:
        numero = str(int(chave[22:31]))
    except ValueError:
        return None
    with Session(engine) as session:
        stmt = (
            select(db.NfeXml.xml_text)
            .where(db.NfeXml.numero == numero)
            .order_by(db.NfeXml.numero.desc())
            .limit(1)
        )
        return session.scalars(stmt).first()

def obter_xml_por_numero(engine, numero: str) -> Optional[str]:
    if not numero:
        return None
    with Session(engine) as session:
        stmt = (
            select(db.NfeXml.xml_text)
            .where(db.NfeXml.numero == numero)
            .order_by(db.NfeXml.id.desc())
            .limit(1)
        )
        return session.scalars(stmt).first()


def extrair_chave_protocolo(xml_text: str) -> tuple[str, str]:
    if not xml_text:
        return "", ""
    try:
        root = etree.fromstring(xml_text.encode("utf-8"))
    except Exception:
        return "", ""
    ns = {"nfe": "http://www.portalfiscal.inf.br/nfe"}
    chave = ""
    inf_nfe = root.find(".//nfe:infNFe", namespaces=ns)
    if inf_nfe is not None:
        chave = (inf_nfe.get("Id") or "").replace("NFe", "")
    protocolo = root.findtext(".//nfe:protNFe/nfe:infProt/nfe:nProt", namespaces=ns) or ""
    return chave, protocolo


def ensure_date(value: Any, fallback: date | None = None) -> date:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value).date()
        except ValueError:
            pass
    return fallback or date.today()


def format_currency(value: float | int | Decimal) -> str:
    return f"R$ {Decimal(value):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def chunked(iterable: Iterable[str], size: int) -> list[list[str]]:
    chunk: list[str] = []
    for item in iterable:
        chunk.append(item)
        if len(chunk) == size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk


def transmitir_nfe(engine, origem: str) -> None:
    cliente_id = st.session_state.get("cliente_id")
    produtos = st.session_state.get("produtos", [])
    if not cliente_id:
        st.error("Selecione um cliente antes de transmitir.")
        return
    if not produtos:
        st.error("Adicione produtos antes de transmitir.")
        return

    preencher_codigos_por_alias(engine, cliente_id, produtos)

    dados_nfe = {
        "nfe_data": date.today(),
        "nfe_numero": "1",
        "nfe_serie": "1",
        "nfe_natureza": "Venda de mercadorias",
        "nfe_tipo": "Saída",
        "nfe_finalidade": "Normal",
        "nfe_consumidor": "Não",
        "nfe_presenca": "Presencial",
        "forma_pagamento": "Boleto",
        "homologacao": False,
    }

    with Session(engine) as session:
        ultimo_numero = session.execute(
            select(db.NfeXml.numero).order_by(db.NfeXml.numero.desc()).limit(1)
        ).scalar()
        if ultimo_numero:
            try:
                dados_nfe["nfe_numero"] = str(int(ultimo_numero) + 1)
            except ValueError:
                dados_nfe["nfe_numero"] = ultimo_numero

    with Session(engine) as session:
        resultado = nfe_business.criar_nfe_pynfe(
            session,
            nfe_data=dados_nfe["nfe_data"],
            nfe_numero=dados_nfe["nfe_numero"],
            nfe_serie=dados_nfe["nfe_serie"],
            nfe_natureza=dados_nfe["nfe_natureza"],
            nfe_tipo=dados_nfe["nfe_tipo"],
            nfe_finalidade=dados_nfe["nfe_finalidade"],
            nfe_consumidor=dados_nfe["nfe_consumidor"],
            nfe_presenca=dados_nfe["nfe_presenca"],
            forma_pagamento=dados_nfe["forma_pagamento"],
            homologacao=dados_nfe["homologacao"],
            cliente_id=cliente_id,
        )

    if not resultado.get("sucesso"):
        mensagem = (
            resultado.get("erro")
            or resultado.get("resultado_detalhes")
            or str(resultado.get("resultado"))
        )
        st.error(f"Falha na transmissão ({origem}): {mensagem}")
        if resultado.get("erro_completo"):
            with st.expander("Detalhes do erro"):
                st.code(resultado["erro_completo"])
        else:
            with st.expander("Detalhes técnicos"):
                st.write(resultado.get("resultado"))
        return

    st.success(
        f"NFe {dados_nfe['nfe_numero']} transmitida! Código SEFAZ: {resultado.get('resultado_codigo')}"
    )

    xml_element = resultado.get("xml_assinado")
    xml_bytes = None
    if xml_element is not None:
        xml_bytes = etree.tostring(xml_element, encoding="utf-8")
        try:
            with Session(engine) as session:
                with session.begin():
                    nfe_business.importar_xml_document(
                        session,
                        xml_bytes,
                        filename=f"{dados_nfe['nfe_numero']}-enviada.xml",
                    )
        except Exception as exc:
            st.warning(f"Não foi possível salvar o XML no banco: {exc}")

    if xml_bytes is not None:
        xml_str = xml_bytes.decode("utf-8")
        st.download_button(
            label="📥 Baixar XML Assinado",
            data=xml_str,
            file_name=f"NFe_{int(dados_nfe['nfe_numero']):09d}_assinada.xml",
            mime="application/xml",
            key=f"download_xml_{dados_nfe['nfe_numero']}",
        )
        if Danfe:
            try:
                danfe = Danfe(xml=xml_str)
                with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp_pdf:
                    danfe.output(tmp_pdf.name)
                    tmp_path = tmp_pdf.name
                with open(tmp_path, "rb") as f_pdf:
                    pdf_data = f_pdf.read()
                st.download_button(
                    label="🖨️ Baixar DANFE (PDF)",
                    data=pdf_data,
                    file_name=f"DANFE_{int(dados_nfe['nfe_numero']):09d}.pdf",
                    mime="application/pdf",
                    key=f"download_danfe_{dados_nfe['nfe_numero']}",
                )
                os.unlink(tmp_path)
            except Exception as exc:
                st.warning(f"Não foi possível gerar o DANFE: {exc}")

    st.session_state.produtos = []
    st.session_state.produto_preselecionado = {}


engine = get_engine()
db.init_db(engine)
ensure_certificate_loaded()
ensure_comunicacao()

st.session_state.setdefault("produtos", [])
st.session_state.setdefault("produto_preselecionado", {})
st.session_state.setdefault("busca_produtos_resultados", [])
st.session_state.setdefault("comunicacao", None)
st.session_state.setdefault("cancel_note_idx", None)
st.session_state.setdefault("cancel_chave", "")
st.session_state.setdefault("cancel_protocolo", "")

st.title("Emissor NFe")
st.caption("Acesso restrito - Streamlit + Neon")

st.session_state["nfe_data"] = date.today()
st.session_state.setdefault("nfe_data", date.today())

st.subheader("Selecionar cliente")
clientes = fetch_clients(engine)
if not clientes:
    st.info("Nenhum cliente cadastrado ainda.")
    cliente_obj = None
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
    escolha = st.selectbox("Cliente para emissão", nomes, index=cliente_default)
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

with st.sidebar:
    st.success("Banco conectado")
    st.caption(
        f"URL banco: {DATABASE_URL.split('@')[-1] if DATABASE_URL else 'indefinida'}"
    )
    st.caption("Certificado carregado a partir dos secrets.")
    st.metric("Produtos na sessao", len(st.session_state.produtos))

aba_planilha, aba_manual, aba_xml, aba_relatorio, aba_consultar, aba_cliente, aba_cancelar = st.tabs(
    ["Importar planilha", "Montar manualmente", "Importar XMLs", "Relatorio", "Consultar", "Cadastrar cliente", "Cancelar NFe"]
)

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
                if st.session_state.get("cliente_id"):
                    preencher_codigos_por_alias(
                        engine, st.session_state.cliente_id, st.session_state.produtos
                    )
                else:
                    st.info("Selecione um cliente para vincular/capturar códigos automaticamente.")
                st.success(f"{len(produtos_importados)} produtos foram carregados na sessao.")
            else:
                st.warning("Nenhum produto foi importado. Verifique a planilha.")

    pronto_para_transmitir = bool(st.session_state.produtos and st.session_state.get("cliente_id"))
    if st.button(
        "Transmitir NFe (planilha)",
        type="primary",
        disabled=not pronto_para_transmitir,
        key="transmit_planilha",
    ):
        transmitir_nfe(engine, "planilha")
    if not pronto_para_transmitir:
        st.caption("Selecione um cliente na aba 'Montar manualmente' e adicione produtos para transmitir.")

with aba_manual:
    st.subheader("Buscar produto no banco")
    with st.form("buscar_produto_form"):
        codigo_busca = st.text_input("Codigo do produto")
        nome_busca = st.text_input("Nome (opcional)")
        buscar = st.form_submit_button("Buscar", type="secondary")
    if buscar:
        st.session_state["busca_produtos_resultados"] = []
        if codigo_busca:
            produto_banco = buscar_produto(engine, codigo_busca, None)
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
        elif nome_busca:
            resultados = buscar_produtos_por_nome(engine, nome_busca)
            if not resultados:
                st.warning("Nenhum produto encontrado para esse nome.")
            elif len(resultados) == 1:
                produto = resultados[0]
                st.session_state.produto_preselecionado = {
                    "codigo": produto.code,
                    "nome": produto.name,
                    "ncm": produto.ncm or "",
                    "unidade": produto.unit or "UN",
                    "cfop": "5102",
                    "cst_icms": "40",
                    "cst_pis": "99",
                    "cst_cofins": "99",
                }
                st.success(f"Produto '{produto.name}' carregado no formulario.")
            else:
                st.session_state["busca_produtos_resultados"] = [
                    {
                        "id": prod.id,
                        "codigo": prod.code or "",
                        "nome": prod.name,
                        "ncm": prod.ncm or "",
                        "unidade": prod.unit or "UN",
                    }
                    for prod in resultados
                ]
                st.info("Varios produtos encontrados. Selecione um abaixo.")
        else:
            st.info("Informe código ou parte do nome para buscar.")

    resultados_guardados = st.session_state.get("busca_produtos_resultados") or []
    if resultados_guardados:
        opcoes = {
            f"{item['codigo']} - {item['nome']}": item for item in resultados_guardados
        }
        selecao = st.selectbox(
            "Produtos encontrados",
            options=list(opcoes.keys()),
            key="select_busca_produto",
        )
        if st.button("Usar produto selecionado", key="usar_produto_busca"):
            item = opcoes[selecao]
            st.session_state.produto_preselecionado = {
                "codigo": item["codigo"],
                "nome": item["nome"],
                "ncm": item["ncm"],
                "unidade": item["unidade"],
                "cfop": "5102",
                "cst_icms": "40",
                "cst_pis": "99",
                "cst_cofins": "99",
            }
            st.session_state["busca_produtos_resultados"] = []
            st.success(f"Produto '{item['nome']}' carregado no formulario.")

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
            preencher_codigos_por_alias(
                engine, st.session_state.get("cliente_id"), [st.session_state.produtos[-1]]
            )
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

    pronto_para_transmitir = bool(st.session_state.produtos and st.session_state.get("cliente_id"))
    if st.button(
        "Transmitir NFe (manual)",
        type="primary",
        disabled=not pronto_para_transmitir,
        key="transmit_manual",
    ):
        transmitir_nfe(engine, "manual")

if st.session_state.produtos and st.session_state.get("cliente_id"):
    st.success("Produtos e cliente selecionados! Clique em 'Transmitir NFe' para gerar e enviar.")
else:
    st.warning("Selecione um cliente e adicione produtos para habilitar a emissão da nota.")

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

with aba_relatorio:
    st.subheader("Relatorio de notas emitidas")
    hoje = date.today()
    inicio_padrao = ensure_date(st.session_state.get("relatorio_inicio"), hoje - timedelta(days=30))
    fim_padrao = ensure_date(st.session_state.get("relatorio_fim"), hoje)
    periodo = st.date_input("Período", value=(inicio_padrao, fim_padrao))
    if isinstance(periodo, tuple) and len(periodo) == 2:
        inicio_sel, fim_sel = periodo
    else:
        inicio_sel = fim_sel = periodo  # type: ignore
    if inicio_sel > fim_sel:
        inicio_sel, fim_sel = fim_sel, inicio_sel
    st.session_state["relatorio_inicio"] = inicio_sel
    st.session_state["relatorio_fim"] = fim_sel

    notas = consultar_notas(engine, inicio_sel, fim_sel)
    if not notas:
        st.info("Nenhuma nota encontrada para o período selecionado.")
    else:
        df_rel = pd.DataFrame(notas)
        total_valor = df_rel["valor_total"].sum()
        st.metric("Quantidade de notas", len(df_rel))
        st.metric("Valor total", format_currency(total_valor))
        st.dataframe(df_rel)

        st.subheader("Resumo por loja (formato planilha)")
        lojas = df_rel["cliente"].unique().tolist()
        for grupo in chunked(lojas, 2):
            cols = st.columns(len(grupo))
            for col, loja in zip(cols, grupo):
                df_loja = df_rel[df_rel["cliente"] == loja].copy()
                if df_loja.empty:
                    continue
                df_loja_display = df_loja[["data", "numero", "valor_total"]].copy()
                df_loja_display.columns = ["Data", "Número", "Valor"]
                df_loja_display["Valor"] = df_loja_display["Valor"].apply(format_currency)
                col.markdown(f"<div style='text-align:center;font-weight:bold;'>{loja}</div>", unsafe_allow_html=True)
                html_table = df_loja_display.to_html(index=False, border=0, justify="center")
                col.markdown(
                    f"<div style='border:1px solid #ddd;border-radius:6px;padding:6px;'>{html_table}</div>",
                    unsafe_allow_html=True,
                )
                total_loja = float(df_loja["valor_total"].sum())
                col.markdown(
                    f"<div style='text-align:center;font-weight:bold;margin-top:4px;'>{format_currency(total_loja)}</div>",
                    unsafe_allow_html=True,
                )
                deposito = Decimal(str(total_loja)) * Decimal("0.985")
                col.markdown(
                    f"<div style='text-align:center;font-weight:bold;background-color:#fff79b;padding:4px;margin-top:4px;'>"
                    f"{format_currency(deposito)}</div>",
                    unsafe_allow_html=True,
                )

        csv = df_rel.to_csv(index=False).encode("utf-8")
        st.download_button(
            "Baixar CSV",
            data=csv,
            file_name=f"relatorio_nfe_{inicio_sel}_{fim_sel}.csv",
            mime="text/csv",
        )

with aba_consultar:
    st.subheader("Consultar notas emitidas")
    hoje = date.today()
    inicio_padrao = ensure_date(st.session_state.get("consulta_inicio"), hoje - timedelta(days=30))
    fim_padrao = ensure_date(st.session_state.get("consulta_fim"), hoje)
    periodo = st.date_input("Período (consulta)", value=(inicio_padrao, fim_padrao), key="consulta_periodo")
    if isinstance(periodo, tuple) and len(periodo) == 2:
        inicio_cons, fim_cons = periodo
    else:
        inicio_cons = fim_cons = ensure_date(periodo)
    if inicio_cons > fim_cons:
        inicio_cons, fim_cons = fim_cons, inicio_cons
    st.session_state["consulta_inicio"] = inicio_cons
    st.session_state["consulta_fim"] = fim_cons

    notas_consulta = consultar_notas(engine, inicio_cons, fim_cons, incluir_canceladas=True)
    if not notas_consulta:
        st.info("Nenhuma nota encontrada nesse período.")
    else:
        df_cons = pd.DataFrame(notas_consulta)
        total_consulta = df_cons["valor_total"].sum()
        st.metric("Quantidade de notas", len(df_cons))
        st.metric("Valor total", format_currency(total_consulta))
        st.dataframe(df_cons)
        opcoes = [
            f"NFe {row['numero']} - {row['data']} - {row['cliente']}" for _, row in df_cons.iterrows()
        ]
        idx = st.selectbox(
            "Selecione a nota",
            range(len(opcoes)),
            format_func=lambda i: opcoes[i],
            key="consulta_select",
        )
        nota_selecionada = df_cons.iloc[idx]
        st.success(f"Nota selecionada: {nota_selecionada['numero']} - {nota_selecionada['cliente']}")
        xml_texto = obter_xml_por_numero(engine, nota_selecionada["numero"])
        if xml_texto:
            st.download_button(
                label="📥 Baixar XML",
                data=xml_texto,
                file_name=f"NFe_{int(nota_selecionada['numero']):09d}.xml",
                mime="application/xml",
                key=f"download_xml_consulta_{nota_selecionada['numero']}",
            )
            if Danfe:
                try:
                    danfe = Danfe(xml=xml_texto)
                    with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp_pdf:
                        danfe.output(tmp_pdf.name)
                        tmp_path = tmp_pdf.name
                    with open(tmp_path, "rb") as f_pdf:
                        pdf_data = f_pdf.read()
                    st.download_button(
                        label="🖨️ Baixar DANFE (PDF)",
                        data=pdf_data,
                        file_name=f"DANFE_{int(nota_selecionada['numero']):09d}.pdf",
                        mime="application/pdf",
                        key=f"download_danfe_consulta_{nota_selecionada['numero']}",
                    )
                    os.unlink(tmp_path)
                except Exception as exc:
                    st.warning(f"Não foi possível gerar o DANFE: {exc}")
        else:
            st.warning("XML não encontrado no banco para esta nota.")

with aba_cliente:
    st.subheader("Cadastrar novo cliente via CNPJ")
    cnpj_busca = st.text_input("CNPJ para buscar", value=st.session_state.get("cliente_cnpj_busca", ""))
    if st.button("Buscar e salvar cliente"):
        st.session_state["cliente_cnpj_busca"] = cnpj_busca
        if not cnpj_busca:
            st.warning("Informe um CNPJ.")
        else:
            with st.spinner("Consultando API publica.cnpj.ws..."):
                try:
                    with Session(engine) as session:
                        dados = nfe_business.extrair_dados_cnpj(cnpj_busca)
                        if "erro" in dados:
                            st.error(dados["erro"])
                        else:
                            cliente = nfe_business.upsert_client(session, dados)
                            session.commit()
                            st.success(f"Cliente {cliente.nome} cadastrado/atualizado.")
                except Exception as exc:
                    st.error(f"Falha ao buscar/salvar CNPJ: {exc}")

with aba_cancelar:
    st.subheader("Cancelar NFe")
    notas_emitidas = listar_notas_emitidas(engine, limite=20)
    if not notas_emitidas:
        st.info("Nenhuma nota encontrada para cancelamento.")
    else:
        opcoes_notas = [
            f"NFe {numero} - emitida em {emitida or 'desconhecida'}" for _, numero, emitida, _ in notas_emitidas
        ]
        selecao_idx = st.selectbox(
            "Selecione a nota",
            range(len(opcoes_notas)),
            format_func=lambda idx: opcoes_notas[idx],
        )
        nota_id, numero_selecionado, emitida_selecao, xml_text = notas_emitidas[selecao_idx]
        st.write(f"Nota selecionada: {numero_selecionado} (emitida em {emitida_selecao or 'desconhecida'})")
        chave_auto, protocolo_auto = extrair_chave_protocolo(xml_text)
        if st.session_state.get("cancel_note_idx") != selecao_idx:
            st.session_state["cancel_note_idx"] = selecao_idx
            st.session_state["cancel_chave"] = chave_auto
            st.session_state["cancel_protocolo"] = protocolo_auto
        chave_cancelamento = st.text_input(
            "Chave de acesso (44 dígitos)",
            value=st.session_state.get("cancel_chave", ""),
        )
        protocolo_cancelamento = st.text_input(
            "Protocolo de autorização",
            value=st.session_state.get("cancel_protocolo", ""),
        )
        justificativa = st.text_area("Justificativa (mínimo 15 caracteres)")

        if st.button("Cancelar NFe", type="primary"):
            resultado = nfe_business.cancelar_nfe(
                chave_cancelamento=chave_cancelamento.strip(),
                protocolo_cancelamento=protocolo_cancelamento.strip(),
                justificativa=justificativa,
                homologacao=False,
            )
            if resultado.get("sucesso"):
                with Session(engine) as session:
                    nfe_row = session.get(db.NfeXml, nota_id)
                    if nfe_row:
                        nfe_row.cancelada = True
                        session.commit()
                st.success(f"NFe cancelada: {resultado.get('cStat')} - {resultado.get('xMotivo')}")
            else:
                st.error(f"Falha ao cancelar: {resultado.get('erro')}")
                if resultado.get("erro_completo"):
                    with st.expander("Detalhes"):
                        st.code(resultado["erro_completo"])
