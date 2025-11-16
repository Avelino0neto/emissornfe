from __future__ import annotations

# app.py
import base64
import os
import stat
import tempfile
import unicodedata
from datetime import date, datetime, timedelta
from typing import Any

import pandas as pd
import streamlit as st
from lxml import etree
from pynfe.processamento.comunicacao import ComunicacaoSefaz
from sqlalchemy import select
from sqlalchemy.orm import Session

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


def consultar_notas(engine, inicio: date, fim: date) -> list[dict[str, Any]]:
    with Session(engine) as session:
        stmt = (
            select(db.NfeXml, db.Client)
            .join(db.Client, db.NfeXml.client_id == db.Client.id)
            .order_by(db.NfeXml.id.desc())
        )
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
                "cliente": cliente.nome if cliente else "",
                "documento": cliente.documento if cliente else "",
                "valor_total": float(nfe.valor_total or 0),
                "hash": nfe.hash,
            }
        )
    return notas


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
            select(db.NfeXml.numero).order_by(db.NfeXml.id.desc()).limit(1)
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

st.title("Emissor NFe")
st.caption("Acesso restrito - Streamlit + Neon")

st.session_state["nfe_data"] = date.today()
st.session_state.setdefault("nfe_data", date.today())

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

aba_planilha, aba_manual, aba_xml, aba_relatorio = st.tabs(
    ["Importar planilha", "Montar manualmente", "Importar XMLs", "Relatorio"]
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
    inicio_padrao = st.session_state.get("relatorio_inicio", hoje - timedelta(days=30))
    fim_padrao = st.session_state.get("relatorio_fim", hoje)
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
        st.metric("Valor total", f"R$ {total_valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))
        st.dataframe(df_rel)
        csv = df_rel.to_csv(index=False).encode("utf-8")
        st.download_button(
            "Baixar CSV",
            data=csv,
            file_name=f"relatorio_nfe_{inicio_sel}_{fim_sel}.csv",
            mime="text/csv",
        )
