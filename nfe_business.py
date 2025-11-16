from __future__ import annotations

import hashlib
import re
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any

import requests
import streamlit as st
from lxml import etree
from sqlalchemy import select
from sqlalchemy.orm import Session

import db
from pynfe.entidades import Emitente, Cliente, NotaFiscal, _fonte_dados
from pynfe.entidades.evento import EventoCancelarNota
from pynfe.processamento.serializacao import SerializacaoXML
from pynfe.processamento.assinatura import AssinaturaA1

CODIGO_BRASIL = "1058"
NFE_NS = {"nfe": "http://www.portalfiscal.inf.br/nfe"}
XML_PARSER = etree.XMLParser(remove_blank_text=True, recover=True)


def _text(node, path, default="") -> str:
    if node is None:
        return default
    try:
        elem = node.find(path, NFE_NS)
    except etree.XPathEvalError:
        return default
    if elem is None or elem.text is None:
        return default
    return elem.text.strip()


def _safe_decimal(value: str | None) -> Decimal | None:
    if not value:
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def extrair_dados_cnpj(cnpj: str) -> dict:
    """
    Consulta a API publica.cnpj.ws e retorna os principais dados do CNPJ.
    """
    response = requests.get(
        f"https://publica.cnpj.ws/cnpj/{cnpj}",
        headers={"Accept": "*/*"},
        timeout=30,
    )

    if response.status_code != 200:
        return {"erro": f"Erro na consulta: {response.status_code}"}

    data = response.json()
    est = data["estabelecimento"]
    tel = est["telefone1"] or est["telefone2"]

    return {
        "cnpj": est["cnpj"],
        "razao_social": data["razao_social"],
        "nome_fantasia": est["nome_fantasia"],
        "logradouro": f"{est['tipo_logradouro']} {est['logradouro']}".strip(),
        "numero": est["numero"],
        "bairro": est["bairro"],
        "inscricao_estadual": (
            est["inscricoes_estaduais"][0]["inscricao_estadual"]
            if est["inscricoes_estaduais"]
            else None
        ),
        "ibge_id": est["cidade"]["ibge_id"],
        "cidade": est["cidade"]["nome"],
        "uf": est["estado"]["sigla"],
        "cep": est["cep"],
        "telefone": tel,
    }


def upsert_client(session: Session, dados: dict) -> db.Client:
    """
    Insere ou atualiza um cliente com base no documento (CNPJ/CPF).
    """
    documento = dados.get("documento") or dados.get("cnpj")
    if not documento:
        raise ValueError("Documento do cliente nao informado.")

    payload = {
        "documento": documento,
        "nome": dados.get("nome") or dados.get("razao_social"),
        "nome_fantasia": dados.get("nome_fantasia"),
        "logradouro": dados.get("logradouro"),
        "numero": dados.get("numero"),
        "bairro": dados.get("bairro"),
        "inscricao_estadual": dados.get("inscricao_estadual"),
        "cidade": dados.get("cidade"),
        "uf": dados.get("uf"),
        "cep": dados.get("cep"),
        "endereco_complemento": dados.get("endereco_complemento"),
        "endereco_pais": dados.get("endereco_pais"),
        "ibge_id": dados.get("ibge_id"),
        "telefone": dados.get("telefone"),
        "email": dados.get("email"),
    }

    stmt = select(db.Client).where(db.Client.documento == documento).with_for_update()
    client = session.scalars(stmt).first()

    if client:
        for field, value in payload.items():
            setattr(client, field, value)
    else:
        client = db.Client(**payload)
        session.add(client)

    session.flush()
    return client


def importar_cliente_por_cnpj(session: Session, cnpj: str) -> dict:
    """
    Consulta o CNPJ na API externa e grava/atualiza o cliente.
    """
    dados = extrair_dados_cnpj(cnpj)
    if "erro" in dados:
        return dados

    client = upsert_client(session, dados)
    return {"status": "ok", "client_id": client.id}


def parse_nfe_xml(xml_bytes: bytes) -> dict[str, Any]:
    """
    Extrai dados principais (destinatário, produtos, totais) de um XML de NFe.
    """
    root = etree.fromstring(xml_bytes, parser=XML_PARSER)
    ide = root.find(".//nfe:ide", NFE_NS)
    inf_nfe = root.find(".//nfe:infNFe", NFE_NS)
    cancelada = False
    if root.tag.endswith("procEventoNFe"):
        desc = root.findtext(".//nfe:detEvento/nfe:descEvento", namespaces=NFE_NS)
        if desc and "Cancelamento" in desc:
            cancelada = True

    numero = _text(ide, "nfe:nNF")
    serie = _text(ide, "nfe:serie")
    data_emissao = _text(ide, "nfe:dhEmi") or _text(ide, "nfe:dEmi")
    valor_total = _text(root, ".//nfe:ICMSTot/nfe:vNF")
    chave = ""
    if inf_nfe is not None:
        chave = (inf_nfe.get("Id") or "").replace("NFe", "")

    dest = root.find(".//nfe:dest", NFE_NS)
    end_dest = dest.find("nfe:enderDest", NFE_NS) if dest is not None else None
    destinatario = {
        "documento": _text(dest, "nfe:CNPJ") or _text(dest, "nfe:CPF"),
        "nome": _text(dest, "nfe:xNome"),
        "nome_fantasia": _text(dest, "nfe:xFant") or None,
        "logradouro": _text(end_dest, "nfe:xLgr"),
        "numero": _text(end_dest, "nfe:nro"),
        "bairro": _text(end_dest, "nfe:xBairro"),
        "inscricao_estadual": _text(dest, "nfe:IE"),
        "cidade": _text(end_dest, "nfe:xMun"),
        "uf": _text(end_dest, "nfe:UF"),
        "cep": _text(end_dest, "nfe:CEP"),
        "endereco_complemento": _text(end_dest, "nfe:xCpl"),
        "endereco_pais": _text(end_dest, "nfe:xPais"),
        "ibge_id": _text(end_dest, "nfe:cMun"),
        "telefone": _text(dest, "nfe:fone"),
        "email": _text(dest, "nfe:email"),
    }

    produtos: list[dict[str, Any]] = []
    for det in root.findall(".//nfe:det", NFE_NS):
        prod = det.find("nfe:prod", NFE_NS)
        imposto_icms = det.find(".//nfe:ICMS", NFE_NS)
        cst_icms = "40"
        if imposto_icms is not None:
            for child in list(imposto_icms):
                cst_tmp = _text(child, "nfe:CST") or _text(child, "nfe:CSOSN")
                if cst_tmp:
                    cst_icms = cst_tmp
                    break

        produtos.append(
            {
                "codigo": _text(prod, "nfe:cProd"),
                "nome": _text(prod, "nfe:xProd"),
                "ncm": _text(prod, "nfe:NCM"),
                "cfop": _text(prod, "nfe:CFOP"),
                "unidade": _text(prod, "nfe:uCom") or "UN",
                "quantidade": _text(prod, "nfe:qCom"),
                "valor_unitario": _text(prod, "nfe:vUnCom"),
                "valor_total": _text(prod, "nfe:vProd"),
                "cst_icms": cst_icms or "40",
            }
        )

    return {
        "numero": numero,
        "serie": serie,
        "data_emissao": data_emissao,
        "valor_total": valor_total,
        "chave": chave,
        "destinatario": destinatario,
        "produtos": produtos,
        "cancelada": cancelada,
    }


def get_emitente_data() -> dict:
    """
    Lê os dados do emitente definidos nos secrets ([emitente]).
    """
    try:
        emitente = dict(st.secrets["emitente"])
    except Exception as exc:
        raise RuntimeError('Dados do emitente nao encontrados em st.secrets["emitente"].') from exc

    required = ["cnpj", "razao_social", "logradouro", "numero", "bairro", "cidade", "uf", "cep"]
    for campo in required:
        if not emitente.get(campo):
            raise RuntimeError(f"Campo obrigatorio do emitente ausente: {campo}")
    return emitente


def limpar_documento(valor: str | None) -> str:
    return re.sub(r"\D", "", valor or "")


def criar_emitente_pynfe():
    """Cria objeto Emitente usando dados de secrets."""
    empresa = get_emitente_data()

    return Emitente(
        razao_social=empresa["razao_social"],
        nome_fantasia=empresa.get("nome_fantasia") or empresa["razao_social"],
        cnpj=limpar_documento(empresa["cnpj"]),
        codigo_de_regime_tributario="3",
        inscricao_estadual=empresa.get("inscricao_estadual") or "",
        endereco_logradouro=empresa["logradouro"],
        endereco_numero=empresa["numero"],
        endereco_bairro=empresa["bairro"],
        endereco_municipio=empresa["cidade"],
        endereco_uf=empresa["uf"],
        endereco_cep=empresa["cep"],
        endereco_pais=CODIGO_BRASIL,
    )


def criar_cliente_pynfe(session: Session, cliente_id: int | None = None):
    """Cria objeto Cliente usando dados do ORM."""
    cliente_id = cliente_id or st.session_state.get("cliente_id")
    if not cliente_id:
        raise ValueError("Nenhum cliente selecionado.")

    cliente = session.get(db.Client, cliente_id)
    if not cliente:
        raise ValueError("Cliente nao encontrado no banco.")

    documento = limpar_documento(cliente.documento)
    tipo_documento = "CPF" if len(documento) == 11 else "CNPJ"
    indicador_ie = "1" if cliente.inscricao_estadual else "9"

    return Cliente(
        razao_social=cliente.nome,
        tipo_documento=tipo_documento,
        email=cliente.email or "",
        numero_documento=documento,
        indicador_ie=indicador_ie,
        inscricao_estadual=cliente.inscricao_estadual or "",
        endereco_logradouro=cliente.logradouro or "",
        endereco_numero=cliente.numero or "",
        endereco_complemento=cliente.endereco_complemento or "",
        endereco_bairro=cliente.bairro or "",
        endereco_municipio=cliente.cidade or "",
        endereco_uf=cliente.uf or "",
        endereco_cep=cliente.cep or "",
        endereco_pais=CODIGO_BRASIL,
        endereco_telefone=cliente.telefone or "",
    )


def criar_notafiscal_pynfe(
    session: Session,
    nfe_data,
    nfe_numero,
    nfe_serie,
    nfe_natureza,
    nfe_tipo,
    nfe_finalidade,
    nfe_consumidor,
    nfe_presenca,
    forma_pagamento,
    cliente_id: int | None = None,
):
    """Cria objeto NotaFiscal usando dados do formulário."""
    emitente = criar_emitente_pynfe()
    cliente = criar_cliente_pynfe(session, cliente_id)
    empresa = get_emitente_data()
    municipio_ibge = empresa.get("ibge_id", "3502804")

    tipo_documento_map = {"Saída": 1, "Entrada": 0}
    finalidade_map = {"Normal": "1", "Complementar": "2", "Ajuste": "3", "Devolução": "4"}
    cliente_final_map = {"Sim": 1, "Não": 0}
    presenca_map = {"Presencial": 1, "Internet": 2, "Teleatendimento": 3, "Não se aplica": 9}
    forma_pagamento_map = {
        "Dinheiro": 0,
        "Cartão de Crédito": 0,
        "Cartão de Débito": 0,
        "PIX": 0,
        "Boleto": 1,
        "Transferência": 0,
    }

    total_tributos = Decimal("0.0")
    for produto in st.session_state.get("produtos", []):
        total_tributos += Decimal(str(produto["valor_total"])) * Decimal("0.15")

    nfe_data_datetime = datetime.combine(nfe_data, datetime.min.time()) if nfe_data else datetime.now()

    return NotaFiscal(
        emitente=emitente,
        cliente=cliente,
        uf=empresa["uf"].upper(),
        natureza_operacao=nfe_natureza or "Venda de mercadorias",
        forma_pagamento=forma_pagamento_map.get(forma_pagamento or "Dinheiro", 0),
        tipo_pagamento=1,
        modelo=55,
        serie=str(nfe_serie) if nfe_serie else "1",
        numero_nf=str(nfe_numero) if nfe_numero else "1",
        data_emissao=nfe_data_datetime,
        data_saida_entrada=nfe_data_datetime,
        tipo_documento=tipo_documento_map.get(nfe_tipo or "Saída", 1),
        municipio=municipio_ibge,
        tipo_impressao_danfe=1,
        forma_emissao="1",
        cliente_final=cliente_final_map.get((nfe_consumidor or "Não").strip(), 0),
        indicador_destino=1,
        indicador_presencial=presenca_map.get(nfe_presenca or "Presencial", 1),
        finalidade_emissao=finalidade_map.get(nfe_finalidade or "Normal", "1"),
        processo_emissao="0",
        transporte_modalidade_frete=9,
        informacoes_adicionais_interesse_fisco="NFe emitida pelo Sistema PyNFe",
        totais_tributos_aproximado=total_tributos,
    )


def importar_xml_document(session: Session, xml_bytes: bytes, filename: str | None = None) -> dict[str, Any]:
    """
    Salva um XML de NFe na tabela nfe_xmls e importa cliente/produtos.
    """
    xml_hash = hashlib.sha256(xml_bytes).hexdigest()
    existing = session.scalars(select(db.NfeXml).where(db.NfeXml.hash == xml_hash)).first()
    if existing:
        nome_cliente = None
        if existing.client_id:
            cliente = session.get(db.Client, existing.client_id)
            nome_cliente = cliente.nome if cliente else None
        return {
            "status": "duplicated",
            "hash": xml_hash,
            "numero": existing.numero,
            "cliente": nome_cliente,
            "arquivo": filename,
        }

    parsed = parse_nfe_xml(xml_bytes)
    if not parsed["destinatario"].get("documento"):
        raise ValueError("Documento do destinatario nao encontrado no XML.")

    cliente = upsert_client(session, parsed["destinatario"])
    store_id = cliente.documento or f"cliente_{cliente.id}"

    produtos_status: list[dict[str, Any]] = []
    for produto in parsed["produtos"]:
        resultado = db.import_row(
            session,
            store_id=store_id,
            name=produto["nome"] or "Produto sem nome",
            code=produto["codigo"] or "",
            ncm=produto["ncm"] or None,
            unit=produto["unidade"] or None,
            cst_icms=produto.get("cst_icms"),
            min_fuzzy_score=90,
        )
        produtos_status.append(
            {
                "codigo": produto.get("codigo"),
                "nome": produto.get("nome"),
                "status": resultado.get("status"),
            }
        )

    valor_total = _safe_decimal(parsed["valor_total"])
    xml_text = xml_bytes.decode("utf-8", errors="ignore")
    nfe_row = db.NfeXml(
        client_id=cliente.id,
        numero=parsed["numero"],
        valor_total=valor_total,
        emitida_em=parsed["data_emissao"],
        xml_text=xml_text,
        hash=xml_hash,
        cancelada=parsed.get("cancelada", False),
    )
    session.add(nfe_row)
    session.flush()

    return {
        "status": "ok",
        "hash": xml_hash,
        "numero": parsed["numero"],
        "cliente": cliente.nome,
        "produtos_status": produtos_status,
        "nfe_id": nfe_row.id,
        "arquivo": filename,
    }


def adicionar_produtos_pynfe(nota_fiscal):
    """Adiciona produtos à NotaFiscal usando dados do st.session_state.produtos."""
    if not st.session_state.get("produtos"):
        raise ValueError("Nenhum produto foi adicionado. Adicione pelo menos um produto/serviço.")

    for i, produto in enumerate(st.session_state.produtos):

        campos_obrigatorios = {
            "codigo": "Código do produto",
            "nome": "Nome/Descrição",
            "ncm": "NCM",
            "cfop": "CFOP",
            "unidade": "Unidade",
            "quantidade": "Quantidade",
            "valor_unitario": "Valor unitário",
            "valor_total": "Valor total",
            "cst_pis": "CST PIS",
            "cst_cofins": "CST COFINS",
            "cst_icms": "CST ICMS",
        }

        for campo, nome_campo in campos_obrigatorios.items():
            if not produto.get(campo) or str(produto.get(campo)).strip() == "":
                raise ValueError(f"Produto {i+1}: Campo '{nome_campo}' não foi preenchido.")

        nota_fiscal.adicionar_produto_servico(
            codigo=produto["codigo"],
            descricao=produto["nome"],
            ncm=produto["ncm"],
            cfop=produto["cfop"],
            unidade_comercial=produto["unidade"],
            quantidade_comercial=Decimal(str(produto["quantidade"])),
            valor_unitario_comercial=Decimal(str(produto["valor_unitario"])),
            valor_total_bruto=Decimal(str(produto["valor_total"])),
            unidade_tributavel=produto["unidade"],
            quantidade_tributavel=Decimal(str(produto["quantidade"])),
            valor_unitario_tributavel=Decimal(str(produto["valor_unitario"])),
            ind_total=1,
            icms_modalidade=produto.get("cst_icms"),
            icms_origem=0,
            icms_csosn=None,
            pis_modalidade=produto["cst_pis"],
            cofins_modalidade=produto["cst_cofins"],
            valor_tributos_aprox=str(
                round(Decimal(str(produto["valor_total"])) * Decimal("0.15"), 2)
            ),
        )

    return nota_fiscal


def criar_nfe_pynfe(
    session: Session,
    nfe_data,
    nfe_numero,
    nfe_serie,
    nfe_natureza,
    nfe_tipo,
    nfe_finalidade,
    nfe_consumidor,
    nfe_presenca,
    forma_pagamento,
    homologacao,
    cliente_id: int | None = None,
):
    """Cria a NFe completa."""
    import traceback

    try:
        try:
            nota_fiscal = criar_notafiscal_pynfe(
                session,
                nfe_data,
                nfe_numero,
                nfe_serie,
                nfe_natureza,
                nfe_tipo,
                nfe_finalidade,
                nfe_consumidor,
                nfe_presenca,
                forma_pagamento,
                cliente_id=cliente_id,
            )
        except Exception as e:
            error_details = traceback.format_exc()
            return {
                "sucesso": False,
                "erro": f"Erro ao criar NotaFiscal: {type(e).__name__}: {str(e)}",
                "erro_completo": f"ERRO NO PASSO 1 - CRIAR NOTAFISCAL:\n{error_details}",
                "nota_fiscal": None,
            }

        try:
            nota_fiscal = adicionar_produtos_pynfe(nota_fiscal)
        except Exception as e:
            error_details = traceback.format_exc()
            return {
                "sucesso": False,
                "erro": f"Erro ao adicionar produtos: {type(e).__name__}: {str(e)}",
                "erro_completo": f"ERRO NO PASSO 2 - ADICIONAR PRODUTOS:\n{error_details}",
                "nota_fiscal": None,
            }

        try:
            serializador = SerializacaoXML(_fonte_dados, homologacao=homologacao)
            nfe_xml = serializador.exportar()
        except Exception as e:
            error_details = traceback.format_exc()
            return {
                "sucesso": False,
                "erro": f"Erro na serialização XML: {type(e).__name__}: {str(e)}",
                "erro_completo": f"ERRO NO PASSO 3 - SERIALIZAÇÃO:\nHomologação={homologacao}\n{error_details}",
                "nota_fiscal": None,
            }

        if not st.session_state.get("certificado_path") or not st.session_state.get("senha_certificado"):
            return {
                "sucesso": False,
                "erro": "Certificado não configurado",
                "erro_completo": (
                    "ERRO NO PASSO 4 - VERIFICAÇÃO CERTIFICADO:\n"
                    f"Certificado path: {st.session_state.get('certificado_path')}\n"
                    f"Senha configurada: {bool(st.session_state.get('senha_certificado'))}"
                ),
                "nota_fiscal": None,
            }

        try:
            a1 = AssinaturaA1(st.session_state.certificado_path, st.session_state.senha_certificado)
            xml_assinado = a1.assinar(nfe_xml)
        except Exception as e:
            error_details = traceback.format_exc()
            return {
                "sucesso": False,
                "erro": f"Erro na assinatura digital: {type(e).__name__}: {str(e)}",
                "erro_completo": (
                    "ERRO NO PASSO 5 - ASSINATURA:\n"
                    f"Certificado: {st.session_state.certificado_path}\n{error_details}"
                ),
                "nota_fiscal": None,
            }
        ##CODEIA NÃO ALTERAR essas linhas
        ##Visualização do XML apagar depois dos teste
        xml_n_ass = etree.tostring(xml_assinado, encoding="unicode")
        with open(f"{nota_fiscal.numero_nf} - Nota.xml", "w", encoding="utf-8") as file:
            file.write(xml_n_ass)
        ##fim da visualização

        if not st.session_state.comunicacao:
            return {
                "sucesso": False,
                "erro": "Conexão com SEFAZ não configurada",
                "erro_completo": "ERRO NO PASSO 6 - VERIFICAÇÃO COMUNICAÇÃO:\nComunicação SEFAZ não está configurada",
                "nota_fiscal": None,
            }

        try:
            resultado = st.session_state.comunicacao.autorizacao(modelo="nfe", nota_fiscal=xml_assinado)
            sucesso = bool(resultado and resultado[0] == 0)
            payload = {
                "sucesso": sucesso,
                "resultado": resultado,
                "nota_fiscal": nota_fiscal,
                "xml_assinado": xml_assinado,
                "resultado_codigo": resultado[0] if resultado else "N/A",
                "resultado_detalhes": f"Código retorno: {resultado[0] if resultado else 'N/A'}",
            }
            if not sucesso:
                mensagem = None
                if resultado and len(resultado) > 1:
                    resposta = resultado[1]
                    if hasattr(resposta, "text"):
                        mensagem = resposta.text
                payload["erro"] = mensagem or payload["resultado_detalhes"]
            return payload
        except Exception as e:
            error_details = traceback.format_exc()
            return {
                "sucesso": False,
                "erro": f"Erro no envio para SEFAZ: {type(e).__name__}: {str(e)}",
                "erro_completo": (
                    "ERRO NO PASSO 7 - ENVIO SEFAZ:\n"
                    f"Tipo comunicação: {type(st.session_state.comunicacao)}\n{error_details}"
                ),
                "nota_fiscal": None,
            }

    except Exception as e:
        import traceback as tb

        error_details = tb.format_exc()
        return {
            "sucesso": False,
            "erro": f"Erro geral não tratado: {type(e).__name__}: {str(e)}",
            "erro_completo": f"ERRO GERAL NÃO TRATADO:\n{error_details}",
            "nota_fiscal": None,
        }


def cancelar_nfe(chave_cancelamento, protocolo_cancelamento, justificativa, homologacao):
    """Cancela uma NFe usando a estrutura PyNFe."""
    import traceback

    try:
        if not chave_cancelamento or len(chave_cancelamento) != 44:
            return {"sucesso": False, "erro": "Chave de acesso deve ter exatamente 44 dígitos", "cStat": None, "xMotivo": None}

        if not protocolo_cancelamento:
            return {"sucesso": False, "erro": "Protocolo de autorização é obrigatório", "cStat": None, "xMotivo": None}

        if not justificativa or len(justificativa.strip()) < 15:
            return {"sucesso": False, "erro": "Justificativa deve ter pelo menos 15 caracteres", "cStat": None, "xMotivo": None}

        if not st.session_state.get("certificado_path") or not st.session_state.get("senha_certificado"):
            return {"sucesso": False, "erro": "Certificado não configurado", "cStat": None, "xMotivo": None}

        if not st.session_state.comunicacao:
            return {"sucesso": False, "erro": "Conexão com SEFAZ não configurada", "cStat": None, "xMotivo": None}

        empresa = get_emitente_data()
        uf = empresa["uf"].upper()

        try:
            cancelar = EventoCancelarNota(
                cnpj=limpar_documento(empresa["cnpj"]),
                chave=chave_cancelamento,
                data_emissao=datetime.now(),
                uf=uf,
                protocolo=protocolo_cancelamento,
                justificativa=justificativa.strip(),
            )

        except Exception as e:
            error_details = traceback.format_exc()
            return {
                "sucesso": False,
                "erro": f"Erro ao criar evento de cancelamento: {type(e).__name__}: {str(e)}",
                "erro_completo": f"ERRO NO PASSO 1 - CRIAR EVENTO:\n{error_details}",
                "cStat": None,
                "xMotivo": None,
            }

        try:
            serializador = SerializacaoXML(_fonte_dados, homologacao=homologacao)
            nfe_cancel = serializador.serializar_evento(cancelar)

        except Exception as e:
            error_details = traceback.format_exc()
            return {
                "sucesso": False,
                "erro": f"Erro na serialização do evento: {type(e).__name__}: {str(e)}",
                "erro_completo": f"ERRO NO PASSO 2 - SERIALIZAÇÃO:\n{error_details}",
                "cStat": None,
                "xMotivo": None,
            }

        try:
            a1 = AssinaturaA1(st.session_state.certificado_path, st.session_state.senha_certificado)
            xml_assinado = a1.assinar(nfe_cancel)

        except Exception as e:
            error_details = traceback.format_exc()
            return {
                "sucesso": False,
                "erro": f"Erro na assinatura digital: {type(e).__name__}: {str(e)}",
                "erro_completo": f"ERRO NO PASSO 3 - ASSINATURA:\n{error_details}",
                "cStat": None,
                "xMotivo": None,
            }

        try:
            envio = st.session_state.comunicacao.evento(modelo="nfe", evento=xml_assinado)

            if hasattr(envio, "text"):
                response_text = envio.text
                import re as _re

                codigos = _re.findall(r"<cStat>(\d+)</cStat>", response_text)
                motivos = _re.findall(r"<xMotivo>(.*?)</xMotivo>", response_text)

                if codigos and motivos:
                    cStat = codigos[-1]
                    xMotivo = motivos[-1]
                    sucesso = cStat in ["135", "136"]

                    return {
                        "sucesso": sucesso,
                        "cStat": cStat,
                        "xMotivo": xMotivo,
                        "response_text": response_text,
                        "erro": None if sucesso else f"SEFAZ rejeitou o cancelamento: {cStat} - {xMotivo}",
                    }
                else:
                    return {
                        "sucesso": False,
                        "erro": "Não foi possível processar a resposta da SEFAZ",
                        "cStat": None,
                        "xMotivo": None,
                        "response_text": response_text,
                    }
            else:
                return {"sucesso": False, "erro": "Resposta inválida da SEFAZ", "cStat": None, "xMotivo": None}

        except Exception as e:
            error_details = traceback.format_exc()
            return {
                "sucesso": False,
                "erro": f"Erro no envio para SEFAZ: {type(e).__name__}: {str(e)}",
                "erro_completo": f"ERRO NO PASSO 4 - ENVIO SEFAZ:\n{error_details}",
                "cStat": None,
                "xMotivo": None,
            }

    except Exception as e:
        import traceback as tb

        error_details = tb.format_exc()
        return {
            "sucesso": False,
            "erro": f"Erro geral não tratado: {type(e).__name__}: {str(e)}",
            "erro_completo": f"ERRO GERAL NÃO TRATADO:\n{error_details}",
            "cStat": None,
            "xMotivo": None,
        }
