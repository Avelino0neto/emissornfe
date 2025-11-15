import requests
from sqlalchemy import select
from sqlalchemy.orm import Session

import db


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
