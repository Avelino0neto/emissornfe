from __future__ import annotations

import base64
from datetime import datetime
from typing import Any, Iterable

import msal
import requests
from cryptography.fernet import Fernet, InvalidToken
from sqlalchemy import select
from sqlalchemy.orm import Session

import db

PROVIDER = "microsoft_graph"
DEFAULT_AUTHORITY = "https://login.microsoftonline.com/common"
DEFAULT_SCOPES = ["User.Read", "Mail.Send"]


def normalize_document(value: str | None) -> str:
    return "".join(ch for ch in str(value or "") if ch.isdigit())


def enabled_for_client(documento: str | None, config: dict[str, Any]) -> bool:
    if not config.get("enabled", False):
        return False
    allowed = [normalize_document(item) for item in config.get("only_client_documents", [])]
    allowed = [item for item in allowed if item]
    return normalize_document(documento) in allowed


def clean_recipients(recipients: Iterable[str] | None) -> list[str]:
    return [str(item).strip() for item in (recipients or []) if str(item).strip()]


def _fernet(encryption_key: str) -> Fernet:
    if not encryption_key:
        raise ValueError("graph.cache_encryption_key nao configurado.")
    return Fernet(encryption_key.encode("ascii"))


def encrypt_cache(serialized_cache: str, encryption_key: str) -> str:
    encrypted = _fernet(encryption_key).encrypt(serialized_cache.encode("utf-8"))
    return encrypted.decode("ascii")


def decrypt_cache(cache_blob: str, encryption_key: str) -> str:
    try:
        decrypted = _fernet(encryption_key).decrypt(cache_blob.encode("ascii"))
    except InvalidToken as exc:
        raise ValueError("Cache MSAL nao pode ser descriptografado com a chave configurada.") from exc
    return decrypted.decode("utf-8")


def load_token_cache(session: Session, encryption_key: str) -> msal.SerializableTokenCache:
    cache = msal.SerializableTokenCache()
    row = session.scalars(
        select(db.OAuthTokenCache).where(db.OAuthTokenCache.provider == PROVIDER)
    ).first()
    if not row:
        return cache
    serialized = decrypt_cache(row.cache_blob, encryption_key)
    cache.deserialize(serialized)
    return cache


def save_serialized_token_cache(session: Session, serialized_cache: str, encryption_key: str) -> None:
    cache_blob = encrypt_cache(serialized_cache, encryption_key)
    row = session.scalars(
        select(db.OAuthTokenCache).where(db.OAuthTokenCache.provider == PROVIDER)
    ).first()
    if row:
        row.cache_blob = cache_blob
        row.updated_at = datetime.utcnow()
    else:
        row = db.OAuthTokenCache(
            provider=PROVIDER,
            cache_blob=cache_blob,
            updated_at=datetime.utcnow(),
        )
        session.add(row)
    session.flush()


def save_token_cache(session: Session, cache: msal.SerializableTokenCache, encryption_key: str) -> None:
    if cache.has_state_changed:
        save_serialized_token_cache(session, cache.serialize(), encryption_key)


def get_access_token(session: Session, graph_config: dict[str, Any]) -> dict[str, Any]:
    client_id = graph_config.get("client_id")
    if not client_id:
        return {"sucesso": False, "erro": "graph.client_id nao configurado."}

    encryption_key = graph_config.get("cache_encryption_key", "")
    scopes = list(graph_config.get("scopes") or DEFAULT_SCOPES)
    authority = graph_config.get("authority") or DEFAULT_AUTHORITY

    try:
        cache = load_token_cache(session, encryption_key)
    except Exception as exc:
        return {"sucesso": False, "erro": str(exc)}

    app = msal.PublicClientApplication(
        client_id=client_id,
        authority=authority,
        token_cache=cache,
    )
    accounts = app.get_accounts()
    if not accounts:
        return {"sucesso": False, "erro": "Cache Microsoft Graph nao encontrado. Reimporte o token_cache.bin."}

    result = app.acquire_token_silent(scopes, account=accounts[0])
    try:
        save_token_cache(session, cache, encryption_key)
    except Exception as exc:
        return {"sucesso": False, "erro": f"Falha ao salvar cache atualizado: {exc}"}

    if not result or "access_token" not in result:
        erro = result.get("error_description") if isinstance(result, dict) else None
        return {
            "sucesso": False,
            "erro": erro or "Token Microsoft Graph expirado ou invalido. Reimporte o token_cache.bin.",
        }

    return {"sucesso": True, "access_token": result["access_token"]}


def send_xml_email(
    session: Session,
    *,
    graph_config: dict[str, Any],
    recipients: Iterable[str],
    subject: str,
    body: str,
    xml_bytes: bytes,
    xml_filename: str,
) -> dict[str, Any]:
    recipients_clean = clean_recipients(recipients)
    if not recipients_clean:
        return {"sucesso": False, "erro": "Nenhum destinatario de e-mail configurado."}

    token_result = get_access_token(session, graph_config)
    if not token_result.get("sucesso"):
        return token_result

    attachment = {
        "@odata.type": "#microsoft.graph.fileAttachment",
        "name": xml_filename,
        "contentType": "application/xml",
        "contentBytes": base64.b64encode(xml_bytes).decode("ascii"),
    }
    payload = {
        "message": {
            "subject": subject,
            "body": {"contentType": "Text", "content": body},
            "toRecipients": [
                {"emailAddress": {"address": email}} for email in recipients_clean
            ],
            "attachments": [attachment],
        },
        "saveToSentItems": True,
    }

    response = requests.post(
        "https://graph.microsoft.com/v1.0/me/sendMail",
        headers={
            "Authorization": f"Bearer {token_result['access_token']}",
            "Content-Type": "application/json",
        },
        json=payload,
        timeout=30,
    )
    if response.status_code == 202:
        return {"sucesso": True, "status_code": response.status_code}
    return {
        "sucesso": False,
        "status_code": response.status_code,
        "erro": response.text or f"HTTP {response.status_code}",
    }
