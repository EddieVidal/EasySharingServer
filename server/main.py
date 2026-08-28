"""
FileSend Relay Server v2.1
--------------------------
Servidor relay otimizado para plataformas de nuvem com suspensão automática (Render, Koyeb).
Utiliza FileResponse para entregas HTTP com Content-Length garantido, suporte a CORS,
e limpeza automática de arquivos de uso único.

A fila de mensagens de chat pendentes (destinatário offline no envio) é persistida no
Supabase (Postgres gerenciado), porque o disco local desses provedores é efêmero — some
a cada redeploy, reinício ou "spin down" por inatividade do plano free.
"""

import os
import json
import time
import base64
import shutil
import string
import secrets
import asyncio
import threading
from pathlib import Path
from datetime import datetime, timedelta, timezone

import httpx
from fastapi import FastAPI, UploadFile, File, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
from starlette.background import BackgroundTask
from pydantic import BaseModel

STORAGE_DIR = Path(os.environ.get("STORAGE_DIR", "./storage"))
METADATA_FILE = STORAGE_DIR / "_metadata.json"
MAX_FILE_SIZE_MB = int(os.environ.get("MAX_FILE_SIZE_MB", "200"))
MAX_FILE_SIZE_BYTES = MAX_FILE_SIZE_MB * 1024 * 1024
EXPIRATION_HOURS = float(os.environ.get("EXPIRATION_HOURS", "6"))
DELETE_AFTER_DOWNLOAD = os.environ.get("DELETE_AFTER_DOWNLOAD", "true").lower() == "true"
CHUNK_SIZE = 1024 * 1024
CODE_LENGTH = 6
CODE_ALPHABET = "".join(c for c in (string.ascii_uppercase + string.digits) if c not in "0O1IL")

# ------------------------------------------------------------------
# Fila de mensagens de chat pendentes — persistida no Supabase
# ------------------------------------------------------------------
MESSAGE_QUEUE_HOURS = float(os.environ.get("MESSAGE_QUEUE_HOURS", "48"))
MESSAGE_QUEUE_MAX_PER_CHANNEL = int(os.environ.get("MESSAGE_QUEUE_MAX_PER_CHANNEL", "50"))

SUPABASE_URL = os.environ.get("SUPABASE_URL", "").rstrip("/")
SUPABASE_SERVICE_KEY = os.environ.get("SUPABASE_SERVICE_KEY", "")
SUPABASE_TABLE = "pending_messages"
SUPABASE_ENABLED = bool(SUPABASE_URL and SUPABASE_SERVICE_KEY)

if not SUPABASE_ENABLED:
    print(
        "[AVISO] SUPABASE_URL / SUPABASE_SERVICE_KEY não configurados: a fila de mensagens "
        "pendentes vai funcionar só em memória e será perdida a cada reinício/sono do servidor."
    )

_http_client: httpx.AsyncClient | None = None


async def _get_http_client() -> httpx.AsyncClient:
    global _http_client
    if _http_client is None:
        _http_client = httpx.AsyncClient(timeout=15.0)
    return _http_client


def _supabase_headers(extra: dict | None = None) -> dict:
    headers = {
        "apikey": SUPABASE_SERVICE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
        "Content-Type": "application/json",
    }
    if extra:
        headers.update(extra)
    return headers


STORAGE_DIR.mkdir(parents=True, exist_ok=True)

app = FastAPI(title="FileSend Relay", version="2.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

_lock = threading.Lock()


def _load_metadata() -> dict:
    if METADATA_FILE.exists():
        try:
            return json.loads(METADATA_FILE.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            return {}
    return {}


def _save_metadata(data: dict) -> None:
    METADATA_FILE.write_text(json.dumps(data, indent=2), encoding="utf-8")
# ==================================================================
# GERENCIAMENTO DE GRUPOS (INSERIR AQUI)
# ==================================================================
GROUPS_FILE = STORAGE_DIR / "_groups.json"

def _load_groups() -> dict:
    if GROUPS_FILE.exists():
        try:
            return json.loads(GROUPS_FILE.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            return {}
    return {}

def _save_groups(data: dict) -> None:
    GROUPS_FILE.write_text(json.dumps(data, indent=2), encoding="utf-8")

# Schema para criação de grupo
class CreateGroupRequest(BaseModel):
    group_name: str
    members: list[str]  # Ex: ["USER-AAAA", "USER-BBBB"]

# Endpoints HTTP para Grupos
@app.post("/groups")
def create_group(req: CreateGroupRequest):
    if not req.group_name.strip() or len(req.members) < 2:
        raise HTTPException(status_code=400, detail="Nome do grupo inválido ou menos de 2 membros.")
    
    with _lock:
        groups = _load_groups()
        group_id = "GROUP-" + secrets.token_hex(6).upper()
        # Gera uma chave simétrica aleatória de 32 bytes para o grupo (em Base64)
        group_key = base64.b64encode(os.urandom(32)).decode("ascii")
        
        normalized_members = list(set([m.strip().upper() for m in req.members]))
        
        groups[group_id] = {
            "group_id": group_id,
            "name": req.group_name.strip(),
            "key": group_key,
            "members": normalized_members,
            "created_at": datetime.now(timezone.utc).isoformat()
        }
        _save_groups(groups)
    return groups[group_id]

@app.get("/groups/{user_id}")
def get_user_groups(user_id: str):
    user_id = user_id.strip().upper()
    groups = _load_groups()
    user_groups = [
        {
            "group_id": g_id, 
            "name": g_info["name"], 
            "key": g_info["key"], 
            "members": g_info["members"]
        }
        for g_id, g_info in groups.items()
        if user_id in g_info["members"]
    ]
    return user_groups
# ==================================================================

def _generate_code(existing: dict) -> str:
    while True:
        code = "".join(secrets.choice(CODE_ALPHABET) for _ in range(CODE_LENGTH))
        if code not in existing:
            return code


def _cleanup_loop():
    while True:
        try:
            with _lock:
                metadata = _load_metadata()
                now = datetime.now(timezone.utc)
                expired_codes = []
                for code, info in metadata.items():
                    expires_at = datetime.fromisoformat(info["expires_at"])
                    if expires_at.tzinfo is None:
                        expires_at = expires_at.replace(tzinfo=timezone.utc)
                    if expires_at < now:
                        expired_codes.append(code)
                for code in expired_codes:
                    _delete_file(code, metadata, save=False)
                if expired_codes:
                    _save_metadata(metadata)
        except Exception as exc:
            print(f"[cleanup] erro: {exc}")
        time.sleep(300)


def _delete_file(code: str, metadata: dict, save: bool = True) -> None:
    folder = STORAGE_DIR / code
    if folder.exists():
        shutil.rmtree(folder, ignore_errors=True)
    metadata.pop(code, None)
    if save:
        _save_metadata(metadata)


threading.Thread(target=_cleanup_loop, daemon=True).start()


# ------------------------------------------------------------------
# Relay de Chat em Tempo Real (WebSocket) com fila de entrega pendente
# ------------------------------------------------------------------
class ChatConnectionManager:
    """
    Mantém as conexões WebSocket agrupadas por canal (channel_id).
    Cada canal representa uma conversa 1-a-1 ou um grupo (o channel_id é
    derivado no cliente a partir dos IDs envolvidos, ou é o próprio GROUP-ID).

    Se um destinatário não estiver conectado no momento do envio, a
    mensagem (já cifrada ponta-a-ponta pelo cliente — o servidor nunca
    vê o conteúdo em claro) fica guardada até que ele conecte. Cada
    cliente se identifica com uma mensagem {"type": "hello", "user_id": ...}
    logo após abrir a conexão.

    Cada mensagem pendente guarda "delivered_to": a lista de user_ids que já
    a receberam. Isso garante que, em grupos com 3+ membros, a mensagem só é
    removida da fila por expiração (MESSAGE_QUEUE_HOURS) ou limite de
    tamanho — nunca porque só um dos membros offline já reconectou — assim
    todo mundo que estava offline recebe a mensagem quando voltar.

    A fila é persistida na tabela `pending_messages` do Supabase (Postgres
    real, não efêmero), pois o disco local do Render/Koyeb é apagado a cada
    reinício, redeploy ou "spin down" por inatividade do plano free — testar
    isso foi o que mostrou que guardar a fila só em arquivo local não é
    confiável nessas plataformas.

    Se SUPABASE_URL/SUPABASE_SERVICE_KEY não estiverem configurados, cai de
    volta para uma fila em memória (não sobrevive a reinícios, mas o
    servidor continua funcionando normalmente enquanto o processo estiver de pé).
    """

    def __init__(self):
        self.channels: dict[str, list[WebSocket]] = {}
        self.connection_user: dict[WebSocket, str] = {}
        # Usado só quando o Supabase não está configurado (modo de fallback).
        self._memory_pending: dict[str, list[dict]] = {}
        self._ws_lock = asyncio.Lock()

    async def connect(self, channel_id: str, websocket: WebSocket) -> None:
        await websocket.accept()
        async with self._ws_lock:
            self.channels.setdefault(channel_id, []).append(websocket)

    async def disconnect(self, channel_id: str, websocket: WebSocket) -> None:
        async with self._ws_lock:
            conns = self.channels.get(channel_id)
            if conns and websocket in conns:
                conns.remove(websocket)
            if conns is not None and not conns:
                self.channels.pop(channel_id, None)
            self.connection_user.pop(websocket, None)

    async def register_user(self, channel_id: str, websocket: WebSocket, user_id: str) -> None:
        """Associa a conexão a um user_id e entrega as mensagens pendentes
        que ainda não foram enviadas a esse usuário especificamente
        (nunca reenvia a própria mensagem de volta ao remetente, e nunca
        reenvia duas vezes para quem já recebeu — importante em grupos)."""
        async with self._ws_lock:
            self.connection_user[websocket] = user_id

        if SUPABASE_ENABLED:
            to_deliver = await self._supabase_fetch_and_mark_delivered(channel_id, user_id)
        else:
            to_deliver = self._memory_fetch_and_mark_delivered(channel_id, user_id)

        for payload in to_deliver:
            try:
                await websocket.send_text(payload)
            except Exception:
                pass

    async def broadcast_or_queue(self, channel_id: str, message: str, sender: WebSocket) -> None:
        """Entrega a mensagem a quem estiver conectado no canal (exceto o
        remetente), marcando cada um como já recebido. Se sobrar alguém do
        canal que não estava conectado (ou a entrega falhar), a mensagem
        também é guardada na fila para ser entregue quando essa pessoa
        conectar — sem duplicar para quem já recebeu ao vivo."""
        async with self._ws_lock:
            conns = list(self.channels.get(channel_id, []))
            sender_id = self.connection_user.get(sender, "desconhecido")
            others = [(c, self.connection_user.get(c)) for c in conns if c is not sender]

        delivered_to_live = []
        for conn, uid in others:
            try:
                await conn.send_text(message)
                if uid:
                    delivered_to_live.append(uid)
            except Exception:
                pass

        # Só considera "totalmente entregue" (sem precisar de fila) quando
        # havia pelo menos um outro participante conectado no canal e a
        # entrega chegou em todos eles.
        if others and len(delivered_to_live) == len(others):
            return

        if SUPABASE_ENABLED:
            await self._supabase_enqueue(channel_id, sender_id, message, delivered_to_live)
        else:
            self._memory_enqueue(channel_id, sender_id, message, delivered_to_live)

    async def purge_expired(self) -> None:
        """Remove mensagens pendentes mais antigas que MESSAGE_QUEUE_HOURS."""
        if SUPABASE_ENABLED:
            await self._supabase_purge_expired()
        else:
            self._memory_purge_expired()

    # ----------------------------------------------------------------
    # Backend: Supabase (persistente)
    # ----------------------------------------------------------------
    async def _supabase_fetch_and_mark_delivered(self, channel_id: str, user_id: str) -> list[str]:
        client = await _get_http_client()
        to_deliver: list[str] = []
        try:
            resp = await client.get(
                f"{SUPABASE_URL}/rest/v1/{SUPABASE_TABLE}",
                headers=_supabase_headers(),
                params={
                    "channel_id": f"eq.{channel_id}",
                    "sender_id": f"neq.{user_id}",
                    "order": "queued_at.asc",
                    "select": "id,payload,delivered_to",
                },
            )
            resp.raise_for_status()
            rows = resp.json()
        except Exception as exc:
            print(f"[supabase] erro ao buscar mensagens pendentes: {exc}")
            return to_deliver

        for row in rows:
            delivered_to = row.get("delivered_to") or []
            if user_id in delivered_to:
                continue
            to_deliver.append(row["payload"])
            try:
                await client.patch(
                    f"{SUPABASE_URL}/rest/v1/{SUPABASE_TABLE}",
                    headers=_supabase_headers({"Prefer": "return=minimal"}),
                    params={"id": f"eq.{row['id']}"},
                    json={"delivered_to": delivered_to + [user_id]},
                )
            except Exception as exc:
                print(f"[supabase] erro ao marcar mensagem {row.get('id')} como entregue: {exc}")

        return to_deliver

    async def _supabase_enqueue(self, channel_id: str, sender_id: str, message: str, delivered_to_live: list[str]) -> None:
        client = await _get_http_client()
        try:
            await client.post(
                f"{SUPABASE_URL}/rest/v1/{SUPABASE_TABLE}",
                headers=_supabase_headers({"Prefer": "return=minimal"}),
                json={
                    "channel_id": channel_id,
                    "sender_id": sender_id,
                    "payload": message,
                    "delivered_to": delivered_to_live,
                },
            )
        except Exception as exc:
            print(f"[supabase] erro ao enfileirar mensagem: {exc}")
            return

        await self._supabase_enforce_channel_limit(client, channel_id)

    async def _supabase_enforce_channel_limit(self, client: httpx.AsyncClient, channel_id: str) -> None:
        try:
            resp = await client.get(
                f"{SUPABASE_URL}/rest/v1/{SUPABASE_TABLE}",
                headers=_supabase_headers(),
                params={
                    "channel_id": f"eq.{channel_id}",
                    "order": "queued_at.asc",
                    "select": "id",
                },
            )
            resp.raise_for_status()
            rows = resp.json()
            excedente = len(rows) - MESSAGE_QUEUE_MAX_PER_CHANNEL
            if excedente > 0:
                ids_antigos = ",".join(str(r["id"]) for r in rows[:excedente])
                await client.delete(
                    f"{SUPABASE_URL}/rest/v1/{SUPABASE_TABLE}",
                    headers=_supabase_headers(),
                    params={"id": f"in.({ids_antigos})"},
                )
        except Exception as exc:
            print(f"[supabase] erro ao aplicar limite de fila do canal {channel_id}: {exc}")

    async def _supabase_purge_expired(self) -> None:
        cutoff = datetime.now(timezone.utc) - timedelta(hours=MESSAGE_QUEUE_HOURS)
        client = await _get_http_client()
        try:
            await client.delete(
                f"{SUPABASE_URL}/rest/v1/{SUPABASE_TABLE}",
                headers=_supabase_headers(),
                params={"queued_at": f"lt.{cutoff.isoformat()}"},
            )
        except Exception as exc:
            print(f"[supabase] erro ao expurgar mensagens antigas: {exc}")

    # ----------------------------------------------------------------
    # Backend: memória (fallback quando o Supabase não está configurado)
    # ----------------------------------------------------------------
    def _memory_fetch_and_mark_delivered(self, channel_id: str, user_id: str) -> list[str]:
        queued = self._memory_pending.get(channel_id, [])
        to_deliver = []
        for msg in queued:
            if msg.get("sender_id") == user_id:
                continue
            delivered_to = msg.setdefault("delivered_to", [])
            if user_id in delivered_to:
                continue
            delivered_to.append(user_id)
            to_deliver.append(msg["payload"])
        return to_deliver

    def _memory_enqueue(self, channel_id: str, sender_id: str, message: str, delivered_to_live: list[str]) -> None:
        queue = self._memory_pending.setdefault(channel_id, [])
        queue.append({
            "sender_id": sender_id,
            "payload": message,
            "queued_at": datetime.now(timezone.utc).isoformat(),
            "delivered_to": delivered_to_live,
        })
        if len(queue) > MESSAGE_QUEUE_MAX_PER_CHANNEL:
            del queue[: len(queue) - MESSAGE_QUEUE_MAX_PER_CHANNEL]

    def _memory_purge_expired(self) -> None:
        now = datetime.now(timezone.utc)
        for channel_id in list(self._memory_pending.keys()):
            fresh = []
            for msg in self._memory_pending[channel_id]:
                queued_at = datetime.fromisoformat(msg["queued_at"])
                if queued_at.tzinfo is None:
                    queued_at = queued_at.replace(tzinfo=timezone.utc)
                if now - queued_at < timedelta(hours=MESSAGE_QUEUE_HOURS):
                    fresh.append(msg)
            if fresh:
                self._memory_pending[channel_id] = fresh
            else:
                self._memory_pending.pop(channel_id, None)


chat_manager = ChatConnectionManager()


@app.on_event("startup")
async def _start_chat_queue_cleanup():
    async def loop():
        while True:
            await asyncio.sleep(300)
            try:
                await chat_manager.purge_expired()
            except Exception as exc:
                print(f"[chat cleanup] erro: {exc}")

    asyncio.create_task(loop())


@app.on_event("shutdown")
async def _close_http_client():
    global _http_client
    if _http_client is not None:
        await _http_client.aclose()
        _http_client = None


@app.websocket("/ws/{channel_id}")
async def chat_websocket(websocket: WebSocket, channel_id: str):
    await chat_manager.connect(channel_id, websocket)
    try:
        while True:
            data = await websocket.receive_text()

            try:
                parsed = json.loads(data)
            except json.JSONDecodeError:
                parsed = None

            if isinstance(parsed, dict) and parsed.get("type") == "hello":
                user_id = str(parsed.get("user_id", "")).strip().upper()
                if user_id:
                    await chat_manager.register_user(channel_id, websocket, user_id)
                continue

            await chat_manager.broadcast_or_queue(channel_id, data, sender=websocket)
    except WebSocketDisconnect:
        pass
    except Exception as exc:
        print(f"[ws:{channel_id}] erro: {exc}")
    finally:
        await chat_manager.disconnect(channel_id, websocket)


class UploadResponse(BaseModel):
    code: str
    filename: str
    size: int
    expires_at: str


class FileInfoResponse(BaseModel):
    filename: str
    size: int
    uploaded_at: str
    expires_at: str


@app.get("/health")
def health():
    return {
        "status": "ok",
        "time": datetime.now(timezone.utc).isoformat(),
        "message_queue": "supabase" if SUPABASE_ENABLED else "memory",
    }


@app.post("/upload", response_model=UploadResponse)
async def upload_file(file: UploadFile = File(...)):
    with _lock:
        metadata = _load_metadata()
        code = _generate_code(metadata)

    folder = STORAGE_DIR / code
    folder.mkdir(parents=True, exist_ok=True)
    dest_path = folder / file.filename

    size = 0
    try:
        with open(dest_path, "wb") as out_file:
            while True:
                chunk = await file.read(CHUNK_SIZE)
                if not chunk:
                    break
                size += len(chunk)
                if size > MAX_FILE_SIZE_BYTES:
                    out_file.close()
                    shutil.rmtree(folder, ignore_errors=True)
                    raise HTTPException(
                        status_code=413,
                        detail=f"Arquivo excede o limite de {MAX_FILE_SIZE_MB}MB",
                    )
                out_file.write(chunk)
    except HTTPException:
        raise
    except Exception as exc:
        shutil.rmtree(folder, ignore_errors=True)
        raise HTTPException(status_code=500, detail=f"Falha no upload: {exc}")

    now = datetime.now(timezone.utc)
    expires_at = now + timedelta(hours=EXPIRATION_HOURS)

    with _lock:
        metadata = _load_metadata()
        metadata[code] = {
            "filename": file.filename,
            "size": size,
            "uploaded_at": now.isoformat(),
            "expires_at": expires_at.isoformat(),
        }
        _save_metadata(metadata)

    return UploadResponse(
        code=code, filename=file.filename, size=size, expires_at=expires_at.isoformat()
    )


@app.get("/files/{code}", response_model=FileInfoResponse)
def file_info(code: str):
    code = code.upper().strip()
    metadata = _load_metadata()
    info = metadata.get(code)
    if not info:
        raise HTTPException(status_code=404, detail="Código não encontrado ou expirado")
    return FileInfoResponse(**info)


@app.get("/download/{code}")
def download_file(code: str):
    code = code.upper().strip()
    metadata = _load_metadata()
    info = metadata.get(code)
    if not info:
        raise HTTPException(status_code=404, detail="Código não encontrado ou expirado")

    file_path = STORAGE_DIR / code / info["filename"]
    if not file_path.exists():
        raise HTTPException(status_code=404, detail="Arquivo não encontrado no servidor")

    def cleanup_after_download():
        with _lock:
            current_metadata = _load_metadata()
            if code in current_metadata:
                _delete_file(code, current_metadata)

    background = BackgroundTask(cleanup_after_download) if DELETE_AFTER_DOWNLOAD else None

    return FileResponse(
        path=file_path,
        filename=info["filename"],
        media_type="application/octet-stream",
        background=background
    )


@app.delete("/files/{code}")
def delete_file(code: str):
    code = code.upper().strip()
    with _lock:
        metadata = _load_metadata()
        if code not in metadata:
            raise HTTPException(status_code=404, detail="Código não encontrado")
        _delete_file(code, metadata)
    return {"status": "deleted", "code": code}


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", "8000"))
    uvicorn.run(app, host="0.0.0.0", port=port)
