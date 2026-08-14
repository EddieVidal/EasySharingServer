"""
FileSend Relay Server v2.0
--------------------------
Servidor relay otimizado para plataformas de nuvem com suspensão automática (Render, Koyeb).
Utiliza FileResponse para entregas HTTP com Content-Length garantido, suporte a CORS,
e limpeza automática de arquivos de uso único.
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

# Fila de mensagens de chat não entregues (destinatário offline no momento do envio)
MESSAGE_QUEUE_HOURS = float(os.environ.get("MESSAGE_QUEUE_HOURS", "48"))
MESSAGE_QUEUE_MAX_PER_CHANNEL = int(os.environ.get("MESSAGE_QUEUE_MAX_PER_CHANNEL", "50"))

STORAGE_DIR.mkdir(parents=True, exist_ok=True)

app = FastAPI(title="FileSend Relay", version="2.0.0")

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
    Cada canal representa uma conversa 1-a-1 entre dois usuários
    (o channel_id é derivado no cliente a partir dos dois IDs).

    Se o destinatário não estiver conectado no momento do envio, a
    mensagem (já cifrada ponta-a-ponta pelo cliente — o servidor nunca
    vê o conteúdo em claro) fica guardada em memória e é entregue assim
    que ele conectar. Para saber "quem é quem" dentro do canal, cada
    cliente se identifica com uma mensagem {"type": "hello", "user_id": ...}
    logo após abrir a conexão.
    """

    def __init__(self):
        self.channels: dict[str, list[WebSocket]] = {}
        self.connection_user: dict[WebSocket, str] = {}
        self.pending: dict[str, list[dict]] = {}
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
        """Associa a conexão a um user_id e entrega qualquer mensagem que
        estava esperando por ele (tudo que não foi originalmente enviado
        por ele mesmo)."""
        async with self._ws_lock:
            self.connection_user[websocket] = user_id
            queued = self.pending.get(channel_id, [])
            to_deliver = [m for m in queued if m["sender_id"] != user_id]
            still_pending = [m for m in queued if m["sender_id"] == user_id]
            if still_pending:
                self.pending[channel_id] = still_pending
            else:
                self.pending.pop(channel_id, None)

        for msg in to_deliver:
            try:
                await websocket.send_text(msg["payload"])
            except Exception:
                pass

    async def broadcast_or_queue(self, channel_id: str, message: str, sender: WebSocket) -> None:
        """Entrega a mensagem a quem estiver conectado no canal (exceto o
        remetente). Se ninguém mais estiver conectado, guarda na fila para
        entrega quando o destinatário conectar."""
        async with self._ws_lock:
            conns = list(self.channels.get(channel_id, []))
            sender_id = self.connection_user.get(sender, "desconhecido")
            others = [c for c in conns if c is not sender]

        delivered = False
        for conn in others:
            try:
                await conn.send_text(message)
                delivered = True
            except Exception:
                pass

        if delivered:
            return

        async with self._ws_lock:
            queue = self.pending.setdefault(channel_id, [])
            queue.append({
                "sender_id": sender_id,
                "payload": message,
                "queued_at": datetime.now(timezone.utc).isoformat(),
            })
            if len(queue) > MESSAGE_QUEUE_MAX_PER_CHANNEL:
                del queue[: len(queue) - MESSAGE_QUEUE_MAX_PER_CHANNEL]

    async def purge_expired(self) -> None:
        """Remove mensagens pendentes mais antigas que MESSAGE_QUEUE_HOURS."""
        now = datetime.now(timezone.utc)
        async with self._ws_lock:
            for channel_id in list(self.pending.keys()):
                fresh = []
                for msg in self.pending[channel_id]:
                    queued_at = datetime.fromisoformat(msg["queued_at"])
                    if now - queued_at < timedelta(hours=MESSAGE_QUEUE_HOURS):
                        fresh.append(msg)
                if fresh:
                    self.pending[channel_id] = fresh
                else:
                    self.pending.pop(channel_id, None)


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
    return {"status": "ok", "time": datetime.now(timezone.utc).isoformat()}


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
