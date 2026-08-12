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
import shutil
import string
import secrets
import threading
from pathlib import Path
from datetime import datetime, timedelta, timezone

from fastapi import FastAPI, UploadFile, File, HTTPException
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
