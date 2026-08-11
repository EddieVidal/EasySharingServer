"""
crypto_utils.py
----------------
Cifragem/decifragem de arquivos em streaming (por chunks), usando
AES-256-GCM da biblioteca `cryptography`. Feito para que o servidor relay
nunca tenha acesso ao conteúdo original nem à chave — tudo acontece no
cliente, antes do upload e depois do download.

Formato do arquivo cifrado (.enc):

  MAGIC (6 bytes) b"FSENC1"
  header_nonce (12 bytes)
  header_len (4 bytes, big-endian)
  header_ciphertext (JSON cifrado: {"filename": ..., "size": ...})
  --- repete até o fim do arquivo ---
  chunk_nonce (12 bytes)
  chunk_len (4 bytes, big-endian)
  chunk_ciphertext

Cada chunk usa um nonce único (contador de 8 bytes + 4 bytes aleatórios),
respeitando o requisito do AES-GCM de nunca reutilizar nonce com a mesma
chave.
"""

import os
import json
import struct
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

MAGIC = b"FSENC1"
CHUNK_SIZE = 1024 * 1024  # 1MB de dado original por chunk (antes de cifrar)


def generate_key() -> bytes:
    """Gera uma chave aleatória de 256 bits."""
    return AESGCM.generate_key(bit_length=256)


def _nonce(chunk_index: int) -> bytes:
    # 8 bytes de contador (garante unicidade) + 4 bytes aleatórios
    return chunk_index.to_bytes(8, "big") + os.urandom(4)


def encrypt_file(input_path: str, output_path: str, key: bytes, filename: str, progress_cb=None) -> None:
    """Cifra input_path e escreve o resultado em output_path."""
    aesgcm = AESGCM(key)
    total_size = os.path.getsize(input_path)

    header = json.dumps({"filename": filename, "size": total_size}).encode("utf-8")
    header_nonce = _nonce(0)
    encrypted_header = aesgcm.encrypt(header_nonce, header, None)

    with open(output_path, "wb") as out:
        out.write(MAGIC)
        out.write(header_nonce)
        out.write(struct.pack(">I", len(encrypted_header)))
        out.write(encrypted_header)

        written = 0
        chunk_index = 1
        with open(input_path, "rb") as f:
            while True:
                chunk = f.read(CHUNK_SIZE)
                if not chunk:
                    break
                nonce = _nonce(chunk_index)
                ciphertext = aesgcm.encrypt(nonce, chunk, None)
                out.write(nonce)
                out.write(struct.pack(">I", len(ciphertext)))
                out.write(ciphertext)

                written += len(chunk)
                chunk_index += 1
                if progress_cb and total_size:
                    progress_cb(written / total_size)


def decrypt_file(input_path: str, output_path: str, key: bytes, progress_cb=None) -> dict:
    """Decifra input_path (formato .enc) e escreve o resultado em output_path.
    Retorna o header original ({"filename": ..., "size": ...}).
    Lança ValueError se a chave estiver errada ou o arquivo estiver corrompido.
    """
    aesgcm = AESGCM(key)

    with open(input_path, "rb") as f:
        magic = f.read(len(MAGIC))
        if magic != MAGIC:
            raise ValueError("Arquivo não reconhecido (assinatura inválida)")

        header_nonce = f.read(12)
        header_len = struct.unpack(">I", f.read(4))[0]
        encrypted_header = f.read(header_len)
        try:
            header = json.loads(aesgcm.decrypt(header_nonce, encrypted_header, None))
        except Exception as exc:
            raise ValueError("Chave incorreta ou arquivo corrompido") from exc

        total_size = header.get("size", 0)
        written = 0

        with open(output_path, "wb") as out:
            while True:
                nonce = f.read(12)
                if len(nonce) < 12:
                    break
                length_bytes = f.read(4)
                if len(length_bytes) < 4:
                    break
                length = struct.unpack(">I", length_bytes)[0]
                ciphertext = f.read(length)
                try:
                    plaintext = aesgcm.decrypt(nonce, ciphertext, None)
                except Exception as exc:
                    raise ValueError("Chave incorreta ou arquivo corrompido") from exc
                out.write(plaintext)
                written += len(plaintext)
                if progress_cb and total_size:
                    progress_cb(written / total_size)

        return header
