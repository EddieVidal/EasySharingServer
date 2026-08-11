"""
FileSend - Cliente Desktop
--------------------------
Interface gráfica (CustomTkinter) para enviar e receber arquivos através
do servidor relay, funcionando entre computadores em redes/internets
diferentes.

Segurança: os arquivos são cifrados com AES-256-GCM ANTES de saírem do
computador de origem, e só são decifrados DEPOIS de chegarem ao destino.
O servidor relay nunca vê o conteúdo original nem a chave de cifragem —
ele só transporta bytes opacos. A chave viaja embutida no código
compartilhado (ex: DYB7RG.k3jX9f...), nunca é enviada ao servidor.

Requisitos: pip install -r requirements.txt
Antes de usar, configure a URL do servidor no arquivo config.json
(gerado automaticamente na primeira execução) ou pela própria interface.
"""

import os
import json
import base64
import tempfile
import threading
import webbrowser
from pathlib import Path

import customtkinter as ctk
from tkinter import filedialog, messagebox
import requests
from requests_toolbelt.multipart.encoder import MultipartEncoder, MultipartEncoderMonitor

import crypto_utils

# ----------------------------------------------------------------------------
# Configuração
# ----------------------------------------------------------------------------
CONFIG_PATH = Path(__file__).parent / "config.json"
DEFAULT_CONFIG = {"server_url": "http://localhost:8000"}
GENERIC_UPLOAD_NAME = "arquivo.enc"  # nome enviado ao servidor - esconde o nome real


def load_config() -> dict:
    if CONFIG_PATH.exists():
        try:
            return {**DEFAULT_CONFIG, **json.loads(CONFIG_PATH.read_text(encoding="utf-8"))}
        except (json.JSONDecodeError, OSError):
            pass
    return dict(DEFAULT_CONFIG)


def save_config(config: dict) -> None:
    CONFIG_PATH.write_text(json.dumps(config, indent=2), encoding="utf-8")


def format_size(num_bytes: int) -> str:
    size = float(num_bytes)
    for unit in ["B", "KB", "MB", "GB"]:
        if size < 1024:
            return f"{size:.1f} {unit}"
        size /= 1024
    return f"{size:.1f} TB"


def key_to_text(key: bytes) -> str:
    return base64.urlsafe_b64encode(key).decode("ascii").rstrip("=")


def text_to_key(text: str) -> bytes:
    padding = "=" * (-len(text) % 4)
    return base64.urlsafe_b64decode(text + padding)


def build_share_string(code: str, key: bytes) -> str:
    return f"{code}.{key_to_text(key)}"


def parse_share_string(share: str) -> tuple[str, bytes]:
    share = share.strip()
    if "." not in share:
        raise ValueError("Formato inválido. Cole o código completo, incluindo a chave (ex: ABC123.xxxxx).")
    code, key_text = share.split(".", 1)
    return code.strip().upper(), text_to_key(key_text.strip())


# ----------------------------------------------------------------------------
# App
# ----------------------------------------------------------------------------
ctk.set_appearance_mode("dark")
ctk.set_default_color_theme("blue")


class FileSendApp(ctk.CTk):
    def __init__(self):
        super().__init__()

        self.config_data = load_config()
        self.selected_file_path: str | None = None
        self.fetched_file_info: dict | None = None
        self.fetched_code: str | None = None
        self.fetched_key: bytes | None = None

        self.title("FileSend - Compartilhamento cifrado entre redes")
        self.geometry("580x560")
        self.minsize(520, 520)

        self._build_layout()

    # ------------------------------------------------------------------
    # Layout
    # ------------------------------------------------------------------
    def _build_layout(self):
        header = ctk.CTkFrame(self, fg_color="transparent")
        header.pack(fill="x", padx=20, pady=(20, 10))

        ctk.CTkLabel(
            header, text="📁 FileSend", font=ctk.CTkFont(size=22, weight="bold")
        ).pack(side="left")

        self.server_status_label = ctk.CTkLabel(
            header, text="●  verificando...", text_color="gray", font=ctk.CTkFont(size=12)
        )
        self.server_status_label.pack(side="right")

        ctk.CTkLabel(
            self, text="🔒 Arquivos são cifrados (AES-256) antes de sair do seu computador",
            text_color="gray", font=ctk.CTkFont(size=11)
        ).pack(padx=20, anchor="w")

        self.tabview = ctk.CTkTabview(self, width=540, height=400)
        self.tabview.pack(fill="both", expand=True, padx=20, pady=10)
        self.tab_send = self.tabview.add("Enviar")
        self.tab_receive = self.tabview.add("Receber")
        self.tab_settings = self.tabview.add("Configurações")

        self._build_send_tab()
        self._build_receive_tab()
        self._build_settings_tab()

        threading.Thread(target=self._check_server_status, daemon=True).start()

    # ------------------------------------------------------------------
    # Aba Enviar
    # ------------------------------------------------------------------
    def _build_send_tab(self):
        tab = self.tab_send

        self.file_label = ctk.CTkLabel(
            tab, text="Nenhum arquivo selecionado", wraplength=460, justify="left"
        )
        self.file_label.pack(pady=(20, 10), padx=10, fill="x")

        ctk.CTkButton(
            tab, text="Selecionar arquivo", command=self._pick_file
        ).pack(pady=5)

        self.send_progress = ctk.CTkProgressBar(tab, width=440)
        self.send_progress.set(0)
        self.send_progress.pack(pady=15)

        self.send_status_label = ctk.CTkLabel(tab, text="")
        self.send_status_label.pack()

        self.send_button = ctk.CTkButton(
            tab, text="Cifrar e enviar", command=self._start_upload, state="disabled"
        )
        self.send_button.pack(pady=15)

        result_frame = ctk.CTkFrame(tab, fg_color="transparent")
        result_frame.pack(pady=10, fill="x", padx=10)

        ctk.CTkLabel(
            result_frame, text="Código + chave (envie isso para quem vai receber):",
            font=ctk.CTkFont(size=12), text_color="gray"
        ).pack()

        self.code_display = ctk.CTkTextbox(result_frame, height=30, width=460, wrap="none")
        self.code_display.configure(state="disabled")
        self.code_display.pack(pady=5)

        self.copy_button = ctk.CTkButton(
            result_frame, text="Copiar código completo", command=self._copy_code, width=180
        )
        # só aparece depois do upload

    def _pick_file(self):
        path = filedialog.askopenfilename(title="Selecione um arquivo")
        if not path:
            return
        self.selected_file_path = path
        size = os.path.getsize(path)
        name = os.path.basename(path)
        self.file_label.configure(text=f"{name}  ({format_size(size)})")
        self.send_button.configure(state="normal")
        self._set_code_display("")
        self.copy_button.pack_forget()
        self.send_progress.set(0)
        self.send_status_label.configure(text="")

    def _set_code_display(self, text: str):
        self.code_display.configure(state="normal")
        self.code_display.delete("1.0", "end")
        self.code_display.insert("1.0", text)
        self.code_display.configure(state="disabled")

    def _start_upload(self):
        if not self.selected_file_path:
            return
        self.send_button.configure(state="disabled")
        threading.Thread(target=self._upload_file, daemon=True).start()

    def _upload_file(self):
        path = self.selected_file_path
        filename = os.path.basename(path)
        server_url = self.config_data["server_url"].rstrip("/")

        key = crypto_utils.generate_key()
        tmp_encrypted = tempfile.NamedTemporaryFile(delete=False, suffix=".enc")
        tmp_encrypted.close()
        enc_path = tmp_encrypted.name

        try:
            # Fase 1: cifrar localmente
            self.after(0, self.send_status_label.configure, {"text": "Cifrando arquivo..."})

            def encrypt_progress(fraction):
                self.after(0, self.send_progress.set, fraction)
                self.after(0, self.send_status_label.configure,
                           {"text": f"Cifrando... {int(fraction * 100)}%"})

            crypto_utils.encrypt_file(path, enc_path, key, filename, progress_cb=encrypt_progress)

            # Fase 2: enviar arquivo cifrado
            self.after(0, self.send_progress.set, 0)
            self.after(0, self.send_status_label.configure, {"text": "Enviando..."})

            def upload_progress(monitor: MultipartEncoderMonitor):
                fraction = monitor.bytes_read / monitor.len if monitor.len else 0
                self.after(0, self.send_progress.set, fraction)
                self.after(0, self.send_status_label.configure,
                           {"text": f"Enviando... {int(fraction * 100)}%"})

            with open(enc_path, "rb") as f:
                encoder = MultipartEncoder(
                    fields={"file": (GENERIC_UPLOAD_NAME, f, "application/octet-stream")}
                )
                monitor = MultipartEncoderMonitor(encoder, upload_progress)
                response = requests.post(
                    f"{server_url}/upload",
                    data=monitor,
                    headers={"Content-Type": monitor.content_type},
                    timeout=None,
                )
            response.raise_for_status()
            data = response.json()
            share_string = build_share_string(data["code"], key)
            self.after(0, self._on_upload_success, share_string)

        except requests.exceptions.RequestException as exc:
            self.after(0, self._on_upload_error, str(exc))
        except Exception as exc:
            self.after(0, self._on_upload_error, f"Erro ao cifrar/enviar: {exc}")
        finally:
            try:
                os.remove(enc_path)
            except OSError:
                pass

    def _on_upload_success(self, share_string: str):
        self.send_progress.set(1.0)
        self.send_status_label.configure(text="Envio concluído — arquivo cifrado no servidor.")
        self._set_code_display(share_string)
        self.copy_button.pack(pady=5)
        self.send_button.configure(state="normal")
        self._last_share_string = share_string

    def _on_upload_error(self, error_msg: str):
        self.send_status_label.configure(text="Falha no envio")
        self.send_button.configure(state="normal")
        messagebox.showerror("Erro no upload", f"Não foi possível enviar o arquivo:\n{error_msg}")

    def _copy_code(self):
        if hasattr(self, "_last_share_string"):
            self.clipboard_clear()
            self.clipboard_append(self._last_share_string)
            self.copy_button.configure(text="Copiado!")
            self.after(1500, lambda: self.copy_button.configure(text="Copiar código completo"))

    # ------------------------------------------------------------------
    # Aba Receber
    # ------------------------------------------------------------------
    def _build_receive_tab(self):
        tab = self.tab_receive

        ctk.CTkLabel(tab, text="Cole o código completo recebido (código + chave):").pack(pady=(20, 5))

        self.code_entry = ctk.CTkEntry(
            tab, placeholder_text="Ex: A1B2C3.k3jX9f...", width=440
        )
        self.code_entry.pack(pady=5)

        ctk.CTkButton(tab, text="Buscar arquivo", command=self._fetch_file_info).pack(pady=10)

        self.receive_info_label = ctk.CTkLabel(tab, text="", wraplength=460, justify="left")
        self.receive_info_label.pack(pady=10, padx=10, fill="x")

        self.receive_progress = ctk.CTkProgressBar(tab, width=440)
        self.receive_progress.set(0)
        self.receive_progress.pack(pady=10)

        self.receive_status_label = ctk.CTkLabel(tab, text="")
        self.receive_status_label.pack()

        self.download_button = ctk.CTkButton(
            tab, text="Baixar e decifrar", command=self._start_download, state="disabled"
        )
        self.download_button.pack(pady=15)

    def _fetch_file_info(self):
        try:
            code, key = parse_share_string(self.code_entry.get())
        except ValueError as exc:
            self.receive_info_label.configure(text=str(exc))
            return

        self.fetched_code = code
        self.fetched_key = key
        server_url = self.config_data["server_url"].rstrip("/")
        self.receive_info_label.configure(text="Buscando...")
        self.download_button.configure(state="disabled")

        def worker():
            try:
                response = requests.get(f"{server_url}/files/{code}", timeout=10)
                if response.status_code == 404:
                    self.after(0, self.receive_info_label.configure,
                               {"text": "Código não encontrado ou expirado."})
                    return
                response.raise_for_status()
                info = response.json()
                self.fetched_file_info = info
                text = (
                    f"Arquivo cifrado encontrado (tamanho no servidor: {format_size(info['size'])}).\n"
                    "O nome real e o conteúdo só aparecem depois de decifrar com a chave.\n"
                    "⚠️ Este código funciona uma única vez — após o download, o arquivo é apagado do servidor."
                )
                self.after(0, self.receive_info_label.configure, {"text": text})
                self.after(0, self.download_button.configure, {"state": "normal"})
            except requests.exceptions.RequestException as exc:
                self.after(0, self.receive_info_label.configure,
                           {"text": f"Erro ao buscar: {exc}"})

        threading.Thread(target=worker, daemon=True).start()

    def _start_download(self):
        if not self.fetched_file_info:
            return
        self.download_button.configure(state="disabled")
        threading.Thread(target=self._download_and_decrypt, daemon=True).start()

    def _download_and_decrypt(self):
        server_url = self.config_data["server_url"].rstrip("/")
        code = self.fetched_code
        key = self.fetched_key
        total_size = self.fetched_file_info["size"] or 1

        tmp_encrypted = tempfile.NamedTemporaryFile(delete=False, suffix=".enc")
        tmp_encrypted.close()
        enc_path = tmp_encrypted.name
        tmp_decrypted_path = enc_path + ".dec"

        try:
            # Fase 1: baixar o arquivo cifrado
            self.after(0, self.receive_status_label.configure, {"text": "Baixando..."})
            with requests.get(f"{server_url}/download/{code}", stream=True, timeout=None) as response:
                response.raise_for_status()
                downloaded = 0
                with open(enc_path, "wb") as f:
                    for chunk in response.iter_content(chunk_size=1024 * 256):
                        if chunk:
                            f.write(chunk)
                            downloaded += len(chunk)
                            fraction = downloaded / total_size
                            self.after(0, self.receive_progress.set, fraction)
                            self.after(0, self.receive_status_label.configure,
                                       {"text": f"Baixando... {int(fraction * 100)}%"})

            # Fase 2: decifrar localmente
            self.after(0, self.receive_progress.set, 0)
            self.after(0, self.receive_status_label.configure, {"text": "Decifrando..."})

            def decrypt_progress(fraction):
                self.after(0, self.receive_progress.set, fraction)
                self.after(0, self.receive_status_label.configure,
                           {"text": f"Decifrando... {int(fraction * 100)}%"})

            header = crypto_utils.decrypt_file(enc_path, tmp_decrypted_path, key, progress_cb=decrypt_progress)
            original_filename = header.get("filename", "arquivo_recebido")

            self.after(0, self._ask_save_location, tmp_decrypted_path, original_filename)

        except ValueError as exc:
            self.after(0, self._on_download_error,
                       f"Falha ao decifrar: {exc}\n"
                       "(chave incorreta ou arquivo corrompido — atenção: o arquivo já foi "
                       "removido do servidor pois o download foi concluído; peça para o remetente "
                       "enviar novamente com um novo código)")
        except requests.exceptions.RequestException as exc:
            self.after(0, self._on_download_error, str(exc))
        finally:
            try:
                os.remove(enc_path)
            except OSError:
                pass

    def _ask_save_location(self, tmp_decrypted_path: str, original_filename: str):
        save_path = filedialog.asksaveasfilename(initialfile=original_filename)
        self.download_button.configure(state="normal")
        if not save_path:
            try:
                os.remove(tmp_decrypted_path)
            except OSError:
                pass
            self.receive_status_label.configure(text="Cancelado.")
            return
        try:
            os.replace(tmp_decrypted_path, save_path)
        except OSError as exc:
            messagebox.showerror("Erro", f"Não foi possível salvar o arquivo:\n{exc}")
            return
        self.receive_progress.set(1.0)
        self.receive_status_label.configure(text="Concluído — arquivo decifrado com sucesso.")
        messagebox.showinfo("Concluído", f"Arquivo salvo em:\n{save_path}")

    def _on_download_error(self, error_msg: str):
        self.receive_status_label.configure(text="Falha no download")
        self.download_button.configure(state="normal")
        messagebox.showerror("Erro", error_msg)

    # ------------------------------------------------------------------
    # Aba Configurações
    # ------------------------------------------------------------------
    def _build_settings_tab(self):
        tab = self.tab_settings

        ctk.CTkLabel(tab, text="URL do servidor relay:").pack(pady=(20, 5))

        self.server_url_entry = ctk.CTkEntry(tab, width=380)
        self.server_url_entry.insert(0, self.config_data["server_url"])
        self.server_url_entry.pack(pady=5)

        ctk.CTkButton(tab, text="Salvar", command=self._save_server_url).pack(pady=10)

        ctk.CTkLabel(
            tab,
            text=(
                "🔒 Como funciona a criptografia:\n"
                "O arquivo é cifrado (AES-256-GCM) no seu computador ANTES de subir.\n"
                "A chave nunca é enviada ao servidor — ela vai embutida no código\n"
                "que você compartilha. Sem essa chave, ninguém (nem o servidor)\n"
                "consegue ler o conteúdo do arquivo."
            ),
            text_color="gray",
            justify="left",
        ).pack(pady=20, padx=10)

        ctk.CTkButton(
            tab, text="Abrir documentação da Koyeb",
            command=lambda: webbrowser.open("https://www.koyeb.com/docs")
        ).pack(pady=5)

    def _save_server_url(self):
        new_url = self.server_url_entry.get().strip()
        if not new_url:
            return
        self.config_data["server_url"] = new_url
        save_config(self.config_data)
        messagebox.showinfo("Salvo", "URL do servidor atualizada.")
        threading.Thread(target=self._check_server_status, daemon=True).start()

    # ------------------------------------------------------------------
    # Status do servidor
    # ------------------------------------------------------------------
    def _check_server_status(self):
        server_url = self.config_data["server_url"].rstrip("/")
        try:
            response = requests.get(f"{server_url}/health", timeout=5)
            if response.status_code == 200:
                self.after(0, self.server_status_label.configure,
                           {"text": "●  servidor online", "text_color": "#2ecc71"})
                return
        except requests.exceptions.RequestException:
            pass
        self.after(0, self.server_status_label.configure,
                   {"text": "●  servidor offline", "text_color": "#e74c3c"})


if __name__ == "__main__":
    app = FileSendApp()
    app.mainloop()
