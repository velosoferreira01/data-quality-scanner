# -*- coding: utf-8 -*-
"""
MJV Data Quality Platform
Configuração do Streamlit para uploads de até 2 GB por arquivo.

Uso:
    python aplicar_upload_2gb.py
"""

from __future__ import annotations

import shutil
from datetime import datetime
from pathlib import Path

MAX_UPLOAD_SIZE_MB = 2048
MAX_UPLOAD_SIZE_BYTES = MAX_UPLOAD_SIZE_MB * 1024 * 1024


def root_dir() -> Path:
    return Path(__file__).resolve().parent


def write_streamlit_config(root: Path) -> None:
    folder = root / ".streamlit"
    folder.mkdir(parents=True, exist_ok=True)
    config_path = folder / "config.toml"

    if config_path.exists():
        backup = folder / f"config_backup_{datetime.now():%Y%m%d_%H%M%S}.toml"
        shutil.copy2(config_path, backup)
        print(f"[OK] Backup do config.toml: {backup}")

    config_path.write_text(
        "[server]\n"
        "# Limite máximo por arquivo, em MB.\n"
        "maxUploadSize = 2048\n\n"
        "# Limite de mensagens grandes do Streamlit, em MB.\n"
        "maxMessageSize = 2048\n",
        encoding="utf-8",
    )
    print(f"[OK] Configuração Streamlit: {config_path}")


def add_constants(text: str) -> str:
    if "MAX_UPLOAD_SIZE_MB = 2048" in text:
        return text

    marker = 'ALLOWED_EXTENSIONS = {".csv", ".xlsx", ".xls", ".parquet"}'
    if marker not in text:
        raise RuntimeError("Não localizei ALLOWED_EXTENSIONS no web_app.py.")

    replacement = marker + "\n\n" + (
        "# Limite de upload da interface web. O limite efetivo também é definido\n"
        "# em .streamlit/config.toml.\n"
        "MAX_UPLOAD_SIZE_MB = 2048\n"
        "MAX_UPLOAD_SIZE_BYTES = MAX_UPLOAD_SIZE_MB * 1024 * 1024"
    )
    return text.replace(marker, replacement, 1)


def add_size_validation(text: str) -> str:
    if "Arquivos acima do limite de 2 GB" in text:
        return text

    target = """    if uploaded_files:\n        invalid_files = ["""
    replacement = """    if uploaded_files:\n        # Validação amigável adicional. O limite principal é aplicado\n        # pelo Streamlit em .streamlit/config.toml.\n        oversized_files = [\n            file\n            for file in uploaded_files\n            if getattr(file, \"size\", 0) > MAX_UPLOAD_SIZE_BYTES\n        ]\n\n        if oversized_files:\n            st.error(\n                \"Arquivos acima do limite de 2 GB: \"\n                + \", \".join(\n                    f\"{file.name} ({file.size / (1024 ** 3):.2f} GB)\"\n                    for file in oversized_files\n                )\n            )\n            st.info(\n                \"O limite configurado é de 2 GB por arquivo. \"\n                \"Para volumes maiores, utilize banco de dados ou divida o arquivo.\"\n            )\n            return\n\n        invalid_files = ["""

    if target not in text:
        raise RuntimeError("Não localizei o bloco de upload no web_app.py.")
    return text.replace(target, replacement, 1)


def update_help(text: str) -> str:
    old = (
        '"Formatos aceitos: CSV, XLSX, XLS e Parquet. "\n'
        '            "Cada execução possui uma pasta isolada."'
    )
    new = (
        '"Formatos aceitos: CSV, XLSX, XLS e Parquet. "\n'
        '            f"Limite máximo: {MAX_UPLOAD_SIZE_MB} MB (2 GB) por arquivo. "\n'
        '            "Cada execução possui uma pasta isolada."'
    )
    return text.replace(old, new, 1) if old in text else text


def update_selected_caption(text: str) -> str:
    old = """        st.caption(\n            f\"{len(uploaded_files)} arquivo(s) selecionado(s).\"\n        )"""
    new = """        total_size_bytes = sum(\n            getattr(file, \"size\", 0) for file in uploaded_files\n        )\n        total_size_mb = total_size_bytes / (1024 * 1024)\n\n        st.caption(\n            f\"{len(uploaded_files)} arquivo(s) selecionado(s) \"\n            f\"• volume total: {total_size_mb:,.2f} MB \"\n            f\"• limite: {MAX_UPLOAD_SIZE_MB} MB (2 GB) por arquivo\"\n        )"""
    return text.replace(old, new, 1) if old in text else text


def patch_web_app(root: Path) -> None:
    path = root / "web_app.py"
    if not path.exists():
        raise FileNotFoundError(f"Arquivo não encontrado: {path}")

    original = path.read_text(encoding="utf-8", errors="replace")
    updated = add_constants(original)
    updated = add_size_validation(updated)
    updated = update_help(updated)
    updated = update_selected_caption(updated)

    if updated == original:
        print("[INFO] web_app.py já está configurado.")
        return

    backup = root / f"web_app_backup_upload_{datetime.now():%Y%m%d_%H%M%S}.py"
    shutil.copy2(path, backup)
    path.write_text(updated, encoding="utf-8")
    print(f"[OK] Backup do web_app.py: {backup}")
    print(f"[OK] web_app.py atualizado: {path}")


def main() -> None:
    root = root_dir()
    print("=" * 72)
    print("MJV DATA QUALITY - UPLOAD DE ATÉ 2 GB POR ARQUIVO")
    print("=" * 72)
    print(f"[INFO] Raiz do projeto: {root}")

    write_streamlit_config(root)
    patch_web_app(root)

    print("\n[OK] Configuração concluída.")
    print("\nValide:")
    print("  python -m py_compile .\\web_app.py")
    print("\nDepois reinicie o Streamlit:")
    print("  Ctrl + C")
    print("  python -m streamlit run web_app.py")
    print("\nLimite configurado: 2048 MB (2 GB) por arquivo.")


if __name__ == "__main__":
    main()
