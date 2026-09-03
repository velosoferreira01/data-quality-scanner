# -*- coding: utf-8 -*-
"""
Aplica correção segura de Unicode no pipeline e no Streamlit.

Execute a partir da raiz do projeto:
    python aplicar_correcao_unicode.py
"""
from pathlib import Path
import py_compile
import shutil
from datetime import datetime

PROJECT_ROOT = Path.cwd().resolve()
PIPELINE = PROJECT_ROOT / "src" / "pipeline" / "run_data_quality_pipeline.py"
WEB_APP = PROJECT_ROOT / "web_app.py"


def backup(path: Path) -> Path:
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    target = path.with_name(f"{path.stem}_backup_unicode_{stamp}{path.suffix}")
    shutil.copy2(path, target)
    print(f"[OK] Backup criado: {target}")
    return target


def patch_pipeline(path: Path) -> None:
    if not path.exists():
        raise FileNotFoundError(f"Pipeline não encontrado: {path}")

    backup(path)
    text = path.read_text(encoding="utf-8")

    helper = '''

def safe_console_print(value=""):
    """Imprime sem falhar em consoles Windows configurados como CP1252."""
    text = "" if value is None else str(value)
    stream = getattr(__import__("sys"), "stdout")
    encoding = getattr(stream, "encoding", None) or "utf-8"
    safe_text = text.encode(encoding, errors="replace").decode(
        encoding, errors="replace"
    )
    try:
        print(safe_text)
    except UnicodeEncodeError:
        print(safe_text.encode("ascii", errors="replace").decode("ascii"))
'''

    if "def safe_console_print(" not in text:
        anchor = "class CommandExecutionError(RuntimeError):\n    pass\n"
        if anchor in text:
            text = text.replace(anchor, anchor + helper, 1)
        else:
            index = text.find("\ndef ")
            if index < 0:
                raise RuntimeError(
                    "Não foi possível localizar ponto de inserção no pipeline."
                )
            text = text[:index] + helper + text[index:]

    text = text.replace(
        "print(captured_output)",
        "safe_console_print(captured_output)",
    )
    text = text.replace(
        "print(result.stdout)",
        "safe_console_print(result.stdout)",
    )

    path.write_text(text, encoding="utf-8")
    py_compile.compile(str(path), doraise=True)
    print(f"[OK] Pipeline corrigido e compilado: {path}")


def patch_web_app(path: Path) -> None:
    if not path.exists():
        print(f"[AVISO] web_app.py não encontrado: {path}")
        return

    backup(path)
    text = path.read_text(encoding="utf-8")
    anchor = 'environment["PYTHONUNBUFFERED"] = "1"'
    addition = (
        anchor
        + '\n    environment["PYTHONUTF8"] = "1"'
        + '\n    environment["PYTHONIOENCODING"] = "utf-8"'
    )

    if (
        'environment["PYTHONIOENCODING"] = "utf-8"' not in text
        and anchor in text
    ):
        text = text.replace(anchor, addition, 1)

    path.write_text(text, encoding="utf-8")
    py_compile.compile(str(path), doraise=True)
    print(f"[OK] Streamlit corrigido e compilado: {path}")


if __name__ == "__main__":
    patch_pipeline(PIPELINE)
    patch_web_app(WEB_APP)
    print("[OK] Correção de Unicode concluída.")
