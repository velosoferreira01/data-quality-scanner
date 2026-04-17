from pathlib import Path
import subprocess
import sys


def run_mjv(file_path):
    file_path = Path(file_path)

    if not file_path.exists():
        raise FileNotFoundError(file_path)

    with open(file_path, "r", encoding="utf-8") as f:
        lines = f.readlines()

    for line in lines:

        cmd = line.strip()

        if not cmd:
            continue

        if cmd.startswith("#"):
            continue

        print(f"[MJV] Executando: {cmd}")

        result = subprocess.run(cmd, shell=True)

        if result.returncode != 0:
            raise RuntimeError(f"Erro ao executar: {cmd}")


if __name__ == "__main__":

    if len(sys.argv) < 2:
        print("Uso:")
        print("python mjv_runner.py setup.mjv")
        sys.exit(1)

    run_mjv(sys.argv[1])