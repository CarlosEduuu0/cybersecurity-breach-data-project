import os
import re
import json
import hashlib
import unicodedata
import pandas as pd
from datetime import datetime
from pathlib import Path
import kagglehub

dataset_path = kagglehub.dataset_download("algozee/cyber-security")

print("path to dataset files:", dataset_path)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
bronze_path = PROJECT_ROOT / "data" / "bronze"
bronze_path.mkdir(parents=True, exist_ok=True)


# ---------------------------------------------------------------------------
# 1.1 — Padronização de nomes de colunas
# ---------------------------------------------------------------------------

def _to_snake_case(name: str) -> str:
    """Converte um nome de coluna para snake_case sem acentos/caracteres especiais."""
    # Decompõe em forma NFD e descarta os caracteres combinantes (diacríticos)
    name = unicodedata.normalize("NFD", name)
    name = "".join(c for c in name if not unicodedata.combining(c))
    # Substitui espaços e hífens por underscore
    name = re.sub(r"[\s\-]+", "_", name)
    # Insere underscore entre CamelCase (ex: "AttackVector" → "Attack_Vector")
    name = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", name)
    # Remove caracteres que não sejam letras, números ou underscore
    name = re.sub(r"[^\w]", "", name)
    # Remove underscores duplos e transforma em lowercase
    name = re.sub(r"_+", "_", name).strip("_").lower()
    return name


def standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Renomeia todas as colunas do DataFrame para snake_case."""
    new_columns = {col: _to_snake_case(col) for col in df.columns}
    renamed = df.rename(columns=new_columns)
    changed = {orig: new for orig, new in new_columns.items() if orig != new}
    if changed:
        print(f"  colunas renomeadas: {changed}")
    return renamed


# ---------------------------------------------------------------------------
# 1.2 — Metadados de ingestão
# ---------------------------------------------------------------------------

metadata_file = bronze_path / "metadata.json"


def _compute_hash(file_path: str) -> str:
    file_hash = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            file_hash.update(chunk)
    return file_hash.hexdigest()


def _load_metadata() -> list:
    if metadata_file.exists():
        with open(metadata_file, "r", encoding="utf-8") as f:
            return json.load(f)
    return []


def _save_metadata(entries: list) -> None:
    with open(metadata_file, "w", encoding="utf-8") as f:
        json.dump(entries, f, indent=2, ensure_ascii=False, default=str)


def record_metadata(df: pd.DataFrame, file_name: str, source_path: str) -> None:
    """Registra os metadados do arquivo ingerido em data/bronze/metadata.json."""
    entry = {
        "nome_arquivo":   file_name,
        "caminho_origem": source_path,
        "num_linhas":     len(df),
        "num_colunas":    len(df.columns),
        "hash_md5":       _compute_hash(source_path),
        "data_hora_carga": datetime.now().isoformat(),
        "colunas":        list(df.columns),
        "tipos":          {col: str(dtype) for col, dtype in df.dtypes.items()},
    }

    entries = _load_metadata()
    # Substitui entrada existente para o mesmo arquivo (re-ingestão)
    entries = [e for e in entries if e["nome_arquivo"] != file_name]
    entries.append(entry)
    _save_metadata(entries)

    print(f"  metadata registrado: {file_name} | {entry['num_linhas']} linhas | hash={entry['hash_md5']}")


# ---------------------------------------------------------------------------
# Carga e salvamento
# ---------------------------------------------------------------------------

def load_file(file_path: str) -> pd.DataFrame | None:
    try:
        if file_path.endswith(".csv"):
            df = pd.read_csv(file_path)
        elif file_path.endswith(".json"):
            df = pd.read_json(file_path)
        else:
            return None

        print(f"loaded: {file_path} | rows: {len(df)}")
        return df

    except Exception as e:
        print(f"error loading {file_path}: {e}")
        return None


def save_parquet(df: pd.DataFrame, file_name: str, source_path: str) -> None:
    output_file = bronze_path / (Path(file_name).stem + ".parquet")
    # Colunas de lineage por linha — permitem rastrear origem diretamente no Parquet
    # sem depender do metadata.json. row_count/hash ficam só no JSON (redundantes por linha).
    df = df.assign(
        ingestion_timestamp=datetime.now(),
        source_file=source_path,
    )
    df.to_parquet(output_file, index=False)
    print(f"  parquet salvo: {output_file}")


# ---------------------------------------------------------------------------
# Pipeline principal
# ---------------------------------------------------------------------------

def run_pipeline():
    print("starting ingestion...")

    bronze_path.mkdir(parents=True, exist_ok=True)

    dataset_path = kagglehub.dataset_download("algozee/cyber-security")
    print(f"dataset path: {dataset_path}")

    for root, dirs, files in os.walk(dataset_path):
        for file in files:
            full_path = os.path.join(root, file)

            df = load_file(full_path)
            if df is None:
                continue

            print(f"\n  [1.1] padronizando colunas de {file}...")
            df = standardize_columns(df)

            print(f"  [1.2] registrando metadados de {file}...")
            record_metadata(df, file, full_path)

            save_parquet(df, file, full_path)

    print("\ningestion finished")


# ---------------------------------------------------------------------------
# 1.3 — Validação da ingestão
# ---------------------------------------------------------------------------

REQUIRED_METADATA_FIELDS = {
    "nome_arquivo", "caminho_origem", "num_linhas", "num_colunas",
    "hash_md5", "data_hora_carga", "colunas", "tipos",
}


def validate_ingestion() -> bool:
    """Verifica se todos os passos da ingestão foram gerados corretamente."""
    print("\n" + "=" * 60)
    print("VALIDAÇÃO DE INGESTÃO")
    print("=" * 60)
    ok = True

    # 1. Pasta bronze existe
    if bronze_path.exists():
        print(f"[OK] Pasta bronze existe: {bronze_path}")
    else:
        print(f"[FALHA] Pasta bronze não encontrada: {bronze_path}")
        return False

    # 2. Pelo menos um Parquet gerado
    parquet_files = list(bronze_path.glob("*.parquet"))
    if parquet_files:
        print(f"[OK] {len(parquet_files)} arquivo(s) Parquet encontrado(s):")
        for pf in parquet_files:
            size_kb = pf.stat().st_size / 1024
            print(f"     {pf.name}  ({size_kb:.1f} KB)")
    else:
        print("[FALHA] Nenhum arquivo Parquet encontrado em data/bronze/")
        ok = False

    # 3. metadata.json existe
    if not metadata_file.exists():
        print("[FALHA] metadata.json não encontrado")
        return False
    print(f"[OK] metadata.json encontrado")

    # 4. metadata.json tem todos os campos obrigatórios em cada entrada
    entries = _load_metadata()
    if not entries:
        print("[FALHA] metadata.json está vazio")
        ok = False
    else:
        for entry in entries:
            missing = REQUIRED_METADATA_FIELDS - set(entry.keys())
            nome = entry.get('nome_arquivo', '(desconhecido)')
            if missing:
                print(f"[FALHA] Campos ausentes em '{nome}': {missing}")
                ok = False
            else:
                print(f"[OK] Metadados completos para '{nome}' "
                      f"({entry['num_linhas']} linhas, hash={entry['hash_md5']})")

    # 5. Cada Parquet tem as colunas de lineage
    for pf in parquet_files:
        try:
            df_check = pd.read_parquet(pf)
            for col in ("ingestion_timestamp", "source_file"):
                if col not in df_check.columns:
                    print(f"[FALHA] Coluna '{col}' ausente em {pf.name}")
                    ok = False
            if ok:
                print(f"[OK] Colunas de lineage presentes em {pf.name}")
        except Exception as e:
            print(f"[FALHA] Erro ao ler {pf.name}: {e}")
            ok = False

    print("=" * 60)
    print("RESULTADO:", "PASSOU" if ok else "FALHOU")
    print("=" * 60)
    return ok


if __name__ == "__main__":
    run_pipeline()
    validate_ingestion()