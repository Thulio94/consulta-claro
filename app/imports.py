from __future__ import annotations

import csv
from pathlib import Path
from typing import Iterator

from . import database


def _open_text(path: Path):
    for encoding in ("utf-8-sig", "utf-8", "latin1"):
        try:
            handle = path.open("r", encoding=encoding, newline="")
            handle.read(8192)
            handle.seek(0)
            return handle
        except UnicodeDecodeError:
            try:
                handle.close()
            except Exception:
                pass
    return path.open("r", encoding="latin1", newline="")


def import_csv(path: Path, cep_column: str, numero_column: str) -> dict[str, int]:
    handle = _open_text(path)
    try:
        sample = handle.read(8192)
        handle.seek(0)
        try:
            delimiter = csv.Sniffer().sniff(sample, delimiters=";,|\t").delimiter
        except csv.Error:
            delimiter = ";"
        reader = csv.DictReader(handle, delimiter=delimiter)
        if not reader.fieldnames:
            raise ValueError("O CSV não possui cabeçalho.")
        fields = {name.strip(): name for name in reader.fieldnames}
        cep_real = fields.get(cep_column.strip())
        numero_real = fields.get(numero_column.strip())
        if not cep_real or not numero_real:
            raise ValueError("As colunas selecionadas não existem no arquivo.")

        def rows() -> Iterator[tuple[str, str]]:
            for row in reader:
                yield row.get(cep_real, ""), row.get(numero_real, "")

        return database.insert_records(rows())
    finally:
        handle.close()
