import argparse
import json
import re
import shutil
import tempfile
import zipfile
from datetime import datetime
from pathlib import Path

import pdfplumber


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Парсинг данных по делам из dump/ZIP в JSONL")
    parser.add_argument("--mode", choices=["text", "claim_outcome"], default="text", help="Режим работы")
    parser.add_argument("-s", "--start", type=int, help="Начальный индекс дела")
    parser.add_argument("-e", "--end", type=int, help="Конечный индекс дела (включительно)")
    parser.add_argument("-a", "--all", action="store_true", help="Обработать все дела из дампа")
    parser.add_argument("--out", type=str, help="Путь к выходному JSONL-файлу")
    parser.add_argument("--zip", dest="zip_file", type=str, help="Путь к ZIP-архиву с папкой documents")
    parser.add_argument("--limit", type=int, help="Ограничение количества дел (для mode=claim_outcome)")
    parser.add_argument(
        "--dictionary",
        type=str,
        help="Путь к claim_decision_dictionary.json (для mode=claim_outcome)",
    )
    return parser.parse_args()


def find_zip_in_root(root: Path) -> Path | None:
    zip_files = sorted(root.glob("*.zip"))
    if len(zip_files) == 1:
        return zip_files[0]
    return None


def prepare_documents_root(project_root: Path, zip_file_arg: str | None) -> tuple[Path, Path | None]:
    temp_extract_dir: Path | None = None

    zip_path: Path | None = None
    if zip_file_arg:
        zip_path = Path(zip_file_arg).expanduser().resolve()
        if not zip_path.exists():
            raise FileNotFoundError(f"ZIP-файл не найден: {zip_path}")
    else:
        zip_path = find_zip_in_root(project_root)

    if zip_path:
        print(f"Используется ZIP: {zip_path.name}")
        temp_extract_dir = Path(tempfile.mkdtemp(prefix="pdf_parser_"))
        with zipfile.ZipFile(zip_path, "r") as zf:
            zf.extractall(temp_extract_dir)
        documents_root = temp_extract_dir / "documents"
        if not documents_root.exists():
            raise FileNotFoundError("В архиве не найдена папка documents")
        return documents_root, temp_extract_dir

    return project_root / "documents", temp_extract_dir


def select_dump(documents_root: Path) -> Path:
    dump_files = sorted(documents_root.glob("sud_db_dump_*.json"), reverse=True)
    if not dump_files:
        raise FileNotFoundError("В папке documents не найдено файлов sud_db_dump_*.json")

    print("Доступные дампы:")
    for i, dump_file in enumerate(dump_files, 1):
        print(f"{i}. {dump_file.name}")

    if len(dump_files) == 1:
        selected_dump = dump_files[0]
        print(f"Автовыбор дампа: {selected_dump.name}")
        return selected_dump

    choice = input("Выбери номер дампа (Enter = 1): ").strip() or "1"
    if not choice.isdigit() or not (1 <= int(choice) <= len(dump_files)):
        raise ValueError(f"Номер должен быть от 1 до {len(dump_files)}")
    return dump_files[int(choice) - 1]


def choose_cases(cases: list[dict], args: argparse.Namespace) -> list[dict]:
    if args.all and (args.start is not None or args.end is not None):
        raise ValueError("Ключ --all нельзя использовать вместе с --start/--end")

    if args.all:
        cases_to_process = cases
        print(f"Режим полного прохода: все дела ({len(cases_to_process)})")
        return cases_to_process

    if args.start is None and args.end is None:
        case_id = 1
        case = next((item for item in cases if item.get("id") == case_id), None)
        if case is None:
            raise ValueError(f"Кейс с id={case_id} не найден")
        print(f"Режим по умолчанию: case_id={case_id}")
        return [case]

    start = 0 if args.start is None else args.start
    end = start if args.end is None else args.end

    if start < 0:
        raise ValueError("start должен быть >= 0")
    if end < start:
        raise ValueError("end должен быть >= start")
    if start >= len(cases):
        raise ValueError(f"start вне диапазона: максимум {len(cases) - 1}")

    end = min(end, len(cases) - 1)
    print(f"Режим диапазона: дела с индекса {start} по {end}")
    return cases[start:end + 1]


def clean_pdf_text(text: str) -> str:
    text = re.sub(r"(\w)-\n(\w)", r"\1\2", text)
    text = re.sub(r"(?<!\n)\n(?!\n)", " ", text)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def load_dictionary(path: Path) -> dict:
    if not path.exists():
        raise FileNotFoundError(f"Словарь не найден: {path}")
    with path.open(encoding="utf-8") as f:
        return json.load(f)


def map_decision(decision: str, mapping: dict) -> str:
    value = (decision or "").strip().lower()
    if not value:
        return mapping.get("empty_decision_label", "unknown")

    exact_map = mapping.get("exact_map", {})
    if value in exact_map:
        return exact_map[value]

    regex_fallback = mapping.get("regex_fallback", {})
    for label, patterns in regex_fallback.items():
        for pattern in patterns:
            if re.search(pattern, value, flags=re.IGNORECASE):
                return label

    return "unknown"


def run_text_mode(
    cases_to_process: list[dict],
    documents_root: Path,
    selected_dump: Path,
    output_file: Path,
    mapping: dict,
) -> int:
    run_generated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    written_records = 0

    with output_file.open("w", encoding="utf-8") as out_f:
        for idx, case in enumerate(cases_to_process, 1):
            docs_for_case = case.get("documents", [])
            case_label = case.get("case_number") or f"id={case.get('id')}"
            print(f"\n[{idx}/{len(cases_to_process)}] Дело: {case_label}")

            if not docs_for_case:
                print("Документы отсутствуют")
                continue

            for doc in docs_for_case:
                print(doc.get("original_filename"), doc.get("file_path"))

            for doc in docs_for_case:
                file_path = doc.get("file_path")
                original_filename = doc.get("original_filename")

                record = {
                    "generated_at": run_generated_at,
                    "selected_dump": selected_dump.name,
                    "case_index": idx,
                    "case_id": case.get("id"),
                    "case_number": case.get("case_number"),
                    "case_type": case.get("case_type"),
                    "court": case.get("court"),
                    "decision": case.get("decision"),
                    "claim_outcome": map_decision(case.get("decision", ""), mapping),
                    "category": case.get("category"),
                    "district_id": case.get("district_id"),
                    "district_name": case.get("district_name"),
                    "category_id": case.get("category_id"),
                    "year": case.get("year"),
                    "document_id": doc.get("id"),
                    "doc_date": doc.get("doc_date"),
                    "doc_type": doc.get("doc_type"),
                    "original_filename": original_filename,
                    "file_path": file_path,
                    "status": "pending",
                    "exists": None,
                    "text_raw": "",
                    "text_clean": "",
                    "text_length": 0,
                }

                if not file_path:
                    print(f"=== {original_filename} ===")
                    print(f"Файл не указан для документа: {original_filename}")
                    record["status"] = "missing_file_path"
                    record["exists"] = False
                    out_f.write(json.dumps(record, ensure_ascii=False) + "\n")
                    written_records += 1
                    continue

                path = documents_root / file_path
                record["exists"] = path.exists()

                if not path.exists():
                    print(f"=== {original_filename} ===")
                    print(f"Файл не найден: {path}")
                    record["status"] = "file_not_found"
                    out_f.write(json.dumps(record, ensure_ascii=False) + "\n")
                    written_records += 1
                    continue

                try:
                    with pdfplumber.open(path) as pdf:
                        text_raw = "\n".join(page.extract_text() or "" for page in pdf.pages).strip()
                except Exception as e:
                    print(f"=== {original_filename} ===")
                    print(f"warning: PDF processing error: {e}")
                    record["status"] = "processing_error"
                    record["error"] = str(e)
                    out_f.write(json.dumps(record, ensure_ascii=False) + "\n")
                    written_records += 1
                    continue

                record["text_raw"] = text_raw

                if not text_raw:
                    print(f"=== {original_filename} ===")
                    print("warning: No text extracted")
                    record["status"] = "no_text_extracted"
                    out_f.write(json.dumps(record, ensure_ascii=False) + "\n")
                    written_records += 1
                    continue

                clean_text = clean_pdf_text(text_raw)
                record["text_clean"] = clean_text
                record["text_length"] = len(clean_text)

                if not clean_text:
                    print(f"=== {original_filename} ===")
                    print("warning: No text left after cleaning")
                    record["status"] = "no_text_features"
                    record["error"] = "No text left after cleaning"
                    out_f.write(json.dumps(record, ensure_ascii=False) + "\n")
                    written_records += 1
                    continue

                record["status"] = "ok"
                print(f"=== {original_filename} ===")
                print("text_length:", record["text_length"])
                out_f.write(json.dumps(record, ensure_ascii=False) + "\n")
                written_records += 1

    return written_records


def run_claim_outcome_mode(
    cases_to_process: list[dict],
    selected_dump: Path,
    output_file: Path,
    mapping: dict,
    limit: int | None,
) -> int:
    run_generated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    written_records = 0

    with output_file.open("w", encoding="utf-8") as out_f:
        for case in cases_to_process:
            if limit is not None and written_records >= limit:
                break

            decision_raw = case.get("decision", "")
            outcome = map_decision(decision_raw, mapping)

            row = {
                "generated_at": run_generated_at,
                "selected_dump": selected_dump.name,
                "case_id": case.get("id"),
                "case_number": case.get("case_number"),
                "decision": decision_raw,
                "claim_outcome": outcome,
            }
            out_f.write(json.dumps(row, ensure_ascii=False) + "\n")
            written_records += 1

    return written_records


def main() -> None:
    args = parse_args()
    project_root = Path(__file__).resolve().parent
    temp_extract_dir: Path | None = None

    try:
        documents_root, temp_extract_dir = prepare_documents_root(project_root, args.zip_file)
        selected_dump = select_dump(documents_root)

        with selected_dump.open(encoding="utf-8") as f:
            data = json.load(f)
        cases = data.get("tables", {}).get("cases", [])

        cases_to_process = choose_cases(cases, args)

        if args.out:
            output_file = Path(args.out)
        elif args.mode == "claim_outcome":
            output_file = project_root / f"decision_test_{datetime.now().strftime('%Y%m%d_%H%M%S')}.jsonl"
        else:
            output_file = project_root / f"parse_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.jsonl"

        output_file.parent.mkdir(parents=True, exist_ok=True)

        dictionary_path = (
            Path(args.dictionary).expanduser().resolve()
            if args.dictionary
            else project_root / "claim_decision_dictionary.json"
        )
        mapping = load_dictionary(dictionary_path)

        if args.mode == "claim_outcome":
            written_records = run_claim_outcome_mode(
                cases_to_process=cases_to_process,
                selected_dump=selected_dump,
                output_file=output_file,
                mapping=mapping,
                limit=args.limit,
            )
        else:
            written_records = run_text_mode(
                cases_to_process=cases_to_process,
                documents_root=documents_root,
                selected_dump=selected_dump,
                output_file=output_file,
                mapping=mapping,
            )

        print(f"\nРезультаты сохранены: {output_file}")
        print(f"Записей в JSONL: {written_records}")

    finally:
        if temp_extract_dir and temp_extract_dir.exists():
            shutil.rmtree(temp_extract_dir)


if __name__ == "__main__":
    main()