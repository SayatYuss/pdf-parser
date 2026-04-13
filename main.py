import json
import re
import argparse
import shutil
import tempfile
import zipfile
from datetime import datetime
from pathlib import Path
import pdfplumber


parser = argparse.ArgumentParser(description="Извлечение и очистка текста из PDF в JSONL")
parser.add_argument("-s", "--start", type=int, help="Начальный индекс дела")
parser.add_argument("-e", "--end", type=int, help="Конечный индекс дела (включительно)")
parser.add_argument("-a", "--all", action="store_true", help="Обработать все дела из дампа")
parser.add_argument("--out", type=str, help="Путь к выходному JSONL-файлу")
parser.add_argument("--zip", dest="zip_file", type=str, help="Путь к ZIP-архиву с папкой documents")
args = parser.parse_args()

project_root = Path(__file__).resolve().parent
temp_extract_dir: Path | None = None


def find_zip_in_root(root: Path) -> Path | None:
    zip_files = sorted(root.glob("*.zip"))
    if len(zip_files) == 1:
        return zip_files[0]
    return None


zip_path: Path | None = None
if args.zip_file:
    zip_path = Path(args.zip_file).expanduser().resolve()
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
else:
    documents_root = project_root / "documents"

dump_files = sorted(documents_root.glob("sud_db_dump_*.json"), reverse=True)
if not dump_files:
    raise FileNotFoundError("В папке documents не найдено файлов sud_db_dump_*.json")

print("Доступные дампы:")
for i, dump_file in enumerate(dump_files, 1):
    print(f"{i}. {dump_file.name}")

if len(dump_files) == 1:
    selected_dump = dump_files[0]
    print(f"Автовыбор дампа: {selected_dump.name}")
else:
    choice = input("Выбери номер дампа (Enter = 1): ").strip() or "1"
    if not choice.isdigit() or not (1 <= int(choice) <= len(dump_files)):
        raise ValueError(f"Номер должен быть от 1 до {len(dump_files)}")
    selected_dump = dump_files[int(choice) - 1]

with selected_dump.open(encoding="utf-8") as f:
    data = json.load(f)

cases = data["tables"]["cases"]

if args.all and (args.start is not None or args.end is not None):
    raise ValueError("Ключ --all нельзя использовать вместе с --start/--end")

if args.all:
    cases_to_process = cases
    print(f"Режим полного прохода: все дела ({len(cases_to_process)})")
elif args.start is None and args.end is None:
    case_id = 1  # режим по умолчанию
    case = next((item for item in cases if item.get("id") == case_id), None)
    if case is None:
        raise ValueError(f"Кейс с id={case_id} не найден")
    cases_to_process = [case]
    print(f"Режим по умолчанию: case_id={case_id}")
else:
    start = 0 if args.start is None else args.start
    end = start if args.end is None else args.end

    if start < 0:
        raise ValueError("start должен быть >= 0")
    if end < start:
        raise ValueError("end должен быть >= start")
    if start >= len(cases):
        raise ValueError(f"start вне диапазона: максимум {len(cases) - 1}")

    end = min(end, len(cases) - 1)
    cases_to_process = cases[start:end + 1]
    print(f"Режим диапазона: дела с индекса {start} по {end}")

run_generated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
if args.out:
    output_file = Path(args.out)
else:
    output_file = project_root / f"parse_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.jsonl"

output_file.parent.mkdir(parents=True, exist_ok=True)
written_records = 0


def clean_pdf_text(text: str) -> str:
    # Remove hyphenation at line breaks: "видео-\nконференц" -> "видеоконференц".
    text = re.sub(r"(\w)-\n(\w)", r"\1\2", text)
    # Convert single line breaks to spaces, keep paragraph boundaries.
    text = re.sub(r"(?<!\n)\n(?!\n)", " ", text)
    # Normalize repeated spaces and too many blank lines.
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()

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

print(f"\nРезультаты сохранены: {output_file}")
print(f"Записей в JSONL: {written_records}")

if temp_extract_dir and temp_extract_dir.exists():
    shutil.rmtree(temp_extract_dir)