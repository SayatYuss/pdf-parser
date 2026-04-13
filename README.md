# PDF Parser

Скрипт извлекает текст из PDF-документов судебных дел и сохраняет результат в `JSONL`.

## Что делает

- читает дамп `sud_db_dump_*.json` из папки `documents` (или из ZIP в корне проекта);
- находит документы каждого дела;
- извлекает текст из PDF (`pdfplumber`);
- очищает текст от лишних пробелов и переносов;
- сохраняет по одной записи на документ в выходной `jsonl`.

## Требования

- Python 3.10+
- Установленные зависимости (минимум `pdfplumber`)

Если используете `venv`:

```bash
source venv/bin/activate
pip install -r requirements.txt
```

## Входные данные

Поддерживаются 2 варианта:

1. Папка `documents` в проекте:
- `documents/sud_db_dump_*.json`
- `documents/files/...*.pdf`

2. ZIP в корне проекта:
- в архиве должна быть папка `documents` с дампом и PDF.
- если в корне ровно один `.zip`, он подхватится автоматически.

## Запуск

### 1) Обработать диапазон дел

```bash
python main.py -s 0 -e 10 --out result.jsonl
```

### 2) Обработать все дела

```bash
python main.py --all --out result.jsonl
```

### 3) Запуск из конкретного ZIP

```bash
python main.py --zip documents_ready_20260413_201510.zip --all --out result.jsonl
```

## Параметры

- `-s, --start` — начальный индекс дела
- `-e, --end` — конечный индекс дела (включительно)
- `-a, --all` — обработать все дела
- `--out` — путь к выходному `jsonl`
- `--zip` — путь к ZIP-архиву с папкой `documents`

Важно:

- `--all` нельзя использовать вместе с `--start/--end`.
- Если `--out` не указан, файл сохранится в корне проекта как `parse_results_YYYYMMDD_HHMMSS.jsonl`.

## Формат выходной записи (`jsonl`)

Каждая строка — отдельный JSON-объект (один документ):

```json
{
  "generated_at": "2026-04-14 01:09:31",
  "selected_dump": "sud_db_dump_20260408_091133.json",
  "case_index": 1,
  "case_id": 1,
  "case_number": "7142-25-00-2/3395",
  "document_id": 1,
  "original_filename": "Павлова Сагиндиков.pdf",
  "file_path": "files/2025/...pdf",
  "status": "ok",
  "exists": true,
  "text_raw": "...",
  "text_clean": "...",
  "text_length": 11484
}
```

## Статусы обработки

- `ok` — текст успешно извлечен и очищен
- `missing_file_path` — в дампе нет `file_path`
- `file_not_found` — PDF не найден по пути
- `no_text_extracted` — из PDF не удалось извлечь текст
- `no_text_features` — после очистки текст пустой
- `processing_error` — ошибка чтения/обработки PDF

## Примечание

Тестировалось на Ubuntu 24.04.
