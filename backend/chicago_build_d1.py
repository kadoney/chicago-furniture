#!/usr/bin/env python3
"""
chicago_build_d1.py — Convert chicago_furniture_raw.json to
chunked SQL files for import into Cloudflare D1 (chicago-furniture database).

Reads:  chicago_furniture_raw.json
Writes: chunks/000_schema.sql
        chunks/import_001.sql  (100 rows each)
        ...
        chunks/run_import.bat

Usage:
  python chicago_build_d1.py              # build from raw JSON
  python chicago_build_d1.py --chunk 50   # rows per chunk (default 100)

Import after running:
  wrangler d1 execute chicago-furniture --remote --file=chunks\\000_schema.sql
  for %f in (chunks\\import_*.sql) do wrangler d1 execute chicago-furniture --remote --file=%f
  (or run the generated chunks\\run_import.bat)
"""

import argparse
import json
import os
import shutil
from collections import defaultdict

SCHEMA_FILE   = os.path.join(os.path.dirname(__file__), 'chicago_schema.sql')
RAW_FILE      = 'chicago_furniture_raw.json'
CHUNKS_DIR    = 'chunks'
DEFAULT_CHUNK = 100


def q(val):
    """SQL-escape a value. Returns NULL for None, quoted string otherwise."""
    if val is None:
        return 'NULL'
    escaped = str(val).replace("'", "''")
    return f"'{escaped}'"


def qi(val):
    """SQL integer or NULL."""
    if val is None:
        return 'NULL'
    try:
        return str(int(val))
    except (TypeError, ValueError):
        return 'NULL'


def build_insert(rec):
    return (
        "INSERT INTO furniture ("
        "aic_id, accession, title, "
        "classification, department, form_bucket, form_type, "
        "maker_name, maker_display, "
        "origin, place, "
        "date_display, date_begin, date_end, "
        "medium, dimensions, "
        "creditline, "
        "image_id, alt_text, "
        "collection_url"
        ") VALUES ("
        f"{qi(rec.get('aic_id'))}, "
        f"{q(rec.get('accession'))}, "
        f"{q(rec.get('title'))}, "
        f"{q(rec.get('classification'))}, "
        f"{q(rec.get('department'))}, "
        f"{q(rec.get('form_bucket'))}, "
        f"{q(rec.get('form_type'))}, "
        f"{q(rec.get('maker_name'))}, "
        f"{q(rec.get('maker_display'))}, "
        f"{q(rec.get('origin'))}, "
        f"{q(rec.get('place'))}, "
        f"{q(rec.get('date_display'))}, "
        f"{qi(rec.get('date_begin'))}, "
        f"{qi(rec.get('date_end'))}, "
        f"{q(rec.get('medium'))}, "
        f"{q(rec.get('dimensions'))}, "
        f"{q(rec.get('creditline'))}, "
        f"{q(rec.get('image_id'))}, "
        f"{q(rec.get('alt_text'))}, "
        f"{q(rec.get('collection_url'))}"
        ");"
    )


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--chunk', type=int, default=DEFAULT_CHUNK, help='Rows per chunk')
    args = ap.parse_args()

    if not os.path.exists(RAW_FILE):
        print(f'ERROR: {RAW_FILE} not found. Run chicago_harvest.py first.')
        exit(1)

    with open(RAW_FILE, encoding='utf-8') as f:
        records = json.load(f)
    print(f'Loaded {len(records)} records from {RAW_FILE}')

    if not os.path.exists(SCHEMA_FILE):
        print(f'ERROR: Schema file not found: {SCHEMA_FILE}')
        exit(1)

    if os.path.exists(CHUNKS_DIR):
        shutil.rmtree(CHUNKS_DIR)
    os.makedirs(CHUNKS_DIR)

    # Schema as chunk 000
    schema_dest = os.path.join(CHUNKS_DIR, '000_schema.sql')
    shutil.copy(SCHEMA_FILE, schema_dest)
    print(f'Schema: {schema_dest}')

    chunk_num  = 1
    chunk_rows = 0
    chunk_file = None
    total_rows = 0
    skipped    = 0

    def open_chunk(n):
        path = os.path.join(CHUNKS_DIR, f'import_{n:03d}.sql')
        f = open(path, 'w', encoding='utf-8')
        f.write(f'-- Art Institute of Chicago furniture import — chunk {n}\n\n')
        return f

    for rec in records:
        if not rec.get('aic_id'):
            skipped += 1
            continue

        if chunk_rows == 0:
            chunk_file = open_chunk(chunk_num)

        try:
            sql = build_insert(rec)
            chunk_file.write(sql + '\n')
            chunk_rows += 1
            total_rows += 1
        except Exception as e:
            print(f'  ERROR on {rec.get("aic_id")}: {e}')
            skipped += 1
            continue

        if chunk_rows >= args.chunk:
            chunk_file.close()
            chunk_rows = 0
            chunk_num += 1

    if chunk_file and chunk_rows > 0:
        chunk_file.close()

    total_chunks = chunk_num if chunk_rows > 0 else chunk_num - 1

    # Batch import .bat
    bat_lines = [
        '@echo off',
        'echo Importing Art Institute of Chicago furniture into D1...',
        'wrangler d1 execute chicago-furniture --remote --file=chunks\\000_schema.sql',
    ]
    for i in range(1, total_chunks + 1):
        bat_lines.append(
            f'wrangler d1 execute chicago-furniture --remote --file=chunks\\import_{i:03d}.sql'
        )
    bat_lines += ['echo Done.', 'pause']
    with open(os.path.join(CHUNKS_DIR, 'run_import.bat'), 'w') as f:
        f.write('\n'.join(bat_lines))

    # Stats
    fb_counts     = defaultdict(int)
    origin_counts = defaultdict(int)
    for rec in records:
        if not rec.get('aic_id'):
            continue
        fb_counts[rec.get('form_bucket') or '(none)'] += 1
        origin_counts[rec.get('origin') or '(none)'] += 1

    print(f'\n=== Build complete ===')
    print(f'Records written: {total_rows}')
    print(f'Skipped:         {skipped}')
    print(f'Chunks:          {total_chunks}')

    print(f'\nForm bucket distribution:')
    for fb, c in sorted(fb_counts.items(), key=lambda x: -x[1]):
        print(f'  {fb:20s} {c:>4d}')

    print(f'\nOrigin distribution (top 15):')
    for i, (o, c) in enumerate(sorted(origin_counts.items(), key=lambda x: -x[1])):
        if i >= 15: break
        print(f'  {o:30s} {c:>4d}')

    print(f'\nTo import:')
    print(f'  cd {os.path.abspath(CHUNKS_DIR)}')
    print(f'  run_import.bat')
