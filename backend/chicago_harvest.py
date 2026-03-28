#!/usr/bin/env python3
"""
chicago_harvest.py — Harvest Art Institute of Chicago furniture objects.

Strategy:
  - POST to /artworks/search with match_phrase filter on classification_titles="furniture"
    and is_public_domain=true
  - API returns full field data inline (no separate hydration step needed)
  - Paginate via 'from' offset until exhausted
  - Normalize into flat records matching chicago_schema.sql
  - Write: chicago_furniture_raw.json
  - Write: chicago_harvest_summary.json

AIC API notes:
  - Base: https://api.artic.edu/api/v1
  - No auth required, CC0 data license
  - Search endpoint: POST /artworks/search with Elasticsearch DSL body
  - Filter works: match_phrase on classification_titles (analyzed text field)
  - Images: IIIF 2 — store image_id (UUID), derive URL at serve time
    URL pattern: https://www.artic.edu/iiif/2/{image_id}/full/843,/0/default.jpg
  - artist_display is multi-line: "Name (nationality, dates)\nCity"
  - main_reference_number = accession number

Usage:
  python chicago_harvest.py               # full harvest (~457 objects)
  python chicago_harvest.py --dry-run     # count only, no data
  python chicago_harvest.py --limit 20    # stop after N objects (testing)
"""

import argparse
import json
import os
import time
import urllib.request
from collections import defaultdict
from datetime import datetime, timezone

# ── Config ────────────────────────────────────────────────────────────────────

API_SEARCH = 'https://api.artic.edu/api/v1/artworks/search'

FIELDS = ','.join([
    'id', 'title',
    'date_display', 'date_start', 'date_end',
    'artist_display', 'place_of_origin',
    'medium_display', 'dimensions',
    'department_title', 'classification_titles', 'artwork_type_title',
    'image_id', 'is_public_domain',
    'thumbnail',
    'credit_line', 'main_reference_number',
])

PAGE_SIZE   = 100
PAGE_DELAY  = 0.4
TIMEOUT     = 30

OUTPUT_RAW     = 'chicago_furniture_raw.json'
OUTPUT_SUMMARY = 'chicago_harvest_summary.json'

HEADERS = {
    'User-Agent':   'Mozilla/5.0 (compatible; SAPFM-Chicago-Harvester/1.0)',
    'Content-Type': 'application/json',
    'Accept':       'application/json',
}

# ── HTTP ──────────────────────────────────────────────────────────────────────

def http_post(url, body, retries=3):
    data = json.dumps(body).encode('utf-8')
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, data=data, headers=HEADERS, method='POST')
            with urllib.request.urlopen(req, timeout=TIMEOUT) as resp:
                return json.loads(resp.read().decode('utf-8'))
        except Exception as e:
            if attempt < retries - 1:
                wait = 2 * (attempt + 1)
                print(f'    Retry {attempt+1}/{retries}: ({e})')
                time.sleep(wait)
            else:
                raise


def build_query(offset=0, limit=PAGE_SIZE):
    return {
        'fields': FIELDS,
        'limit': limit,
        'from': offset,
        'query': {
            'bool': {
                'filter': [
                    {'match_phrase': {'classification_titles': 'furniture'}},
                    {'term': {'is_public_domain': True}},
                ]
            }
        }
    }


# ── form_bucket mapping ──────────────────────────────────────────────────────

FORM_BUCKET_MAP = {
    # Chair / Seating
    'chair':              'Chair',
    'side chair':         'Chair',
    'armchair':           'Chair',
    'easy chair':         'Chair',
    'rocking chair':      'Chair',
    'corner chair':       'Chair',
    'wing chair':         'Chair',
    'highchair':          'Chair',
    'stool':              'Chair',
    'footstool':          'Chair',
    'throne':             'Chair',
    'fauteuil':           'Chair',
    # Sofa & Bench
    'sofa':               'Sofa & Bench',
    'settee':             'Sofa & Bench',
    'bench':              'Sofa & Bench',
    'daybed':             'Sofa & Bench',
    'canapé':             'Sofa & Bench',
    # Table
    'table':              'Table',
    'tea table':          'Table',
    'side table':         'Table',
    'console table':      'Table',
    'center table':       'Table',
    'card table':         'Table',
    'pier table':         'Table',
    'gaming table':       'Table',
    'dining table':       'Table',
    'dressing table':     'Table',
    'worktable':          'Table',
    'work table':         'Table',
    'drop-leaf table':    'Table',
    'tilt-top table':     'Table',
    'pembroke table':     'Table',
    'bureau table':       'Table',
    # Case Piece
    'cabinet':            'Case Piece',
    'cupboard':           'Case Piece',
    'wardrobe':           'Case Piece',
    'chest':              'Case Piece',
    'chest of drawers':   'Case Piece',
    'commode':            'Case Piece',
    'bookcase':           'Case Piece',
    'sideboard':          'Case Piece',
    'buffet':             'Case Piece',
    'highboy':            'Case Piece',
    'lowboy':             'Case Piece',
    'press':              'Case Piece',
    'linen press':        'Case Piece',
    'blanket chest':      'Case Piece',
    'kas':                'Case Piece',
    'armoire':            'Case Piece',
    'credenza':           'Case Piece',
    'vitrine':            'Case Piece',
    'secretary bookcase': 'Case Piece',
    'case furniture':     'Case Piece',
    # Desk
    'desk':               'Desk',
    'desk and bookcase':  'Desk',
    'secretary':          'Desk',
    'bureau':             'Desk',
    'writing desk':       'Desk',
    'writing table':      'Desk',
    'slant-front desk':   'Desk',
    'fall-front desk':    'Desk',
    # Bed
    'bed':                'Bed',
    'cradle':             'Bed',
    'bedstead':           'Bed',
    # Stand
    'stand':              'Stand',
    'candlestand':        'Stand',
    'plant stand':        'Stand',
    'hall stand':         'Stand',
    'fire screen':        'Stand',
    'screen':             'Stand',
    'torchère':           'Stand',
    'candle stand':       'Stand',
    'music stand':        'Stand',
    'pedestal':           'Stand',
    # Clock
    'tall clock':         'Clock',
    'clock':              'Clock',
    'mantel clock':       'Clock',
    'wall clock':         'Clock',
    'shelf clock':        'Clock',
    # Mirror
    'mirror':             'Mirror',
    'looking glass':      'Mirror',
}


def classify_title(title):
    """Try to extract a form type from the title for form_bucket mapping."""
    if not title:
        return None
    t = title.lower().strip()

    if t in FORM_BUCKET_MAP:
        return t

    # Longest-match prefix
    for form in sorted(FORM_BUCKET_MAP.keys(), key=len, reverse=True):
        if t.startswith(form):
            return form

    # Strip common prefixes
    for prefix in ('pair of ', 'set of ', 'miniature ', "child's "):
        if t.startswith(prefix):
            rest = t[len(prefix):]
            for article in ('a ', 'an ', 'the '):
                if rest.startswith(article):
                    rest = rest[len(article):]
                    break
            for form in sorted(FORM_BUCKET_MAP.keys(), key=len, reverse=True):
                if rest.startswith(form):
                    return form

    # Anywhere in title
    for form in sorted(FORM_BUCKET_MAP.keys(), key=len, reverse=True):
        if form in t:
            return form

    return None


# ── Parse record ──────────────────────────────────────────────────────────────

def parse_record(obj):
    """Parse a single AIC API artwork object into a flat record."""

    obj_id    = obj.get('id')
    title     = obj.get('title', '')
    accession = obj.get('main_reference_number')

    # Artist — first line only for maker_name; full string for maker_display
    artist_display = obj.get('artist_display') or ''
    maker_name = artist_display.split('\n')[0].strip() if artist_display else None
    maker_display = artist_display.replace('\n', '; ') if artist_display else None

    # Origin — place_of_origin is already a clean string
    place_of_origin = obj.get('place_of_origin')
    # Top-level origin: first comma segment
    origin = None
    if place_of_origin:
        origin = place_of_origin.split(',')[0].strip()

    # Dates
    date_display = obj.get('date_display')
    date_begin   = obj.get('date_start')
    date_end     = obj.get('date_end')

    try:
        date_begin = int(date_begin) if date_begin is not None else None
    except (TypeError, ValueError):
        date_begin = None
    try:
        date_end = int(date_end) if date_end is not None else None
    except (TypeError, ValueError):
        date_end = None

    # Medium / technique
    medium = obj.get('medium_display')

    # Dimensions
    dimensions = obj.get('dimensions')

    # Department + classification
    department         = obj.get('department_title')
    classification     = obj.get('artwork_type_title')   # "Furniture" for furniture objects
    classification_all = ', '.join(obj.get('classification_titles') or [])

    # Credit
    creditline = obj.get('credit_line')

    # Image — store id only; URLs derived at serve time
    image_id = obj.get('image_id')

    # Alt text from thumbnail
    thumb = obj.get('thumbnail') or {}
    alt_text = thumb.get('alt_text')

    # Collection URL
    collection_url = f'https://www.artic.edu/artworks/{obj_id}'

    # Form classification from title
    form_type   = classify_title(title)
    form_bucket = FORM_BUCKET_MAP.get(form_type) if form_type else None

    # Also check classification_titles for form hints if title didn't match
    if not form_bucket:
        cls_titles = [c.lower() for c in (obj.get('classification_titles') or [])]
        for cls in cls_titles:
            form_type_from_cls = classify_title(cls)
            if form_type_from_cls:
                form_type   = form_type_from_cls
                form_bucket = FORM_BUCKET_MAP.get(form_type)
                break

    return {
        'aic_id':           obj_id,
        'accession':        accession,
        'title':            title,
        'classification':   classification,
        'classification_all': classification_all,
        'department':       department,
        'form_bucket':      form_bucket,
        'form_type':        form_type,
        'maker_name':       maker_name,
        'maker_display':    maker_display,
        'origin':           origin,
        'place':            place_of_origin,
        'date_display':     date_display,
        'date_begin':       date_begin,
        'date_end':         date_end,
        'medium':           medium,
        'dimensions':       dimensions,
        'creditline':       creditline,
        'image_id':         image_id,
        'alt_text':         alt_text,
        'collection_url':   collection_url,
        'harvested_at':     datetime.now(timezone.utc).isoformat(),
    }


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--dry-run', action='store_true', help='Count only, no data')
    ap.add_argument('--limit',   type=int, default=None, help='Stop after N objects (testing)')
    args = ap.parse_args()

    print('=== Art Institute of Chicago — Furniture Harvest ===\n')

    # Dry run: just get the total
    if args.dry_run:
        data = http_post(API_SEARCH, build_query(limit=1))
        total = data.get('pagination', {}).get('total', 0)
        print(f'Total public-domain furniture objects: {total}')
        return

    records = []
    offset  = 0
    total   = None

    while True:
        limit_this = min(PAGE_SIZE, (args.limit - len(records)) if args.limit else PAGE_SIZE)
        data = http_post(API_SEARCH, build_query(offset=offset, limit=limit_this))

        pagination = data.get('pagination', {})
        if total is None:
            total = pagination.get('total', 0)
            print(f'Total available: {total}\n')

        page_data = data.get('data', [])
        if not page_data:
            print('No more data.')
            break

        for obj in page_data:
            rec = parse_record(obj)
            flags = []
            if rec['maker_name']:  flags.append('maker')
            if rec['image_id']:    flags.append('img')
            if rec['origin']:      flags.append(rec['origin'][:10])
            if rec['form_bucket']: flags.append(rec['form_bucket'])
            flag_str = ' [' + ','.join(flags) + ']' if flags else ''
            t = rec['title'] or '(untitled)'
            print(f'  {rec["aic_id"]:>8d}  {t[:55]}{flag_str}')
            records.append(rec)

        offset += len(page_data)
        print(f'  ... {offset}/{total} fetched')

        if args.limit and len(records) >= args.limit:
            print(f'[limit {args.limit}] stopping')
            break
        if offset >= total:
            break

        time.sleep(PAGE_DELAY)

    # Save raw JSON
    with open(OUTPUT_RAW, 'w', encoding='utf-8') as f:
        json.dump(records, f, indent=2, ensure_ascii=False)

    # Summary stats
    fb_counts     = defaultdict(int)
    origin_counts = defaultdict(int)
    dept_counts   = defaultdict(int)
    for rec in records:
        fb_counts[rec.get('form_bucket') or '(none)'] += 1
        origin_counts[rec.get('origin') or '(none)'] += 1
        dept_counts[rec.get('department') or '(none)'] += 1

    summary = {
        'harvested_at':      datetime.now(timezone.utc).isoformat(),
        'total_available':   total,
        'total_harvested':   len(records),
        'with_maker':        sum(1 for r in records if r.get('maker_name')),
        'with_date':         sum(1 for r in records if r.get('date_display')),
        'with_medium':       sum(1 for r in records if r.get('medium')),
        'with_dimensions':   sum(1 for r in records if r.get('dimensions')),
        'with_image':        sum(1 for r in records if r.get('image_id')),
        'with_origin':       sum(1 for r in records if r.get('origin')),
        'form_bucket_distribution': dict(sorted(fb_counts.items(),     key=lambda x: -x[1])),
        'origin_distribution':      dict(sorted(origin_counts.items(), key=lambda x: -x[1])),
        'department_distribution':  dict(sorted(dept_counts.items(),   key=lambda x: -x[1])),
    }
    with open(OUTPUT_SUMMARY, 'w', encoding='utf-8') as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)

    print(f'\n=== Done ===')
    print(f'Harvested:  {len(records)}')
    print(f'\nField population:')
    for field in ('with_maker', 'with_date', 'with_medium', 'with_dimensions',
                  'with_image', 'with_origin'):
        print(f'  {field:20s} {summary[field]:>4d}/{len(records)}')

    print(f'\nForm bucket distribution:')
    for fb, c in sorted(fb_counts.items(), key=lambda x: -x[1]):
        print(f'  {fb:20s} {c:>4d}')

    print(f'\nOrigin distribution (top 15):')
    for i, (o, c) in enumerate(sorted(origin_counts.items(), key=lambda x: -x[1])):
        if i >= 15: break
        print(f'  {o:30s} {c:>4d}')

    print(f'\nDepartment distribution:')
    for d, c in sorted(dept_counts.items(), key=lambda x: -x[1]):
        print(f'  {d:40s} {c:>4d}')

    print(f'\nOutput: {OUTPUT_RAW}, {OUTPUT_SUMMARY}')


if __name__ == '__main__':
    main()
