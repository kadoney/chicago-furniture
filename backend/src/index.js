/**
 * chicago-collection — Cloudflare Worker
 * SAPFM Art Institute of Chicago Furniture Explorer API
 *
 * D1 binding: AIC_DB (database: chicago-furniture)
 * Pattern: mirrors cleveland-collection Worker
 *
 * Key difference from Cleveland: images are IIIF — image_id stored in D1,
 * URLs derived here:
 *   Thumb: https://www.artic.edu/iiif/2/{image_id}/full/400,/0/default.jpg
 *   Full:  https://www.artic.edu/iiif/2/{image_id}/full/843,/0/default.jpg
 *
 * Endpoints:
 *   GET /search  — filtered furniture search
 *   GET /counts  — object counts per form_bucket, origin, department
 *   GET /health  — status + row count
 */

const VERSION    = 'v1';
const IIIF_BASE  = 'https://www.artic.edu/iiif/2';

const VALID_FORMS = new Set([
  'Chair', 'Table', 'Case Piece', 'Desk',
  'Bed', 'Stand', 'Sofa & Bench', 'Clock', 'Mirror',
]);

const DEFAULT_LIMIT = 50;
const MAX_LIMIT     = 200;

function iiifThumb(image_id) {
  return image_id ? `${IIIF_BASE}/${image_id}/full/400,/0/default.jpg` : null;
}
function iiifFull(image_id) {
  return image_id ? `${IIIF_BASE}/${image_id}/full/843,/0/default.jpg` : null;
}

function addImageUrls(row) {
  if (!row) return row;
  row.image_thumb_url = iiifThumb(row.image_id);
  row.image_full_url  = iiifFull(row.image_id);
  return row;
}

function jsonResponse(data, status = 200) {
  return new Response(JSON.stringify(data), {
    status,
    headers: {
      'Content-Type':                'application/json',
      'Access-Control-Allow-Origin': '*',
      'Cache-Control':               'public, max-age=300',
    },
  });
}

function errorResponse(msg, status = 500) {
  return jsonResponse({ error: msg }, status);
}

async function handleSearch(request, env) {
  const p      = Object.fromEntries(new URL(request.url).searchParams);
  const limit  = Math.min(MAX_LIMIT, Math.max(1, parseInt(p.limit)  || DEFAULT_LIMIT));
  const offset = Math.max(0, parseInt(p.offset) || 0);

  const where  = [];
  const params = [];

  // form_bucket filter
  if (p.form && VALID_FORMS.has(p.form)) {
    where.push('form_bucket = ?');
    params.push(p.form);
  }

  // origin filter
  if (p.origin) {
    where.push('origin = ?');
    params.push(p.origin);
  }

  // department filter
  if (p.department) {
    where.push('department = ?');
    params.push(p.department);
  }

  // date range
  if (p.date_from) {
    where.push('(date_end IS NULL OR date_end >= ?)');
    params.push(parseInt(p.date_from));
  }
  if (p.date_to) {
    where.push('(date_begin IS NULL OR date_begin <= ?)');
    params.push(parseInt(p.date_to));
  }

  // maker (partial match)
  if (p.maker) {
    where.push('LOWER(maker_name) LIKE ?');
    params.push(`%${p.maker.toLowerCase()}%`);
  }

  // free text — title, maker, medium
  if (p.q) {
    const term = `%${p.q}%`;
    where.push('(title LIKE ? OR LOWER(maker_display) LIKE ? OR LOWER(medium) LIKE ?)');
    params.push(term, term.toLowerCase(), term.toLowerCase());
  }

  const whereClause = where.length ? `WHERE ${where.join(' AND ')}` : '';

  const countSql = `SELECT COUNT(*) as total FROM furniture ${whereClause}`;

  const dataSql = `
    SELECT
      id, aic_id, accession,
      title, classification, department,
      form_bucket, form_type,
      maker_name, maker_display,
      origin, place,
      date_display, date_begin, date_end,
      medium, dimensions,
      creditline,
      image_id, alt_text,
      collection_url
    FROM furniture
    ${whereClause}
    ORDER BY
      CASE WHEN date_begin IS NOT NULL THEN 0 ELSE 1 END,
      date_begin ASC
    LIMIT ? OFFSET ?
  `;

  try {
    const [countResult, dataResult] = await Promise.all([
      env.AIC_DB.prepare(countSql).bind(...params).first(),
      env.AIC_DB.prepare(dataSql).bind(...params, limit, offset).all(),
    ]);

    const results = (dataResult.results ?? []).map(addImageUrls);

    return jsonResponse({
      total:   countResult?.total ?? 0,
      offset,
      limit,
      results,
    });
  } catch (err) {
    return errorResponse(`Search failed: ${err.message}`);
  }
}

async function handleCounts(env) {
  try {
    const [formResult, originResult, deptResult, totals] = await Promise.all([
      env.AIC_DB.prepare(`
        SELECT form_bucket, COUNT(*) as n
        FROM furniture
        GROUP BY form_bucket
        ORDER BY n DESC
      `).all(),
      env.AIC_DB.prepare(`
        SELECT origin, COUNT(*) as n
        FROM furniture
        WHERE origin IS NOT NULL
        GROUP BY origin
        ORDER BY n DESC
      `).all(),
      env.AIC_DB.prepare(`
        SELECT department, COUNT(*) as n
        FROM furniture
        WHERE department IS NOT NULL
        GROUP BY department
        ORDER BY n DESC
      `).all(),
      env.AIC_DB.prepare(`
        SELECT COUNT(*) as total
        FROM furniture
      `).first(),
    ]);

    const by_form = {};
    for (const row of formResult.results ?? []) {
      by_form[row.form_bucket ?? '(none)'] = row.n;
    }
    const by_origin = {};
    for (const row of originResult.results ?? []) {
      by_origin[row.origin] = row.n;
    }
    const by_department = {};
    for (const row of deptResult.results ?? []) {
      by_department[row.department] = row.n;
    }

    return jsonResponse({
      total: totals?.total ?? 0,
      by_form,
      by_origin,
      by_department,
    });
  } catch (err) {
    return errorResponse(`Counts failed: ${err.message}`);
  }
}

async function handleHealth(env) {
  try {
    const result = await env.AIC_DB.prepare(
      'SELECT COUNT(*) as n FROM furniture'
    ).first();
    return jsonResponse({
      status:  'ok',
      version: VERSION,
      db:      'chicago-furniture',
      objects: result?.n ?? 0,
    });
  } catch (err) {
    return errorResponse(`Health check failed: ${err.message}`);
  }
}

export default {
  async fetch(request, env) {
    const { pathname } = new URL(request.url);

    if (request.method === 'OPTIONS') {
      return new Response(null, {
        headers: {
          'Access-Control-Allow-Origin':  '*',
          'Access-Control-Allow-Methods': 'GET, OPTIONS',
          'Access-Control-Allow-Headers': 'Content-Type',
        },
      });
    }

    if (pathname === '/search') return handleSearch(request, env);
    if (pathname === '/counts') return handleCounts(env);
    if (pathname === '/health') return handleHealth(env);

    return jsonResponse({
      worker:    'chicago-collection v1',
      version:   VERSION,
      endpoints: {
        '/health':  'Status + object count',
        '/counts':  'Object counts by form_bucket, origin, department',
        '/search':  [
          '?form=Chair|Table|Case Piece|Desk|Bed|Stand|Sofa & Bench|Clock|Mirror',
          '&origin=New York|Philadelphia|Boston|France|...',
          '&department=Arts of the Americas|Applied Arts of Europe',
          '&date_from=1600&date_to=1800',
          '&maker=herter',
          '&q=free text (title, maker, medium)',
          '&limit=50&offset=0',
        ].join(' '),
      },
    });
  },
};
