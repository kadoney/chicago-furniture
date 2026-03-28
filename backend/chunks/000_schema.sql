-- chicago-furniture D1 schema
-- Worker: chicago-collection
-- Binding: AIC_DB
-- Data source: Art Institute of Chicago API (CC0 data, public domain images)
-- ~457 public-domain furniture objects harvested

-- Drop and recreate for clean import
DROP TABLE IF EXISTS furniture;

CREATE TABLE furniture (
  id              INTEGER PRIMARY KEY AUTOINCREMENT,

  -- Identity
  aic_id          INTEGER UNIQUE,      -- AIC object ID (dedup key)
  accession       TEXT,                -- museum accession number e.g. "1986.26"

  -- Display title
  title           TEXT NOT NULL,

  -- Classification
  classification  TEXT,                -- artwork_type_title e.g. "Furniture"
  department      TEXT,                -- e.g. "Arts of the Americas", "Applied Arts of Europe"
  form_bucket     TEXT,                -- normalized UI category: Chair, Table, Case Piece, etc.
  form_type       TEXT,                -- derived from title e.g. "side chair", "bureau table"

  -- Maker
  maker_name      TEXT,                -- first line of artist_display e.g. "Herter Brothers (American, 1864–1906)"
  maker_display   TEXT,                -- full artist_display (lines joined with "; ")

  -- Origin / Place
  origin          TEXT,                -- top-level origin: New York, Philadelphia, France, etc.
  place           TEXT,                -- full place_of_origin string

  -- Date
  date_display    TEXT,                -- human-readable e.g. "c. 1770" or "1878–80"
  date_begin      INTEGER,             -- numeric begin year for range filter
  date_end        INTEGER,             -- numeric end year for range filter

  -- Materials
  medium          TEXT,                -- medium_display e.g. "Mahogany, chestnut, and tulip poplar"

  -- Physical
  dimensions      TEXT,                -- e.g. "134.6 × 180.3 × 40.6 cm (53 × 71 × 16 in.)"

  -- Credit
  creditline      TEXT,                -- acquisition credit line

  -- Image — IIIF key stored; URLs derived at serve time
  -- Thumb:  https://www.artic.edu/iiif/2/{image_id}/full/400,/0/default.jpg
  -- Full:   https://www.artic.edu/iiif/2/{image_id}/full/843,/0/default.jpg
  image_id        TEXT,                -- UUID e.g. "63610c6d-4b70-6623-6069-c86a6eeff766"
  alt_text        TEXT,                -- thumbnail.alt_text for accessibility

  -- Links
  collection_url  TEXT                 -- https://www.artic.edu/artworks/{aic_id}
);

-- Indexes matching expected query patterns
CREATE INDEX IF NOT EXISTS idx_form_bucket  ON furniture(form_bucket);
CREATE INDEX IF NOT EXISTS idx_form_type    ON furniture(form_type);
CREATE INDEX IF NOT EXISTS idx_origin       ON furniture(origin);
CREATE INDEX IF NOT EXISTS idx_department   ON furniture(department);
CREATE INDEX IF NOT EXISTS idx_date_begin   ON furniture(date_begin);
CREATE INDEX IF NOT EXISTS idx_date_end     ON furniture(date_end);
CREATE INDEX IF NOT EXISTS idx_maker_name   ON furniture(maker_name);
CREATE INDEX IF NOT EXISTS idx_accession    ON furniture(accession);
