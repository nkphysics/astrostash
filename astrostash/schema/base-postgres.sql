CREATE TABLE IF NOT EXISTS queries (
    id SERIAL PRIMARY KEY,
    hash TEXT NOT NULL UNIQUE,
    last_refreshed DATE,
    refresh_rate INTEGER
);

CREATE TABLE IF NOT EXISTS responses (
    id SERIAL PRIMARY KEY,
    hash TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS query_response_pivot (
    queryid INTEGER REFERENCES queries(id),
    responseid INTEGER REFERENCES responses(id),
    UNIQUE (queryid, responseid)
);

CREATE TABLE IF NOT EXISTS response_rowid_pivot (
    responseid INTEGER REFERENCES responses(id),
    rowid TEXT,
    UNIQUE (responseid, rowid)
);

CREATE TABLE IF NOT EXISTS local_data_paths (
    id SERIAL PRIMARY KEY,
    catalog TEXT NOT NULL,
    rowid TEXT NOT NULL,
    location TEXT NOT NULL,
    UNIQUE (catalog, rowid, location)
);

CREATE INDEX IF NOT EXISTS idx_query_response ON query_response_pivot(queryid);
CREATE INDEX IF NOT EXISTS idx_query_response_id ON query_response_pivot(responseid);
CREATE INDEX IF NOT EXISTS idx_response_rowid ON response_rowid_pivot(rowid);
