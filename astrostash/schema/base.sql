CREATE TABLE IF NOT EXISTS queries (
    id INTEGER PRIMARY KEY,
    hash TEXT NOT NULL,
    last_refreshed DATE,
    refresh_rate INTEGER,
    UNIQUE (hash)
);

CREATE TABLE IF NOT EXISTS responses (
    id INTEGER PRIMARY KEY,
    hash TEXT NOT NULL,
    UNIQUE (hash)
);

CREATE TABLE IF NOT EXISTS query_response_pivot (
    queryid INTEGER,
    responseid INTEGER,
    FOREIGN KEY (queryid) REFERENCES queries(id),
    FOREIGN KEY (responseid) REFERENCES responses(id),
    UNIQUE (queryid, responseid)
);

CREATE TABLE IF NOT EXISTS response_rowid_pivot (
    responseid INTEGER,
    rowid TEXT,
    FOREIGN KEY (responseid) REFERENCES responses(id),
    UNIQUE (responseid, rowid)
);

CREATE TABLE IF NOT EXISTS local_data_paths (
    id INTEGER PRIMARY KEY,
    catalog TEXT NOT NULL,
    rowid TEXT NOT NULL,
    location TEXT NOT NULL,
    UNIQUE (catalog, rowid, location)
);
