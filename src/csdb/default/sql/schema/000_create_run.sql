CREATE TABLE IF NOT EXISTS run (
        id INTEGER,
        name VARCHAR NOT NULL, 
        source VARCHAR NOT NULL,
        PRIMARY KEY (id), 
        UNIQUE (name)
);
CREATE SEQUENCE seq_run START 1;