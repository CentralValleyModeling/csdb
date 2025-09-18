CREATE TABLE IF NOT EXISTS variable (
        id INTEGER,
        name VARCHAR NOT NULL, 
        code_name VARCHAR NOT NULL, 
        kind VARCHAR NOT NULL, 
        units VARCHAR NOT NULL,
        PRIMARY KEY (id),
        CONSTRAINT unique_variable UNIQUE (code_name)
);
CREATE SEQUENCE seq_variable START 1;