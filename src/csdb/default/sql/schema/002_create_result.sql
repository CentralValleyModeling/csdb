CREATE TABLE IF NOT EXISTS result (
        datetime DATE NOT NULL, 
        value FLOAT NOT NULL, 
        run_id INTEGER NOT NULL, 
        variable_id INTEGER NOT NULL, 
        CONSTRAINT unique_datapoint UNIQUE (run_id, variable_id, datetime), 
        FOREIGN KEY(run_id) REFERENCES run (id), 
        FOREIGN KEY(variable_id) REFERENCES variable (id)
);
CREATE SEQUENCE seq_result START 1;