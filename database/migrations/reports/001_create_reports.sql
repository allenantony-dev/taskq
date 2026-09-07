CREATE TABLE reports (
    job_id BIGINT PRIMARY KEY,
    content TEXT NOT NULL,
    fencing_token BIGINT NOT NULL
);
