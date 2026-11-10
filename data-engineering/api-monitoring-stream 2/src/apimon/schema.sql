CREATE TABLE IF NOT EXISTS service_windows (
    window_start   timestamptz NOT NULL,
    window_end     timestamptz NOT NULL,
    service        text        NOT NULL,
    requests       bigint      NOT NULL,
    errors         bigint      NOT NULL,
    error_rate     double precision,
    avg_latency_ms double precision,
    p95_latency_ms double precision,
    PRIMARY KEY (service, window_start)
);
CREATE TABLE IF NOT EXISTS alerts (
    window_start timestamptz NOT NULL,
    service      text        NOT NULL,
    rule         text        NOT NULL,
    value        double precision,
    threshold    double precision,
    PRIMARY KEY (service, window_start, rule)
);
