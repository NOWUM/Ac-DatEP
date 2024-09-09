CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

CREATE EXTENSION IF NOT EXISTS postgis CASCADE;

CREATE TABLE IF NOT EXISTS sensors (
    id              BIGSERIAL           UNIQUE,
    ex_id           TEXT                NOT NULL,
    source          TEXT                NOT NULL,
    longitude       DOUBLE PRECISION    NOT NULL,
    latitude        DOUBLE PRECISION    NOT NULL,
    geometry        GEOMETRY            NOT NULL,
    description     TEXT,
    confidential    BOOLEAN             DEFAULT true,
    PRIMARY KEY     (ex_id, source)
);

CREATE TABLE IF NOT EXISTS datastreams(
    id              BIGSERIAL   UNIQUE,
    ex_id           TEXT        NOT NULL,
    sensor_id       INTEGER     REFERENCES  sensors (id),
    type            TEXT        NOT NULL,
    unit            TEXT        NOT NULL,
    confidential    BOOLEAN     DEFAULT true,
    PRIMARY KEY     (sensor_id, type)
);

CREATE TABLE IF NOT EXISTS measurements(
    datastream_id   INTEGER             REFERENCES datastreams(id),
    timestamp       TIMESTAMPTZ         NOT NULL,
    value           DOUBLE PRECISION    NOT NULL,
    confidential    BOOLEAN             DEFAULT true,
    PRIMARY KEY     (datastream_id, timestamp)
);

CREATE TABLE IF NOT EXISTS users(
    id              BIGSERIAL   UNIQUE,
    username        TEXT        UNIQUE  NOT NULL,
    hashed_password TEXT        NOT NULL,
    role            TEXT        NOT NULL
);

CREATE MATERIALIZED VIEW latest_measurements AS (
    SELECT
        meas.datastream_id,
        meas."timestamp",
        meas.value,
        meas.confidential
    FROM measurements AS meas
    JOIN (
        SELECT
            measurements.datastream_id AS max_datastream_id,
            max(measurements."timestamp") AS max_timestamp
        FROM measurements
        GROUP BY measurements.datastream_id) AS max_ts
    ON meas.datastream_id = max_ts.max_datastream_id AND meas."timestamp" = max_ts.max_timestamp);

CREATE UNIQUE INDEX index_latest_measurements ON latest_measurements (datastream_id, timestamp);
