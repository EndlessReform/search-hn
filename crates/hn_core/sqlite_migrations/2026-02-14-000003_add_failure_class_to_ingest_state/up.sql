ALTER TABLE ingest_segments
ADD COLUMN failure_class TEXT
CHECK (failure_class IS NULL OR failure_class IN ('network_transient', 'http_4xx', 'decode', 'schema'));

ALTER TABLE ingest_exceptions
ADD COLUMN failure_class TEXT
CHECK (failure_class IS NULL OR failure_class IN ('network_transient', 'http_4xx', 'decode', 'schema'));
