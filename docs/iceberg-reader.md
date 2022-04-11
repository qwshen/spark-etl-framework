

Reader options:
snapshot-id: Snapshot ID of the table snapshot to read. Default: (latest)
as-of-timestamp	(latest)	A timestamp in milliseconds; the snapshot used will be the snapshot current at this time.
split-size	As per table property	Overrides this table’s read.split.target-size and read.split.metadata-target-size
lookback	As per table property	Overrides this table’s read.split.planning-lookback
file-open-cost	As per table property	Overrides this table’s read.split.open-file-cost
vectorization-enabled	As per table property	Overrides this table’s read.parquet.vectorization.enabled
batch-size	As per table property	Overrides this table’s read.parquet.vectorization.batch-size
stream-from-timestamp	(none)	A timestamp in milliseconds to stream from; if before the oldest known ancestor snapshot, the oldest will be used