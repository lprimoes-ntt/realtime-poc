USE ServingDB;
GO

DELETE FROM dbo.proj_orders_kpi_by_minute_segment;
DELETE FROM dbo.proj_orders_latest;

UPDATE dbo.ctl_ingestion_watermarks
SET last_ingested_lsn = 0x00000000000000000000,
    updated_at = SYSUTCDATETIME();

UPDATE dbo.ctl_projection_checkpoints
SET last_consumed_lsn = 0x00000000000000000000,
    updated_at = SYSUTCDATETIME();

UPDATE dbo.ctl_projection_metadata
SET as_of_lsn = NULL,
    as_of_time = NULL,
    built_at = NULL,
    status = 'INIT',
    last_error = NULL;
GO
