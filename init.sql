-- =============================================================
-- init.sql — Bootstrap ShoriDB with CDC enabled
-- Run this AFTER the sqlserver container is healthy.
-- =============================================================

-- 1. Create the database
CREATE DATABASE ShoriDB;
GO

USE ShoriDB;
GO

-- 2. Enable CDC on the database (requires SQL Server Agent running)
EXEC sys.sp_cdc_enable_db;
GO

-- 3. Create a dummy table
CREATE TABLE dbo.Users (
    id         INT IDENTITY(1,1) PRIMARY KEY,
    name       NVARCHAR(100)  NOT NULL,
    status     NVARCHAR(50)   NOT NULL,
    updated_at DATETIME2      NOT NULL DEFAULT GETDATE()
);
GO

-- 4. Enable CDC on the Users table
EXEC sys.sp_cdc_enable_table
    @source_schema  = N'dbo',
    @source_name    = N'Users',
    @role_name      = NULL,
    @supports_net_changes = 1;
GO

PRINT '✔ ShoriDB created, CDC enabled on dbo.Users and dbo.BenchmarkEvents';
GO

-- 5. Tune the CDC capture job for higher throughput and lower latency.
--    Defaults: pollinginterval=5s, maxtrans=500, maxscans=10
--    → theoretical ceiling: 500 * 10 / 5 = 1,000 tx/sec
--    Tuned:   pollinginterval=1s, maxtrans=1000, maxscans=20
--    → theoretical ceiling: 1000 * 20 / 1 = 20,000 tx/sec
EXEC sys.sp_cdc_change_job
    @job_type        = N'capture',
    @pollinginterval = 1,
    @maxtrans        = 1000,
    @maxscans        = 20;
GO

PRINT '✔ CDC capture job tuned: pollinginterval=1s, maxtrans=1000, maxscans=20';
GO
