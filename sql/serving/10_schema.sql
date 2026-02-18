USE ServingDB;
GO

IF OBJECT_ID('dbo.ctl_ingestion_watermarks', 'U') IS NULL
BEGIN
  CREATE TABLE dbo.ctl_ingestion_watermarks (
    source_name NVARCHAR(100) NOT NULL,
    capture_instance NVARCHAR(128) NOT NULL,
    last_ingested_lsn VARBINARY(10) NOT NULL,
    updated_at DATETIME2 NOT NULL CONSTRAINT DF_ctl_ingestion_watermarks_updated_at DEFAULT (SYSUTCDATETIME()),
    CONSTRAINT PK_ctl_ingestion_watermarks PRIMARY KEY (source_name, capture_instance)
  );
END
GO

IF OBJECT_ID('dbo.ctl_projection_checkpoints', 'U') IS NULL
BEGIN
  CREATE TABLE dbo.ctl_projection_checkpoints (
    projection_name NVARCHAR(128) NOT NULL,
    capture_instance NVARCHAR(128) NOT NULL,
    last_consumed_lsn VARBINARY(10) NOT NULL,
    updated_at DATETIME2 NOT NULL CONSTRAINT DF_ctl_projection_checkpoints_updated_at DEFAULT (SYSUTCDATETIME()),
    CONSTRAINT PK_ctl_projection_checkpoints PRIMARY KEY (projection_name, capture_instance)
  );
END
GO

IF OBJECT_ID('dbo.ctl_projection_metadata', 'U') IS NULL
BEGIN
  CREATE TABLE dbo.ctl_projection_metadata (
    projection_name NVARCHAR(128) NOT NULL,
    as_of_lsn VARBINARY(10) NULL,
    as_of_time DATETIME2 NULL,
    built_at DATETIME2 NULL,
    status NVARCHAR(20) NOT NULL CONSTRAINT DF_ctl_projection_metadata_status DEFAULT ('INIT'),
    last_error NVARCHAR(4000) NULL,
    CONSTRAINT PK_ctl_projection_metadata PRIMARY KEY (projection_name)
  );
END
GO

IF OBJECT_ID('dbo.stg_cdc_customers', 'U') IS NULL
BEGIN
  CREATE TABLE dbo.stg_cdc_customers (
    lsn VARBINARY(10) NOT NULL,
    seqval VARBINARY(10) NOT NULL,
    op TINYINT NOT NULL,
    customer_id INT NOT NULL,
    segment NVARCHAR(50) NULL,
    is_active BIT NULL,
    updated_at DATETIME2 NULL,
    ingested_at DATETIME2 NOT NULL CONSTRAINT DF_stg_cdc_customers_ingested_at DEFAULT (SYSUTCDATETIME()),
    CONSTRAINT UQ_stg_cdc_customers UNIQUE (lsn, seqval, customer_id)
  );

  CREATE INDEX IX_stg_cdc_customers_lsn ON dbo.stg_cdc_customers(lsn, seqval);
  CREATE INDEX IX_stg_cdc_customers_customer ON dbo.stg_cdc_customers(customer_id, lsn DESC, seqval DESC);
  CREATE INDEX IX_stg_cdc_customers_ingested_at ON dbo.stg_cdc_customers(ingested_at);
END
GO

IF OBJECT_ID('dbo.stg_cdc_customers', 'U') IS NOT NULL
AND NOT EXISTS (
  SELECT 1
  FROM sys.indexes
  WHERE name = 'IX_stg_cdc_customers_ingested_at'
    AND object_id = OBJECT_ID('dbo.stg_cdc_customers')
)
BEGIN
  CREATE INDEX IX_stg_cdc_customers_ingested_at ON dbo.stg_cdc_customers(ingested_at);
END
GO

IF OBJECT_ID('dbo.stg_cdc_orders', 'U') IS NULL
BEGIN
  CREATE TABLE dbo.stg_cdc_orders (
    lsn VARBINARY(10) NOT NULL,
    seqval VARBINARY(10) NOT NULL,
    op TINYINT NOT NULL,
    order_id BIGINT NOT NULL,
    customer_id INT NULL,
    amount DECIMAL(18,2) NULL,
    status NVARCHAR(20) NULL,
    created_at DATETIME2 NULL,
    updated_at DATETIME2 NULL,
    ingested_at DATETIME2 NOT NULL CONSTRAINT DF_stg_cdc_orders_ingested_at DEFAULT (SYSUTCDATETIME()),
    CONSTRAINT UQ_stg_cdc_orders UNIQUE (lsn, seqval, order_id)
  );

  CREATE INDEX IX_stg_cdc_orders_lsn ON dbo.stg_cdc_orders(lsn, seqval);
  CREATE INDEX IX_stg_cdc_orders_order ON dbo.stg_cdc_orders(order_id, lsn DESC, seqval DESC);
  CREATE INDEX IX_stg_cdc_orders_created_at ON dbo.stg_cdc_orders(created_at);
  CREATE INDEX IX_stg_cdc_orders_ingested_at ON dbo.stg_cdc_orders(ingested_at);
END
GO

IF OBJECT_ID('dbo.stg_cdc_orders', 'U') IS NOT NULL
AND NOT EXISTS (
  SELECT 1
  FROM sys.indexes
  WHERE name = 'IX_stg_cdc_orders_ingested_at'
    AND object_id = OBJECT_ID('dbo.stg_cdc_orders')
)
BEGIN
  CREATE INDEX IX_stg_cdc_orders_ingested_at ON dbo.stg_cdc_orders(ingested_at);
END
GO

IF OBJECT_ID('dbo.stg_cdc_payments', 'U') IS NULL
BEGIN
  CREATE TABLE dbo.stg_cdc_payments (
    lsn VARBINARY(10) NOT NULL,
    seqval VARBINARY(10) NOT NULL,
    op TINYINT NOT NULL,
    payment_id BIGINT NOT NULL,
    order_id BIGINT NULL,
    paid_amount DECIMAL(18,2) NULL,
    paid_at DATETIME2 NULL,
    ingested_at DATETIME2 NOT NULL CONSTRAINT DF_stg_cdc_payments_ingested_at DEFAULT (SYSUTCDATETIME()),
    CONSTRAINT UQ_stg_cdc_payments UNIQUE (lsn, seqval, payment_id)
  );

  CREATE INDEX IX_stg_cdc_payments_lsn ON dbo.stg_cdc_payments(lsn, seqval);
  CREATE INDEX IX_stg_cdc_payments_payment ON dbo.stg_cdc_payments(payment_id, lsn DESC, seqval DESC);
  CREATE INDEX IX_stg_cdc_payments_paid_at ON dbo.stg_cdc_payments(paid_at);
  CREATE INDEX IX_stg_cdc_payments_ingested_at ON dbo.stg_cdc_payments(ingested_at);
END
GO

IF OBJECT_ID('dbo.stg_cdc_payments', 'U') IS NOT NULL
AND NOT EXISTS (
  SELECT 1
  FROM sys.indexes
  WHERE name = 'IX_stg_cdc_payments_ingested_at'
    AND object_id = OBJECT_ID('dbo.stg_cdc_payments')
)
BEGIN
  CREATE INDEX IX_stg_cdc_payments_ingested_at ON dbo.stg_cdc_payments(ingested_at);
END
GO

IF OBJECT_ID('dbo.proj_orders_kpi_by_minute_segment', 'U') IS NULL
BEGIN
  CREATE TABLE dbo.proj_orders_kpi_by_minute_segment (
    minute_bucket DATETIME2 NOT NULL,
    segment NVARCHAR(50) NOT NULL,
    orders_count INT NOT NULL,
    orders_amount_sum DECIMAL(18,2) NOT NULL,
    paid_amount_sum DECIMAL(18,2) NOT NULL,
    CONSTRAINT PK_proj_orders_kpi PRIMARY KEY (minute_bucket, segment)
  );

  CREATE INDEX IX_proj_orders_kpi_minute ON dbo.proj_orders_kpi_by_minute_segment(minute_bucket);
END
GO

IF OBJECT_ID('dbo.proj_orders_latest', 'U') IS NULL
BEGIN
  CREATE TABLE dbo.proj_orders_latest (
    order_id BIGINT NOT NULL,
    customer_id INT NULL,
    segment NVARCHAR(50) NULL,
    amount DECIMAL(18,2) NULL,
    status NVARCHAR(20) NULL,
    created_at DATETIME2 NULL,
    updated_at DATETIME2 NULL,
    __source_lsn VARBINARY(10) NOT NULL,
    CONSTRAINT PK_proj_orders_latest PRIMARY KEY (order_id)
  );

  CREATE INDEX IX_proj_orders_latest_segment ON dbo.proj_orders_latest(segment);
END
GO
