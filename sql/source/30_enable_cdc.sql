USE SourceDB;
GO

IF (SELECT is_cdc_enabled FROM sys.databases WHERE name = 'SourceDB') = 0
BEGIN
  EXEC sys.sp_cdc_enable_db;
END
GO

IF OBJECT_ID('dbo.customers', 'U') IS NOT NULL
AND NOT EXISTS (
  SELECT 1
  FROM cdc.change_tables
  WHERE source_object_id = OBJECT_ID('dbo.customers')
)
BEGIN
  EXEC sys.sp_cdc_enable_table
    @source_schema = N'dbo',
    @source_name = N'customers',
    @role_name = NULL,
    @supports_net_changes = 0;
END
GO

IF OBJECT_ID('dbo.orders', 'U') IS NOT NULL
AND NOT EXISTS (
  SELECT 1
  FROM cdc.change_tables
  WHERE source_object_id = OBJECT_ID('dbo.orders')
)
BEGIN
  EXEC sys.sp_cdc_enable_table
    @source_schema = N'dbo',
    @source_name = N'orders',
    @role_name = NULL,
    @supports_net_changes = 0;
END
GO

IF OBJECT_ID('dbo.payments', 'U') IS NOT NULL
AND NOT EXISTS (
  SELECT 1
  FROM cdc.change_tables
  WHERE source_object_id = OBJECT_ID('dbo.payments')
)
BEGIN
  EXEC sys.sp_cdc_enable_table
    @source_schema = N'dbo',
    @source_name = N'payments',
    @role_name = NULL,
    @supports_net_changes = 0;
END
GO

SELECT
  OBJECT_ID('cdc.fn_cdc_get_all_changes_dbo_customers') AS fn_customers,
  OBJECT_ID('cdc.fn_cdc_get_all_changes_dbo_orders') AS fn_orders,
  OBJECT_ID('cdc.fn_cdc_get_all_changes_dbo_payments') AS fn_payments;
GO
