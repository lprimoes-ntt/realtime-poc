USE SourceDB;
GO

IF OBJECT_ID('dbo.customers', 'U') IS NULL
BEGIN
  CREATE TABLE dbo.customers (
    customer_id INT IDENTITY(1,1) NOT NULL,
    segment NVARCHAR(50) NOT NULL,
    is_active BIT NOT NULL CONSTRAINT DF_customers_is_active DEFAULT (1),
    updated_at DATETIME2 NOT NULL CONSTRAINT DF_customers_updated_at DEFAULT (SYSUTCDATETIME()),
    CONSTRAINT PK_customers PRIMARY KEY (customer_id)
  );
END
GO

IF OBJECT_ID('dbo.orders', 'U') IS NULL
BEGIN
  CREATE TABLE dbo.orders (
    order_id BIGINT IDENTITY(1,1) NOT NULL,
    customer_id INT NOT NULL,
    amount DECIMAL(18,2) NOT NULL,
    status NVARCHAR(20) NOT NULL,
    created_at DATETIME2 NOT NULL CONSTRAINT DF_orders_created_at DEFAULT (SYSUTCDATETIME()),
    updated_at DATETIME2 NOT NULL CONSTRAINT DF_orders_updated_at DEFAULT (SYSUTCDATETIME()),
    CONSTRAINT PK_orders PRIMARY KEY (order_id)
  );

  CREATE INDEX IX_orders_customer_id ON dbo.orders(customer_id);
  CREATE INDEX IX_orders_created_at ON dbo.orders(created_at);
END
GO

IF OBJECT_ID('dbo.payments', 'U') IS NULL
BEGIN
  CREATE TABLE dbo.payments (
    payment_id BIGINT IDENTITY(1,1) NOT NULL,
    order_id BIGINT NOT NULL,
    paid_amount DECIMAL(18,2) NOT NULL,
    paid_at DATETIME2 NOT NULL CONSTRAINT DF_payments_paid_at DEFAULT (SYSUTCDATETIME()),
    CONSTRAINT PK_payments PRIMARY KEY (payment_id)
  );

  CREATE INDEX IX_payments_order_id ON dbo.payments(order_id);
  CREATE INDEX IX_payments_paid_at ON dbo.payments(paid_at);
END
GO
