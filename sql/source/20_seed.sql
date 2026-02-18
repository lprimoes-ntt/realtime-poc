USE SourceDB;
GO

DECLARE @customers_is_identity BIT = CASE WHEN COLUMNPROPERTY(OBJECT_ID('dbo.customers'), 'customer_id', 'IsIdentity') = 1 THEN 1 ELSE 0 END;
DECLARE @orders_is_identity BIT = CASE WHEN COLUMNPROPERTY(OBJECT_ID('dbo.orders'), 'order_id', 'IsIdentity') = 1 THEN 1 ELSE 0 END;
DECLARE @payments_is_identity BIT = CASE WHEN COLUMNPROPERTY(OBJECT_ID('dbo.payments'), 'payment_id', 'IsIdentity') = 1 THEN 1 ELSE 0 END;

IF NOT EXISTS (SELECT 1 FROM dbo.customers)
BEGIN
  IF @customers_is_identity = 1
  BEGIN
    INSERT INTO dbo.customers (segment, is_active, updated_at)
    VALUES
      (N'SMB', 1, SYSUTCDATETIME()),
      (N'Enterprise', 1, SYSUTCDATETIME()),
      (N'SMB', 1, SYSUTCDATETIME()),
      (N'MidMarket', 1, SYSUTCDATETIME());
  END
  ELSE
  BEGIN
    INSERT INTO dbo.customers (customer_id, segment, is_active, updated_at)
    VALUES
      (1, N'SMB', 1, SYSUTCDATETIME()),
      (2, N'Enterprise', 1, SYSUTCDATETIME()),
      (3, N'SMB', 1, SYSUTCDATETIME()),
      (4, N'MidMarket', 1, SYSUTCDATETIME());
  END
END

IF NOT EXISTS (SELECT 1 FROM dbo.orders)
BEGIN
  DECLARE @customer_smb_1 INT = (
    SELECT TOP 1 customer_id
    FROM dbo.customers
    WHERE segment = N'SMB'
    ORDER BY customer_id
  );
  DECLARE @customer_enterprise INT = (
    SELECT TOP 1 customer_id
    FROM dbo.customers
    WHERE segment = N'Enterprise'
    ORDER BY customer_id
  );
  DECLARE @customer_smb_2 INT = (
    SELECT TOP 1 customer_id
    FROM dbo.customers
    WHERE segment = N'SMB' AND customer_id <> @customer_smb_1
    ORDER BY customer_id
  );

  IF @orders_is_identity = 1
  BEGIN
    INSERT INTO dbo.orders (customer_id, amount, status, created_at, updated_at)
    VALUES
      (@customer_smb_1, 120.00, N'open', DATEADD(MINUTE, -12, SYSUTCDATETIME()), DATEADD(MINUTE, -12, SYSUTCDATETIME())),
      (@customer_enterprise, 950.00, N'paid', DATEADD(MINUTE, -9, SYSUTCDATETIME()), DATEADD(MINUTE, -6, SYSUTCDATETIME())),
      (@customer_smb_2, 300.00, N'open', DATEADD(MINUTE, -5, SYSUTCDATETIME()), DATEADD(MINUTE, -5, SYSUTCDATETIME()));
  END
  ELSE
  BEGIN
    DECLARE @next_order_id BIGINT = ISNULL((SELECT MAX(order_id) FROM dbo.orders), 0) + 1;

    INSERT INTO dbo.orders (order_id, customer_id, amount, status, created_at, updated_at)
    VALUES
      (@next_order_id, @customer_smb_1, 120.00, N'open', DATEADD(MINUTE, -12, SYSUTCDATETIME()), DATEADD(MINUTE, -12, SYSUTCDATETIME())),
      (@next_order_id + 1, @customer_enterprise, 950.00, N'paid', DATEADD(MINUTE, -9, SYSUTCDATETIME()), DATEADD(MINUTE, -6, SYSUTCDATETIME())),
      (@next_order_id + 2, @customer_smb_2, 300.00, N'open', DATEADD(MINUTE, -5, SYSUTCDATETIME()), DATEADD(MINUTE, -5, SYSUTCDATETIME()));
  END
END

IF NOT EXISTS (SELECT 1 FROM dbo.payments)
BEGIN
  DECLARE @paid_order_id BIGINT = (
    SELECT TOP 1 order_id
    FROM dbo.orders
    WHERE status = N'paid'
    ORDER BY created_at
  );

  IF @payments_is_identity = 1
  BEGIN
    INSERT INTO dbo.payments (order_id, paid_amount, paid_at)
    VALUES (@paid_order_id, 950.00, DATEADD(MINUTE, -6, SYSUTCDATETIME()));
  END
  ELSE
  BEGIN
    DECLARE @next_payment_id BIGINT = ISNULL((SELECT MAX(payment_id) FROM dbo.payments), 0) + 1;

    INSERT INTO dbo.payments (payment_id, order_id, paid_amount, paid_at)
    VALUES (@next_payment_id, @paid_order_id, 950.00, DATEADD(MINUTE, -6, SYSUTCDATETIME()));
  END
END
GO
