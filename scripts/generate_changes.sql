USE SourceDB;
GO

DECLARE @customers_is_identity BIT = CASE WHEN COLUMNPROPERTY(OBJECT_ID('dbo.customers'), 'customer_id', 'IsIdentity') = 1 THEN 1 ELSE 0 END;
DECLARE @orders_is_identity BIT = CASE WHEN COLUMNPROPERTY(OBJECT_ID('dbo.orders'), 'order_id', 'IsIdentity') = 1 THEN 1 ELSE 0 END;
DECLARE @payments_is_identity BIT = CASE WHEN COLUMNPROPERTY(OBJECT_ID('dbo.payments'), 'payment_id', 'IsIdentity') = 1 THEN 1 ELSE 0 END;

DECLARE @new_customer_id INT;
DECLARE @new_order_id BIGINT;
DECLARE @existing_order_id BIGINT;
DECLARE @existing_open_order_id BIGINT;

-- New customer
IF @customers_is_identity = 1
BEGIN
  DECLARE @new_customer TABLE (customer_id INT);
  INSERT INTO dbo.customers (segment, is_active, updated_at)
  OUTPUT INSERTED.customer_id INTO @new_customer(customer_id)
  VALUES (N'Enterprise', 1, SYSUTCDATETIME());

  SELECT TOP 1 @new_customer_id = customer_id FROM @new_customer;
END
ELSE
BEGIN
  SELECT @new_customer_id = ISNULL(MAX(customer_id), 0) + 1
  FROM dbo.customers;

  INSERT INTO dbo.customers (customer_id, segment, is_active, updated_at)
  VALUES (@new_customer_id, N'Enterprise', 1, SYSUTCDATETIME());
END

-- New order for that customer
IF @orders_is_identity = 1
BEGIN
  DECLARE @new_order TABLE (order_id BIGINT);
  INSERT INTO dbo.orders (customer_id, amount, status, created_at, updated_at)
  OUTPUT INSERTED.order_id INTO @new_order(order_id)
  VALUES (@new_customer_id, 720.00, N'open', SYSUTCDATETIME(), SYSUTCDATETIME());

  SELECT TOP 1 @new_order_id = order_id FROM @new_order;
END
ELSE
BEGIN
  SELECT @new_order_id = ISNULL(MAX(order_id), 0) + 1
  FROM dbo.orders;

  INSERT INTO dbo.orders (order_id, customer_id, amount, status, created_at, updated_at)
  VALUES (@new_order_id, @new_customer_id, 720.00, N'open', SYSUTCDATETIME(), SYSUTCDATETIME());
END

-- Update an existing order (if available)
SELECT TOP 1 @existing_order_id = order_id
FROM dbo.orders
ORDER BY order_id;

IF @existing_order_id IS NOT NULL
BEGIN
  UPDATE dbo.orders
  SET amount = amount + 55.00,
      status = N'paid',
      updated_at = SYSUTCDATETIME()
  WHERE order_id = @existing_order_id;

  IF @payments_is_identity = 1
  BEGIN
    INSERT INTO dbo.payments (order_id, paid_amount, paid_at)
    VALUES (@existing_order_id, 175.00, SYSUTCDATETIME());
  END
  ELSE
  BEGIN
    DECLARE @new_payment_id BIGINT;
    SELECT @new_payment_id = ISNULL(MAX(payment_id), 0) + 1
    FROM dbo.payments;

    INSERT INTO dbo.payments (payment_id, order_id, paid_amount, paid_at)
    VALUES (@new_payment_id, @existing_order_id, 175.00, SYSUTCDATETIME());
  END
END

-- Segment update on an existing customer
UPDATE c
SET c.segment = N'MidMarket',
    c.updated_at = SYSUTCDATETIME()
FROM dbo.customers c
WHERE c.customer_id = (
  SELECT TOP 1 customer_id
  FROM dbo.customers
  WHERE is_active = 1
  ORDER BY customer_id
);

-- Demonstrate delete handling, but avoid deleting the newly inserted order
SELECT TOP 1 @existing_open_order_id = order_id
FROM dbo.orders
WHERE status = N'open' AND order_id <> @new_order_id
ORDER BY order_id;

IF @existing_open_order_id IS NOT NULL
BEGIN
  DELETE FROM dbo.orders
  WHERE order_id = @existing_open_order_id;
END
GO
