-- =============================================================
-- Bootstrap ShoriDB_3 with CDC enabled
-- =============================================================

CREATE DATABASE ShoriDB_3;
GO

USE ShoriDB_3;
GO

EXEC sys.sp_cdc_enable_db;
GO

CREATE TABLE dbo.Customers (
    id         INT IDENTITY(1,1) PRIMARY KEY,
    name       NVARCHAR(100)  NOT NULL,
    industry   NVARCHAR(50),
    created_at DATETIME2      NOT NULL DEFAULT GETDATE()
);
GO

CREATE TABLE dbo.Users (
    id         INT IDENTITY(1,1) PRIMARY KEY,
    full_name  NVARCHAR(100)  NOT NULL,
    email      NVARCHAR(100)  NOT NULL,
    role       NVARCHAR(50)   NOT NULL,
    created_at DATETIME2      NOT NULL DEFAULT GETDATE()
);
GO

CREATE TABLE dbo.Projects (
    id          INT IDENTITY(1,1) PRIMARY KEY,
    customer_id INT NOT NULL FOREIGN KEY REFERENCES dbo.Customers(id),
    name        NVARCHAR(100)  NOT NULL,
    project_key NVARCHAR(10)   NOT NULL,
    created_at  DATETIME2      NOT NULL DEFAULT GETDATE()
);
GO

CREATE TABLE dbo.Tickets (
    id          INT IDENTITY(1,1) PRIMARY KEY,
    project_id  INT NOT NULL FOREIGN KEY REFERENCES dbo.Projects(id),
    reporter_id INT NOT NULL FOREIGN KEY REFERENCES dbo.Users(id),
    assignee_id INT NULL FOREIGN KEY REFERENCES dbo.Users(id),
    title       NVARCHAR(200)  NOT NULL,
    description NVARCHAR(MAX),
    status      NVARCHAR(50)   NOT NULL,
    priority    NVARCHAR(50)   NOT NULL,
    created_at  DATETIME2      NOT NULL DEFAULT GETDATE(),
    updated_at  DATETIME2      NOT NULL DEFAULT GETDATE()
);
GO

CREATE TABLE dbo.TicketComments (
    id           INT IDENTITY(1,1) PRIMARY KEY,
    ticket_id    INT NOT NULL FOREIGN KEY REFERENCES dbo.Tickets(id),
    author_id    INT NOT NULL FOREIGN KEY REFERENCES dbo.Users(id),
    comment_text NVARCHAR(MAX)  NOT NULL,
    created_at   DATETIME2      NOT NULL DEFAULT GETDATE()
);
GO

EXEC sys.sp_cdc_enable_table
    @source_schema  = N'dbo',
    @source_name    = N'Customers',
    @role_name      = NULL,
    @supports_net_changes = 1;
GO

EXEC sys.sp_cdc_enable_table
    @source_schema  = N'dbo',
    @source_name    = N'Users',
    @role_name      = NULL,
    @supports_net_changes = 1;
GO

EXEC sys.sp_cdc_enable_table
    @source_schema  = N'dbo',
    @source_name    = N'Projects',
    @role_name      = NULL,
    @supports_net_changes = 1;
GO

EXEC sys.sp_cdc_enable_table
    @source_schema  = N'dbo',
    @source_name    = N'Tickets',
    @role_name      = NULL,
    @supports_net_changes = 1;
GO

EXEC sys.sp_cdc_enable_table
    @source_schema  = N'dbo',
    @source_name    = N'TicketComments',
    @role_name      = NULL,
    @supports_net_changes = 1;
GO

PRINT '✔ ShoriDB_3 created, CDC enabled on all tables';
GO

EXEC sys.sp_cdc_change_job
    @job_type        = N'capture',
    @pollinginterval = 1,
    @maxtrans        = 1000,
    @maxscans        = 20;
GO

PRINT '✔ CDC capture job tuned for ShoriDB_3';
GO

-- =============================================================
-- Bootstrap ShoriDB_4 with CDC enabled
-- =============================================================

CREATE DATABASE ShoriDB_4;
GO

USE ShoriDB_4;
GO

EXEC sys.sp_cdc_enable_db;
GO

CREATE TABLE dbo.Customers (
    id         INT IDENTITY(1,1) PRIMARY KEY,
    name       NVARCHAR(100)  NOT NULL,
    industry   NVARCHAR(50),
    created_at DATETIME2      NOT NULL DEFAULT GETDATE()
);
GO

CREATE TABLE dbo.Users (
    id         INT IDENTITY(1,1) PRIMARY KEY,
    full_name  NVARCHAR(100)  NOT NULL,
    email      NVARCHAR(100)  NOT NULL,
    role       NVARCHAR(50)   NOT NULL,
    created_at DATETIME2      NOT NULL DEFAULT GETDATE()
);
GO

CREATE TABLE dbo.Projects (
    id          INT IDENTITY(1,1) PRIMARY KEY,
    customer_id INT NOT NULL FOREIGN KEY REFERENCES dbo.Customers(id),
    name        NVARCHAR(100)  NOT NULL,
    project_key NVARCHAR(10)   NOT NULL,
    created_at  DATETIME2      NOT NULL DEFAULT GETDATE()
);
GO

CREATE TABLE dbo.Tickets (
    id          INT IDENTITY(1,1) PRIMARY KEY,
    project_id  INT NOT NULL FOREIGN KEY REFERENCES dbo.Projects(id),
    reporter_id INT NOT NULL FOREIGN KEY REFERENCES dbo.Users(id),
    assignee_id INT NULL FOREIGN KEY REFERENCES dbo.Users(id),
    title       NVARCHAR(200)  NOT NULL,
    description NVARCHAR(MAX),
    status      NVARCHAR(50)   NOT NULL,
    priority    NVARCHAR(50)   NOT NULL,
    created_at  DATETIME2      NOT NULL DEFAULT GETDATE(),
    updated_at  DATETIME2      NOT NULL DEFAULT GETDATE()
);
GO

CREATE TABLE dbo.TicketComments (
    id           INT IDENTITY(1,1) PRIMARY KEY,
    ticket_id    INT NOT NULL FOREIGN KEY REFERENCES dbo.Tickets(id),
    author_id    INT NOT NULL FOREIGN KEY REFERENCES dbo.Users(id),
    comment_text NVARCHAR(MAX)  NOT NULL,
    created_at   DATETIME2      NOT NULL DEFAULT GETDATE()
);
GO

EXEC sys.sp_cdc_enable_table
    @source_schema  = N'dbo',
    @source_name    = N'Customers',
    @role_name      = NULL,
    @supports_net_changes = 1;
GO

EXEC sys.sp_cdc_enable_table
    @source_schema  = N'dbo',
    @source_name    = N'Users',
    @role_name      = NULL,
    @supports_net_changes = 1;
GO

EXEC sys.sp_cdc_enable_table
    @source_schema  = N'dbo',
    @source_name    = N'Projects',
    @role_name      = NULL,
    @supports_net_changes = 1;
GO

EXEC sys.sp_cdc_enable_table
    @source_schema  = N'dbo',
    @source_name    = N'Tickets',
    @role_name      = NULL,
    @supports_net_changes = 1;
GO

EXEC sys.sp_cdc_enable_table
    @source_schema  = N'dbo',
    @source_name    = N'TicketComments',
    @role_name      = NULL,
    @supports_net_changes = 1;
GO

PRINT '✔ ShoriDB_4 created, CDC enabled on all tables';
GO

EXEC sys.sp_cdc_change_job
    @job_type        = N'capture',
    @pollinginterval = 1,
    @maxtrans        = 1000,
    @maxscans        = 20;
GO

PRINT '✔ CDC capture job tuned for ShoriDB_4';
GO

