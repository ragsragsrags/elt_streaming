USE [master]
GO
/****** Object:  Database [testDBWH]    Script Date: 11/30/2025 12:55:43 AM ******/
CREATE DATABASE [testDBWH]
 CONTAINMENT = NONE
 ON  PRIMARY 
( NAME = N'testDBWH', FILENAME = N'C:\Program Files\Microsoft SQL Server\MSSQL16.MSSQLSERVER05\MSSQL\DATA\testDBWH.mdf' , SIZE = 139264KB , MAXSIZE = UNLIMITED, FILEGROWTH = 65536KB )
 LOG ON 
( NAME = N'testDBWH_log', FILENAME = N'C:\Program Files\Microsoft SQL Server\MSSQL16.MSSQLSERVER05\MSSQL\DATA\testDBWH_log.ldf' , SIZE = 991232KB , MAXSIZE = 2048GB , FILEGROWTH = 65536KB )
 WITH CATALOG_COLLATION = DATABASE_DEFAULT, LEDGER = OFF
GO
ALTER DATABASE [testDBWH] SET COMPATIBILITY_LEVEL = 160
GO
IF (1 = FULLTEXTSERVICEPROPERTY('IsFullTextInstalled'))
begin
EXEC [testDBWH].[dbo].[sp_fulltext_database] @action = 'enable'
end
GO
ALTER DATABASE [testDBWH] SET ANSI_NULL_DEFAULT OFF 
GO
ALTER DATABASE [testDBWH] SET ANSI_NULLS OFF 
GO
ALTER DATABASE [testDBWH] SET ANSI_PADDING OFF 
GO
ALTER DATABASE [testDBWH] SET ANSI_WARNINGS OFF 
GO
ALTER DATABASE [testDBWH] SET ARITHABORT OFF 
GO
ALTER DATABASE [testDBWH] SET AUTO_CLOSE OFF 
GO
ALTER DATABASE [testDBWH] SET AUTO_SHRINK OFF 
GO
ALTER DATABASE [testDBWH] SET AUTO_UPDATE_STATISTICS ON 
GO
ALTER DATABASE [testDBWH] SET CURSOR_CLOSE_ON_COMMIT OFF 
GO
ALTER DATABASE [testDBWH] SET CURSOR_DEFAULT  GLOBAL 
GO
ALTER DATABASE [testDBWH] SET CONCAT_NULL_YIELDS_NULL OFF 
GO
ALTER DATABASE [testDBWH] SET NUMERIC_ROUNDABORT OFF 
GO
ALTER DATABASE [testDBWH] SET QUOTED_IDENTIFIER OFF 
GO
ALTER DATABASE [testDBWH] SET RECURSIVE_TRIGGERS OFF 
GO
ALTER DATABASE [testDBWH] SET  ENABLE_BROKER 
GO
ALTER DATABASE [testDBWH] SET AUTO_UPDATE_STATISTICS_ASYNC OFF 
GO
ALTER DATABASE [testDBWH] SET DATE_CORRELATION_OPTIMIZATION OFF 
GO
ALTER DATABASE [testDBWH] SET TRUSTWORTHY OFF 
GO
ALTER DATABASE [testDBWH] SET ALLOW_SNAPSHOT_ISOLATION OFF 
GO
ALTER DATABASE [testDBWH] SET PARAMETERIZATION SIMPLE 
GO
ALTER DATABASE [testDBWH] SET READ_COMMITTED_SNAPSHOT OFF 
GO
ALTER DATABASE [testDBWH] SET HONOR_BROKER_PRIORITY OFF 
GO
ALTER DATABASE [testDBWH] SET RECOVERY FULL 
GO
ALTER DATABASE [testDBWH] SET  MULTI_USER 
GO
ALTER DATABASE [testDBWH] SET PAGE_VERIFY CHECKSUM  
GO
ALTER DATABASE [testDBWH] SET DB_CHAINING OFF 
GO
ALTER DATABASE [testDBWH] SET FILESTREAM( NON_TRANSACTED_ACCESS = OFF ) 
GO
ALTER DATABASE [testDBWH] SET TARGET_RECOVERY_TIME = 60 SECONDS 
GO
ALTER DATABASE [testDBWH] SET DELAYED_DURABILITY = DISABLED 
GO
ALTER DATABASE [testDBWH] SET ACCELERATED_DATABASE_RECOVERY = OFF  
GO
EXEC sys.sp_db_vardecimal_storage_format N'testDBWH', N'ON'
GO
ALTER DATABASE [testDBWH] SET QUERY_STORE = ON
GO
ALTER DATABASE [testDBWH] SET QUERY_STORE (OPERATION_MODE = READ_WRITE, CLEANUP_POLICY = (STALE_QUERY_THRESHOLD_DAYS = 30), DATA_FLUSH_INTERVAL_SECONDS = 900, INTERVAL_LENGTH_MINUTES = 60, MAX_STORAGE_SIZE_MB = 1000, QUERY_CAPTURE_MODE = AUTO, SIZE_BASED_CLEANUP_MODE = AUTO, MAX_PLANS_PER_QUERY = 200, WAIT_STATS_CAPTURE_MODE = ON)
GO
USE [testDBWH]
GO
/****** Object:  UserDefinedFunction [dbo].[GetDateTime2FromUnixEpoch]    Script Date: 11/30/2025 12:55:43 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE FUNCTION [dbo].[GetDateTime2FromUnixEpoch]
(
	@value BIGINT,
	@granularity NVARCHAR(50)
)
RETURNS DATETIME2
AS
BEGIN

	DECLARE @dateTime2 DATETIME2
	
	IF @granularity = 'NANOSECOND'
		BEGIN

			IF @value = -4852116231933722724 
				SET @dateTime2 = CAST('9999-12-31 23:59:59.9999999' AS DATETIME2)
			ELSE
				SET @dateTime2 = 
					DATEADD
					(
						NANOSECOND,
						CAST(((CAST(@value AS DECIMAL(20, 0)) / 1000000000) - CAST(CAST(@value AS DECIMAL(20, 0)) / 1000000000 AS INT)) * 1000000000 AS INT),
						DATEADD
						(
							SECOND, 
							CAST(CAST(@value AS DECIMAL(20, 0)) / 1000000000 AS INT), 
							CAST('1970-01-01' AS DATETIME2)
						)
					)

		END
	ELSE IF @granularity = 'MILLISECOND'
		BEGIN

			SET @dateTime2 = 
				DATEADD
				(
					MILLISECOND, 
					CAST(((CAST(@value AS DECIMAL(20, 0)) / 1000) - CAST(CAST(@value AS DECIMAL(20, 0)) / 1000 AS INT)) * 1000 AS INT),
					DATEADD
					(
						SECOND, 
						CAST(CAST(@value AS DECIMAL(20, 0)) / 1000 AS INT), 
						CAST('1970-01-01' AS DATETIME2)
					)
				)

		END

	RETURN @dateTime2

END
GO
/****** Object:  Table [dbo].[customers]    Script Date: 11/30/2025 12:55:43 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[customers](
	[id] [int] NOT NULL,
	[first_name] [varchar](255) NOT NULL,
	[last_name] [varchar](255) NOT NULL,
	[email] [varchar](255) NOT NULL,
	[valid_from] [datetime2](7) NULL,
	[valid_to] [datetime2](7) NULL,
PRIMARY KEY CLUSTERED 
(
	[id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY],
UNIQUE NONCLUSTERED 
(
	[email] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[customers_archive]    Script Date: 11/30/2025 12:55:43 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[customers_archive](
	[id] [int] NOT NULL,
	[first_name] [varchar](255) NOT NULL,
	[last_name] [varchar](255) NOT NULL,
	[email] [varchar](255) NOT NULL,
	[valid_from] [datetime2](7) NULL,
	[valid_to] [datetime2](7) NULL,
	[deleted] [bit] NULL
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[load_history]    Script Date: 11/30/2025 12:55:43 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[load_history](
	[table_name] [nvarchar](50) NOT NULL,
	[load_date] [datetime2](7) NOT NULL,
	[last_message_id] [int] NOT NULL,
	[status] [nvarchar](50) NOT NULL,
	[is_preloaded] [bit] NOT NULL,
	[ending_offset] [int] NOT NULL,
	[is_validated] [bit] NULL,
	[validated_date] [datetime2](7) NULL
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[messages]    Script Date: 11/30/2025 12:55:43 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[messages](
	[message_id] [int] IDENTITY(1,1) NOT NULL,
	[operation] [nvarchar](500) NULL,
	[schema] [nvarchar](50) NULL,
	[table] [nvarchar](50) NULL,
	[ts_ns] [bigint] NULL,
	[before] [nvarchar](max) NULL,
	[after] [nvarchar](max) NULL,
	[processed] [tinyint] NULL,
	[key_id] [int] NULL,
PRIMARY KEY CLUSTERED 
(
	[message_id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [dbo].[orders]    Script Date: 11/30/2025 12:55:43 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[orders](
	[id] [int] NOT NULL,
	[order_date] [date] NOT NULL,
	[purchaser] [int] NOT NULL,
	[quantity] [int] NOT NULL,
	[product_id] [int] NOT NULL,
	[valid_from] [datetime2](7) NOT NULL,
	[valid_to] [datetime2](7) NOT NULL,
PRIMARY KEY CLUSTERED 
(
	[id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[orders_archive]    Script Date: 11/30/2025 12:55:43 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[orders_archive](
	[id] [int] NOT NULL,
	[order_date] [date] NOT NULL,
	[purchaser] [int] NOT NULL,
	[quantity] [int] NOT NULL,
	[product_id] [int] NOT NULL,
	[valid_from] [datetime2](7) NOT NULL,
	[valid_to] [datetime2](7) NOT NULL,
	[deleted] [bit] NULL
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[products]    Script Date: 11/30/2025 12:55:43 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[products](
	[id] [int] NOT NULL,
	[name] [varchar](255) NOT NULL,
	[description] [varchar](512) NULL,
	[weight] [float] NULL,
	[valid_from] [datetime2](7) NULL,
	[valid_to] [datetime2](7) NULL,
PRIMARY KEY CLUSTERED 
(
	[id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[products_archive]    Script Date: 11/30/2025 12:55:43 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[products_archive](
	[id] [int] NOT NULL,
	[name] [varchar](255) NOT NULL,
	[description] [varchar](512) NULL,
	[weight] [float] NULL,
	[valid_from] [datetime2](7) NULL,
	[valid_to] [datetime2](7) NULL,
	[deleted] [bit] NULL
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[products_on_hand]    Script Date: 11/30/2025 12:55:43 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[products_on_hand](
	[product_id] [int] NOT NULL,
	[quantity] [int] NOT NULL,
	[valid_from] [datetime2](7) NULL,
	[valid_to] [datetime2](7) NULL,
 CONSTRAINT [PK__products__47027DF5E7BCB66C] PRIMARY KEY CLUSTERED 
(
	[product_id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[products_on_hand_archive]    Script Date: 11/30/2025 12:55:43 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[products_on_hand_archive](
	[product_id] [int] NOT NULL,
	[quantity] [int] NOT NULL,
	[valid_from] [datetime2](7) NULL,
	[valid_to] [datetime2](7) NULL,
	[deleted] [bit] NULL
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[validated_dates]    Script Date: 11/30/2025 12:55:43 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[validated_dates](
	[validated_date] [datetime2](7) NOT NULL
) ON [PRIMARY]
GO
/****** Object:  StoredProcedure [dbo].[GetPreloadStatus]    Script Date: 11/30/2025 12:55:43 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO









/*

EXEC [dbo].[GetPreloadStatus] 

*/
CREATE PROCEDURE [dbo].[GetPreloadStatus]
(
	@Tables NVARCHAR(MAX)
)
AS
BEGIN

    SET NOCOUNT ON;
    SET XACT_ABORT ON;

	DECLARE @is_preloaded_validated BIT = 0
	DECLARE @is_preloaded BIT = 0
	DECLARE @is_initialized BIT = 0

	IF (
		SELECT
			COUNT(*)
		FROM
			STRING_SPLIT(@Tables, ',') T LEFT JOIN
			[dbo].[load_history] LH ON
				LH.[table_name] = T.[value] AND
				LH.[status] = 'Successful' AND
				ISNULL(LH.[is_preloaded], 0) = 1 AND
				ISNULL(LH.[is_validated], 0) = 1
		WHERE
			LH.[table_name] IS NULL
		) = 0
	BEGIN

		SET @is_preloaded_validated = 1
		SET @is_preloaded = 1
		SET @is_initialized = 1

	END

	IF @is_preloaded = 0 AND (
		SELECT
			COUNT(*)
		FROM
			STRING_SPLIT(@Tables, ',') T LEFT JOIN
			[dbo].[load_history] LH ON
				LH.[table_name] = T.[value] AND
				LH.[status] = 'Successful' AND
				ISNULL(LH.[is_preloaded], 0) = 1
		WHERE
			LH.[table_name] IS NULL
		) = 0
	BEGIN

		SET @is_preloaded = 1
		SET @is_initialized = 1

	END
		
	IF @is_initialized = 0 AND (
		SELECT
			COUNT(*)
		FROM
			STRING_SPLIT(@Tables, ',') T LEFT JOIN
			[dbo].[load_history] LH ON
				LH.[table_name] = T.[value] AND
				LH.[status] = 'Successful' 
		WHERE
			LH.[table_name] IS NULL
		) = 0
	BEGIN

		SET @is_initialized = 1

	END

	SELECT
		[is_initialized] = @is_initialized,
		[is_preloaded] = @is_preloaded,
		[is_preloaded_validated] = @is_preloaded_validated
	
END;
GO
/****** Object:  StoredProcedure [dbo].[InsertMessages]    Script Date: 11/30/2025 12:55:43 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO








/*

EXEC [dbo].[ProcessProductMessage] 
	@message_id=1055,
	@operation='r',
	@before=NULL,
	@after='{"id": 101, "name": "scooter", "description": "Small 2-wheel scooter", "weight": 3.14}'

*/
CREATE PROCEDURE [dbo].[InsertMessages]
(
	@Messages NVARCHAR(MAX),
	@Preloaded BIT
)
AS
BEGIN

    SET NOCOUNT ON;
    SET XACT_ABORT ON;

	BEGIN TRAN

	;WITH tmpMessages AS 
	(
		SELECT 
			M.[operation],
			M.[schema],
			M.[table],
			M.[ts_ns],
			M.[before],
			M.[after],
			[processed] = 0,
			[key_id] = CAST(
				CASE 
					-- when operation is delete then use before
					WHEN M.[operation] = 'd' THEN
						CASE 
							WHEN M.[table] IN ('products_on_hand', 'products_on_hand_archive') THEN JSON_VALUE(M.[before], '$.product_id')
							ELSE JSON_VALUE(M.[before], '$.id')
						END
					-- when operation is in create, read or update the use after
					ELSE 
						CASE 
							WHEN M.[table] IN ('products_on_hand', 'products_on_hand_archive') THEN JSON_VALUE(M.[after], '$.product_id')
							ELSE JSON_VALUE(M.[after], '$.id')
						END
				END AS INT
			)
		FROM 
			OPENJSON(@Messages) WITH 
			(
				[operation] NVARCHAR(2) '$.payload.op',
				[schema] NVARCHAR(50) '$.payload.source.schema',
				[table] NVARCHAR(50) '$.payload.source.table',
				[ts_ns] BIGINT '$.payload.source.ts_ns',
				[before] NVARCHAR(MAX) '$.payload.before' AS JSON,
				[after] NVARCHAR(MAX) '$.payload.after' AS JSON
			) M
	)

	-- insert to messages table
	INSERT INTO [dbo].[messages]
	(
		[operation],
		[schema],
		[table],
		[ts_ns],
		[before],
		[after],
		[processed],
		[key_id]
	)
	SELECT DISTINCT
		M.[operation],
		M.[schema],
		M.[table],
		M.[ts_ns],
		M.[before],
		M.[after],
		M.[processed],
		M.[key_id]
	FROM
		tmpMessages M LEFT JOIN
		[dbo].[messages] M2 ON
			M2.[key_id] = M.[key_id] AND
			M2.[operation] = M.[operation] AND
			M2.[table] = M.[table] AND
			M2.[ts_ns] = M.[ts_ns]
	WHERE
		(
			@PreLoaded = 1 OR
			(
				@PreLoaded = 0 AND
				M.[operation] <> 'r' AND
				M2.message_id IS NULL
			) 
		)
	
	COMMIT TRAN

END;
GO
/****** Object:  StoredProcedure [dbo].[InsertOffsetLoadHistories]    Script Date: 11/30/2025 12:55:43 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO





/*

EXEC [dbo].[ProcessProductMessage] '[{"table":"customers","startingOffset":10},{"table":"customers_archive","startingOffset":5}]'
	
*/
CREATE PROCEDURE [dbo].[InsertOffsetLoadHistories]
(
	@StartingOffsets NVARCHAR(MAX)
)
AS
BEGIN

    SET NOCOUNT ON;
    SET XACT_ABORT ON;

	BEGIN TRAN

	-- insert load history after initial load
	INSERT INTO [dbo].[load_history]
	(
		[table_name],
		[load_date],
		[last_message_id],
		[status],
		[is_preloaded],
		[ending_offset],
		[is_validated]
	)
	SELECT
		[table_name] = SO.[table],
		GETUTCDATE(),
		[last_message_id] = -1,
		[status] = 'Initial',
		[is_preloaded] = 1,
		[ending_offset] = MAX(SO.[startingOffset]) + COUNT(M.message_id),
		0
	FROM 
		OPENJSON(@StartingOffsets) WITH 
		(
			[table] NVARCHAR(50) '$.table',
			[startingOffset] INT '$.startingOffset'
		) SO LEFT JOIN
		[dbo].[messages] M ON
			M.[table] = SO.[table] AND
			M.[operation] = 'r'
	GROUP BY
		SO.[table]

	COMMIT TRAN

END;
GO
/****** Object:  StoredProcedure [dbo].[ProcessCustomerMessage]    Script Date: 11/30/2025 12:55:43 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO










/*

-- insert
EXEC [dbo].[ProcessCustomerMessage] 
	@message_id=1,
	@operation='r',
	@before=NULL,
	@after='{"id": 1001, "first_name": "Sally", "last_name": "Thomas", "email": "sally.thomas@acme.com", "valid_from": 1763475193772470500, "valid_to": -4852116231933722724}',
	@ts_ns=1763475236090741500

-- insert
EXEC [dbo].[ProcessCustomerMessage] 
	@message_id=4,
	@operation='r',
	@before=NULL,
	@after='{"id": 1004, "first_name": "Anne", "last_name": "Kretchmar", "email": "annek@noanswer.org", "valid_from": 1763475193797639000, "valid_to": -4852116231933722724}',
	@ts_ns=1763475236090741500

-- update
EXEC [dbo].[ProcessCustomerMessage] 
	@message_id=27,
	@operation='u',
	@before='{"id": 1001, "first_name": "Sally", "last_name": "Thomas", "email": "sally.thomas@acme.com", "valid_from": 1763475193772470500, "valid_to": -4852116231933722724}',
	@after='{"id": 1001, "first_name": "Sally", "last_name": "Thomas 1", "email": "sally.thomas@acme.com", "valid_from": 1763475416046773700, "valid_to": -4852116231933722724}',
	@ts_ns=1763475416050000000

-- delete
EXEC [dbo].[ProcessCustomerMessage] 
	@message_id=28,
	@operation='d',
	@before='{"id": 1004, "first_name": "Anne", "last_name": "Kretchmar", "email": "annek@noanswer.org", "valid_from": 1763475193797639000, "valid_to": -4852116231933722724}',
	@after=NULL,
	@ts_ns=1763475420447000000

*/

CREATE PROCEDURE [dbo].[ProcessCustomerMessage]
(
	@message_id INT,
	@operation NVARCHAR(2),
	@before NVARCHAR(MAX),
	@after NVARCHAR(MAX),
	@ts_ns BIGINT
)
WITH EXECUTE AS OWNER
AS
BEGIN

    SET NOCOUNT ON;
    SET XACT_ABORT ON;

	DECLARE @InsertSql NVARCHAR(MAX) = N'
		INSERT INTO [dbo].[{{table}}]
		(
			[id],
			[first_name],
			[last_name],
			[email],
			[valid_from],
			[valid_to]
			{{deleted_insert}}
		)
		SELECT 
			[id],
			[first_name],
			[last_name],
			[email],
			[valid_from] = 
				CASE
					WHEN @transaction IN (1, 2) THEN
						[dbo].[GetDateTime2FromUnixEpoch](
							C.[valid_from], 
							''NANOSECOND''
						)
					ELSE 
						[dbo].[GetDateTime2FromUnixEpoch](
							JSON_VALUE(@before, ''$.valid_from''), 
							''NANOSECOND''
						)
				END,
			[valid_to] = 
				CASE 
					WHEN @transaction = 1 THEN
						[dbo].[GetDateTime2FromUnixEpoch](
							C.[valid_to], 
							''NANOSECOND''
						)
					WHEN @transaction = 2 THEN
						[dbo].[GetDateTime2FromUnixEpoch](
							JSON_VALUE(@before, ''$.valid_from''), 
							''NANOSECOND''
						)
					ELSE
						[dbo].[GetDateTime2FromUnixEpoch](
							@ts_ns, 
							''NANOSECOND''
						)
				END
			{{deleted_select}}
		FROM 
			OPENJSON(@json) WITH 
			(
				[id] NVARCHAR(50) ''$.id'',
				[first_name] NVARCHAR(255) ''$.first_name'',
				[last_name] NVARCHAR(255) ''$.last_name'',
				[email] NVARCHAR(255) ''$.email'',
				[valid_from] BIGINT ''$.valid_from'',
				[valid_to] BIGINT ''$.valid_to''
			) C
		{{where_insert}}
	'

	-- insert customer record
	IF @operation = 'c'
	BEGIN

		SET @InsertSql = REPLACE(@InsertSql, '{{table}}', 'customers')
		SET @InsertSql = REPLACE(@InsertSql, '{{deleted_insert}}', '')
		SET @InsertSql = REPLACE(@InsertSql, '{{deleted_select}}', '')
		SET @InsertSql = REPLACE(@InsertSql, '{{where_insert}}', ' WHERE NOT EXISTS (SELECT 1 FROM [dbo].[customers] C2 WHERE C2.id = C.id)')

		EXEC sp_executesql @InsertSql, N'@json NVARCHAR(MAX), @transaction TINYINT, @before NVARCHAR(MAX), @ts_ns BIGINT', @after, 1, @before, @ts_ns

	END

	-- update customer record
	IF @operation = 'u'
	BEGIN
		
		-- update customer record
		UPDATE 
			[dbo].[customers]
		SET 
			[id] = C.id_u,
			[first_name] = C.first_name_u,
			[last_name] = C.last_name_u,
			[email] = C.email_u,
			[valid_from] = [dbo].[GetDateTime2FromUnixEpoch](
				C.[valid_from_u], 
				'NANOSECOND'
			),
			[valid_to] = [dbo].[GetDateTime2FromUnixEpoch](
				C.[valid_to_u], 
				'NANOSECOND'
			)
		FROM 
			OPENJSON(@after) WITH 
			(
				[id_u] NVARCHAR(50) '$.id',
				[first_name_u] NVARCHAR(255) '$.first_name',
				[last_name_u] NVARCHAR(255) '$.last_name',
				[email_u] NVARCHAR(255) '$.email',
				[valid_from_u] BIGINT '$.valid_from',
				[valid_to_u] BIGINT '$.valid_to'
			) C
		WHERE
			[id] = C.id_u

		-- insert archive record in customers_archive
		SET @InsertSql = REPLACE(@InsertSql, '{{table}}', 'customers_archive')
		SET @InsertSql = REPLACE(@InsertSql, '{{deleted_insert}}', ', [deleted]')
		SET @InsertSql = REPLACE(@InsertSql, '{{deleted_select}}', ', 0')
		SET @InsertSql = REPLACE(@InsertSql, '{{where_insert}}', '')
		
		EXEC sp_executesql @InsertSql, N'@json NVARCHAR(MAX), @transaction TINYINT, @before NVARCHAR(MAX), @ts_ns BIGINT', @before, 2, @after, @ts_ns

	END

	-- delete customer
	IF @operation = 'd'
	BEGIN

		DELETE
			[dbo].[customers]
		WHERE
			id IN
			(
				SELECT
					id
				FROM
					OPENJSON(@before) WITH 
					(
						[id] NVARCHAR(50) '$.id'
					)
			)

		-- insert archive record in customers_archive
		SET @InsertSql = REPLACE(@InsertSql, '{{table}}', 'customers_archive')
		SET @InsertSql = REPLACE(@InsertSql, '{{deleted_insert}}', ', [deleted]')
		SET @InsertSql = REPLACE(@InsertSql, '{{deleted_select}}', ', 1')
		SET @InsertSql = REPLACE(@InsertSql, '{{where_insert}}', '')
		
		EXEC sp_executesql @InsertSql, N'@json NVARCHAR(MAX), @transaction TINYINT, @before NVARCHAR(MAX), @ts_ns BIGINT', @before, 3, @before, @ts_ns
		
	END

	-- update processed record to complete
	UPDATE 
		[dbo].[messages]
	SET
		processed = 2
	WHERE
		message_id = @message_id

END;
GO
/****** Object:  StoredProcedure [dbo].[ProcessMessages]    Script Date: 11/30/2025 12:55:43 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO




















/*

EXEC [dbo].[ProcessMessages] 500

*/
CREATE PROCEDURE [dbo].[ProcessMessages]
(
	@NoOfRows INT = 10000
)
WITH EXECUTE AS OWNER
AS
BEGIN

    SET NOCOUNT ON;
    SET XACT_ABORT ON;

	-- stop processing if there is already pre-processed records,
	-- likely a process wass already started or there was error occurred
	IF EXISTS 
	(
		SELECT
			1
		FROM
			[dbo].[messages]
		WHERE
			[processed] = 1
	)
	BEGIN
	
		RAISERROR('There are messsages with processed = 1.', 16, 1);

	END

	ELSE
	
	BEGIN

		-- create temp messages table for processing
		IF OBJECT_ID('tempdb..#Messages') IS NOT NULL 
			DROP TABLE #Messages

		CREATE TABLE #Messages
		(
			[message_id] INT,
			[operation] NVARCHAR(500),
			[schema] NVARCHAR(50),
			[table] NVARCHAR(50),
			[ts_ns] BIGINT,
			[before] NVARCHAR(MAX),
			[after] NVARCHAR(MAX),
			[processed] TINYINT, -- 0 = not processed, 1 = processing, 2 = processed
			[processed_tsns] BIGINT
		)

		-- insert messages to be processed
		INSERT INTO #Messages
		(
			[message_id],
			[operation],
			[schema],
			[table],
			[ts_ns],
			[before],
			[after],
			[processed],
			[processed_tsns]
		)
		SELECT TOP (@NoOfRows)
			[message_id],
			[operation],
			[schema],
			[table],
			[ts_ns],
			[before],
			[after],
			[processed],
			CASE
				WHEN M.[operation] IN ('c', 'u', 'r') THEN 
					CAST(
						JSON_VALUE(M.[after], 
						CASE
							WHEN [table] LIKE '%_archive' then '$.valid_to'
							ELSE '$.valid_from'
						END
					) AS BIGINT)
				ELSE M.[ts_ns]
			END
		FROM
			[dbo].[messages] M
		WHERE
			[processed] = 0 AND
			[operation] <> 'r'
		ORDER BY
			CASE
				WHEN M.[operation] IN ('c', 'u', 'r') THEN 
					CAST(
						JSON_VALUE(M.[after], 
						CASE
							WHEN [table] LIKE '%_archive' then '$.valid_to'
							ELSE '$.valid_from'
						END
					) AS BIGINT)
				ELSE M.[ts_ns]
			END,
			[message_id]

		DECLARE @maxValidFrom BIGINT
		DECLARE @minValidFrom BIGINT
	
		SELECT
			@minValidFrom = MIN(M.[processed_tsns]),
			@maxValidFrom = MAX(M.[processed_tsns])
		FROM
			#Messages M

		-- insert with same date
		INSERT INTO #Messages
		(
			[message_id],
			[operation],
			[schema],
			[table],
			[ts_ns],
			[before],
			[after],
			[processed],
			[processed_tsns]
		)
		SELECT
			[message_id],
			[operation],
			[schema],
			[table],
			[ts_ns],
			[before],
			[after],
			[processed],
			CASE
				WHEN [operation] IN ('c', 'u', 'r') THEN 
					CAST(
						JSON_VALUE([after], 
						CASE
							WHEN [table] LIKE '%_archive' then '$.valid_to'
							ELSE '$.valid_from'
						END
					) AS BIGINT)
				ELSE [ts_ns]
			END
		FROM
			[dbo].[messages]
		WHERE
			[processed] = 0 AND
			[operation] <> 'r' AND
			CASE
				WHEN [operation] IN ('c', 'u', 'r') THEN 
					CAST(
						JSON_VALUE([after], 
						CASE
							WHEN [table] LIKE '%_archive' then '$.valid_to'
							ELSE '$.valid_from'
						END
					) AS BIGINT)
				ELSE [ts_ns]
			END BETWEEN @minValidFrom AND @maxValidFrom AND
			[message_id] NOT IN
			(
				SELECT
					[message_id]
				FROM
					#Messages M
			) 

		IF (SELECT [MessageCount] = COUNT(*) FROM #Messages) > 0
		BEGIN

			-- Update the first 1 
			UPDATE 
				M
			SET
				M.processed = 1
			FROM
				[dbo].[messages] M
			WHERE
				M.message_id IN 
				(
					SELECT TOP 1
						M2.message_id
					FROM
						#Messages M2 
					ORDER BY
						ts_ns
				)

			-- update processed to 1
			BEGIN TRAN

			UPDATE 
				M
			SET
				M.processed = 1
			FROM
				[dbo].[messages] M
			WHERE
				M.message_id IN
				(
					SELECT
						M2.message_id
					FROM
						#Messages M2 
				)

			COMMIT TRAN

			BEGIN TRAN

			WHILE (SELECT COUNT(*) FROM #Messages WHERE processed = 0) > 0
			BEGIN

				DECLARE @message_id INT
				DECLARE @operation NVARCHAR(2)
				DECLARE @schema NVARCHAR(50)
				DECLARE @table NVARCHAR(50)
				DECLARE @ts_ns BIGINT
				DECLARE @before NVARCHAR(MAX) = NULL
				DECLARE @after NVARCHAR(MAX) = NULL

				SELECT TOP 1
					@message_id = M.message_id,
					@operation = M.operation,
					@schema = M.[schema],
					@table = M.[table],
					@ts_ns = M.[ts_ns],
					@before = M.[before],
					@after = M.[after]
				FROM 
					#Messages M
				WHERE
					processed = 0
				ORDER BY
					[processed_tsns],
					[message_id]

				-- customers
				IF @schema = 'dbo' AND @table = 'customers'
				BEGIN

					EXEC [dbo].[ProcessCustomerMessage] 
						@message_id=@message_id,
						@operation=@operation,
						@before=@before,
						@after=@after,
						@ts_ns=@ts_ns

				END

				IF @schema = 'dbo' AND @table = 'orders'
				BEGIN

					EXEC [dbo].[ProcessOrderMessage] 
						@message_id=@message_id,
						@operation=@operation,
						@before=@before,
						@after=@after,
						@ts_ns=@ts_ns

				END

				IF @schema = 'dbo' AND @table = 'products'
				BEGIN

					EXEC [dbo].[ProcessProductMessage] 
						@message_id=@message_id,
						@operation=@operation,
						@before=@before,
						@after=@after,
						@ts_ns=@ts_ns

				END

				IF @schema = 'dbo' AND @table = 'products_on_hand'
				BEGIN

					EXEC [dbo].[ProcessProductOnHandMessage] 
						@message_id=@message_id,
						@operation=@operation,
						@before=@before,
						@after=@after,
						@ts_ns=@ts_ns

				END

				UPDATE 
					#Messages
				SET
					processed = 1
				WHERE
					message_id = @message_id

			END

			UPDATE 
				M
			SET
				M.processed = 2
			FROM
				[dbo].[messages] M
			WHERE
				M.message_id IN
				(
					SELECT
						M2.message_id
					FROM
						#Messages M2 
				)


			-- insert load history for every process
			INSERT INTO [dbo].[load_history]
			(
				[table_name],
				[load_date],
				[last_message_id],
				[status],
				[is_preloaded],
				[ending_offset],
				[is_validated],
				[validated_date]
			)
			VALUES
			(
				'all',
				CAST(GETUTCDATE() AS DATETIME2(7)),
				(SELECT MAX([message_id]) FROM #Messages),
				'Successful',
				0,
				-1,
				0,
				(
					SELECT
						[validated_date] = [dbo].[GetDateTime2FromUnixEpoch](
							MAX(M.[validated_date]), 
							'NANOSECOND'
						)
					FROM
						(
							SELECT
								[validated_date] = 
									CASE
										WHEN M.[operation] IN ('c', 'u', 'r') THEN 
											CAST(
												JSON_VALUE(M.[after], 
												CASE
													WHEN [table] LIKE '%_archive' then '$.valid_to'
													ELSE '$.valid_from'
												END
											) AS BIGINT)
										ELSE M.[ts_ns]
									END
							FROM
								#Messages M
						) M
				)
			)

			COMMIT TRAN

		END

		PRINT('after process messages')

	END

END;
GO
/****** Object:  StoredProcedure [dbo].[ProcessOrderMessage]    Script Date: 11/30/2025 12:55:43 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO







/*

EXEC [dbo].[ProcessOrderMessage] 
	@message_id=47,
	@operation='r',
	@before=NULL,
	@after='{"id":10001,"order_date":16816,"purchaser":1001,"quantity":1,"product_id":102}'

*/
CREATE PROCEDURE [dbo].[ProcessOrderMessage]
(
	@message_id INT,
	@operation NVARCHAR(2),
	@before NVARCHAR(MAX),
	@after NVARCHAR(MAX),
	@ts_ns BIGINT
)
WITH EXECUTE AS OWNER
AS
BEGIN

    SET NOCOUNT ON;
    SET XACT_ABORT ON;

	-- sql template for insert
	DECLARE @InsertSql NVARCHAR(MAX) = N'
		INSERT INTO [dbo].[{{table}}]
		(
			[id],
			[order_date],
			[purchaser],
			[quantity],
			[product_id],
			[valid_from],
			[valid_to]
			{{deleted_insert}}
		)
		SELECT 
			[id],
			CAST(DATEADD(DAY, [order_date], ''1970-01-01'') AS DATE),
			[purchaser],
			[quantity],
			[product_id],
			[valid_from] = 
				CASE 
					WHEN @transaction IN (1, 2) THEN
						[dbo].[GetDateTime2FromUnixEpoch](
							O.[valid_from], 
							''NANOSECOND''
						)
					ELSE
						[dbo].[GetDateTime2FromUnixEpoch](
							JSON_VALUE(@before, ''$.valid_from''), 
							''NANOSECOND''
						)
				END,
			[valid_to] = 
				CASE 
					WHEN @transaction = 1 THEN
						[dbo].[GetDateTime2FromUnixEpoch](
							O.[valid_to], 
							''NANOSECOND''
						)
					WHEN @transaction = 2 THEN
						[dbo].[GetDateTime2FromUnixEpoch](
							JSON_VALUE(@before, ''$.valid_from''), 
							''NANOSECOND''
						)
					ELSE
						[dbo].[GetDateTime2FromUnixEpoch](
							@ts_ns, 
							''NANOSECOND''
						)
				END
			{{deleted_select}}
		FROM 
			OPENJSON(@json) WITH 
			(
				[id] NVARCHAR(50) ''$.id'',
				[order_date] INT ''$.order_date'',
				[purchaser] INT ''$.purchaser'',
				[quantity] INT ''$.quantity'',
				[product_id] INT ''$.product_id'',
				[valid_from] BIGINT ''$.valid_from'',
				[valid_to] BIGINT ''$.valid_to''
			) O
		{{where_insert}}
	'

	-- insert order record
	IF @operation = 'c'
	BEGIN
			
		SET @InsertSql = REPLACE(@InsertSql, '{{table}}', 'orders')
		SET @InsertSql = REPLACE(@InsertSql, '{{deleted_insert}}', '')
		SET @InsertSql = REPLACE(@InsertSql, '{{deleted_select}}', '')
		SET @InsertSql = REPLACE(@InsertSql, '{{where_insert}}', ' WHERE NOT EXISTS (SELECT 1 FROM [dbo].[orders] O2 WHERE O2.[product_id] = O.[product_id])')

		EXEC sp_executesql @InsertSql, N'@json NVARCHAR(MAX), @transaction TINYINT, @before NVARCHAR(MAX), @ts_ns BIGINT', @after, 1, @before, @ts_ns

	END

	-- update order record
	IF @operation = 'u'
	BEGIN
			
		-- update order record
		UPDATE 
			dbo.orders
		SET
			[id] = O.[id_u],
			[order_date] = CAST(DATEADD(DAY, [order_date_u], '1970-01-01') AS DATE),
			[purchaser] = O.[purchaser_u],
			[quantity] = O.[quantity_u],
			[product_id] = O.[product_id_u],
			[valid_from] = [dbo].[GetDateTime2FromUnixEpoch](O.[valid_from_u], 'NANOSECOND'),
			[valid_to] = [dbo].[GetDateTime2FromUnixEpoch](O.[valid_to_u], 'NANOSECOND')
		FROM 
			OPENJSON(@after) WITH 
			(
				[id_u] NVARCHAR(50) '$.id',
				[order_date_u] INT '$.order_date',
				[purchaser_u] INT '$.purchaser',
				[quantity_u] INT '$.quantity',
				[product_id_u] INT '$.product_id',
				[valid_from_u] BIGINT '$.valid_from',
				[valid_to_u] BIGINT '$.valid_to'
			) O
		WHERE
			[id] = O.id_u

		-- insert archive record in orders_archive
		SET @InsertSql = REPLACE(@InsertSql, '{{table}}', 'orders_archive')
		SET @InsertSql = REPLACE(@InsertSql, '{{deleted_insert}}', ', [deleted]')
		SET @InsertSql = REPLACE(@InsertSql, '{{deleted_select}}', ', 0')
		SET @InsertSql = REPLACE(@InsertSql, '{{where_insert}}', '')

		EXEC sp_executesql @InsertSql, N'@json NVARCHAR(MAX), @transaction TINYINT, @before NVARCHAR(MAX), @ts_ns BIGINT', @before, 2, @after, @ts_ns

	END

	-- delete order record
	IF @operation = 'd'
	BEGIN

		DELETE
			dbo.orders
		WHERE
			id IN
			(
				SELECT
					id
				FROM
					OPENJSON(@before) WITH 
					(
						[id] NVARCHAR(50) '$.id'
					)
			)

		-- insert data in products_archive
		SET @InsertSql = REPLACE(@InsertSql, '{{table}}', 'orders_archive')
		SET @InsertSql = REPLACE(@InsertSql, '{{deleted_insert}}', ', [deleted]')
		SET @InsertSql = REPLACE(@InsertSql, '{{deleted_select}}', ', 1')
		SET @InsertSql = REPLACE(@InsertSql, '{{where_insert}}', '')

		EXEC sp_executesql @InsertSql, N'@json NVARCHAR(MAX), @transaction TINYINT, @before NVARCHAR(MAX), @ts_ns BIGINT', @before, 3, @before, @ts_ns

	END

	-- insert processsed status to 2 for complete
	UPDATE 
		[dbo].[messages]
	SET
		processed = 2
	WHERE
		message_id = @message_id

END;
GO
/****** Object:  StoredProcedure [dbo].[ProcessProductMessage]    Script Date: 11/30/2025 12:55:43 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO





/*

-- insert
EXEC [dbo].[ProcessProductMessage] 
	@message_id=17,
	@operation='r',
	@before=NULL,
	@after='{"id": 109, "name": "spare tire", "description": "24 inch spare tire", "weight": 22.2, "valid_from": 1763475185503852500, "valid_to": -4852116231933722724}',
	@ts_ns=1763475236128274700

-- insert
EXEC [dbo].[ProcessProductMessage] 
	@message_id=30,
	@operation='c',
	@before=NULL,
	@after='{"id": 110, "name": "12 inch spare tire ", "description": "12 inch spare tire", "weight": 12.0, "valid_from": 1763476793301726600, "valid_to": -4852116231933722724}',
	@ts_ns=1763476793300000000

-- update
EXEC [dbo].[ProcessProductMessage] 
	@message_id=29,
	@operation='u',
	@before='{"id": 109, "name": "spare tire", "description": "24 inch spare tire", "weight": 22.2, "valid_from": 1763475185503852500, "valid_to": -4852116231933722724}',
	@after='{"id": 109, "name": "spare tire", "description": "24 inch spare tire 1", "weight": 22.2, "valid_from": 1763476788261566500, "valid_to": -4852116231933722724}',
	@ts_ns=1763476788263000000

-- delete
EXEC [dbo].[ProcessProductMessage] 
	@message_id=31,
	@operation='d',
	@before='{"id": 110, "name": "12 inch spare tire ", "description": "12 inch spare tire", "weight": 12.0, "valid_from": 1763476793301726600, "valid_to": -4852116231933722724}',
	@after=NULL,
	@ts_ns=1763476796960000000

*/
CREATE PROCEDURE [dbo].[ProcessProductMessage]
(
	@message_id INT,
	@operation NVARCHAR(2),
	@before NVARCHAR(MAX),
	@after NVARCHAR(MAX),
	@ts_ns BIGINT
)
AS
BEGIN

    SET NOCOUNT ON;
    SET XACT_ABORT ON;

	DECLARE @InsertSql NVARCHAR(MAX) = N'
		INSERT INTO [dbo].[{{table}}]
		(
			[id],
			[name],
			[description],
			[weight],
			[valid_from],
			[valid_to]
			{{deleted_insert}}
		)
		SELECT 
			[id],
			[name],
			[description],
			[weight],
			[valid_from] =
				CASE 
					WHEN @transaction IN (1, 2) THEN
						[dbo].[GetDateTime2FromUnixEpoch](
							P.[valid_from], 
							''NANOSECOND''
						)
					ELSE
						[dbo].[GetDateTime2FromUnixEpoch](
							JSON_VALUE(@before, ''$.valid_from''), 
							''NANOSECOND''
						)
				END,
			[valid_to] = 
				CASE 
					WHEN @transaction = 1 THEN
						[dbo].[GetDateTime2FromUnixEpoch](
							P.[valid_to], 
							''NANOSECOND''
						)
					WHEN @transaction = 2 THEN
						[dbo].[GetDateTime2FromUnixEpoch](
							JSON_VALUE(@before, ''$.valid_from''), 
							''NANOSECOND''
						)
					ELSE
						[dbo].[GetDateTime2FromUnixEpoch](
							@ts_ns, 
							''NANOSECOND''
						)
				END
			{{deleted_select}}
		FROM 
			OPENJSON(@json) WITH 
			(
				[id] INT ''$.id'',
				[name] VARCHAR(255) ''$.name'',
				[description] VARCHAR(512) ''$.description'',
				[weight] float ''$.weight'',
				[valid_from] BIGINT ''$.valid_from'',
				[valid_to] BIGINT ''$.valid_to''
			) P
		{{where_insert}}
	'

	-- insert product record
	IF @operation = 'c'
	BEGIN
			
		SET @InsertSql = REPLACE(@InsertSql, '{{table}}', 'products')
		SET @InsertSql = REPLACE(@InsertSql, '{{deleted_insert}}', '')
		SET @InsertSql = REPLACE(@InsertSql, '{{deleted_select}}', '')
		SET @InsertSql = REPLACE(@InsertSql, '{{where_insert}}', ' WHERE NOT EXISTS (SELECT 1 FROM [dbo].[products] P2 WHERE P2.id = P.id)')

		EXEC sp_executesql @InsertSql, N'@json NVARCHAR(MAX), @transaction TINYINT, @before NVARCHAR(MAX), @ts_ns BIGINT', @after, 1, @before, @ts_ns

	END

	-- update product record
	IF @operation = 'u'
	BEGIN
			
		-- update product record
		UPDATE 
			dbo.products
		SET
			[id] = P.[id_u],
			[name] = P.[name_u],
			[description] = P.[description_u],
			[weight] = P.[weight_u],
			[valid_from] = [dbo].[GetDateTime2FromUnixEpoch](
				P.[valid_from_u], 
				'NANOSECOND'
			),
			[valid_to] = [dbo].[GetDateTime2FromUnixEpoch](
				P.[valid_to_u], 
				'NANOSECOND'
			)
		FROM 
			OPENJSON(@after) WITH 
			(
				[id_u] INT '$.id',
				[name_u] VARCHAR(255) '$.name',
				[description_u] VARCHAR(512) '$.description',
				[weight_u] FLOAT '$.weight',
				[valid_from_u] BIGINT '$.valid_from',
				[valid_to_u] BIGINT '$.valid_to'
			) P
		WHERE
			[id] = P.[id_u]

		-- insert archive record in products_archive
		SET @InsertSql = REPLACE(@InsertSql, '{{table}}', 'products_archive')
		SET @InsertSql = REPLACE(@InsertSql, '{{deleted_insert}}', ', [deleted]')
		SET @InsertSql = REPLACE(@InsertSql, '{{deleted_select}}', ', 0')
		SET @InsertSql = REPLACE(@InsertSql, '{{where_insert}}', '')

		EXEC sp_executesql @InsertSql, N'@json NVARCHAR(MAX), @transaction TINYINT, @before NVARCHAR(MAX), @ts_ns BIGINT', @before, 2, @after, @ts_ns

	END

	-- delete product record
	IF @operation = 'd'
	BEGIN

		DELETE
			dbo.products
		WHERE
			id IN
			(
				SELECT
					id
				FROM
					OPENJSON(@before) WITH 
					(
						[id] NVARCHAR(50) '$.id'
					)
			)

		-- insert archive record in products_archive
		SET @InsertSql = REPLACE(@InsertSql, '{{table}}', 'products_archive')
		SET @InsertSql = REPLACE(@InsertSql, '{{deleted_insert}}', ', [deleted]')
		SET @InsertSql = REPLACE(@InsertSql, '{{deleted_select}}', ', 1')
		SET @InsertSql = REPLACE(@InsertSql, '{{where_insert}}', '')
		
		EXEC sp_executesql @InsertSql, N'@json NVARCHAR(MAX), @transaction TINYINT, @before NVARCHAR(MAX), @ts_ns BIGINT', @before, 3, @before, @ts_ns

	END

	-- update processed status to completed = 2
	UPDATE 
		[dbo].[messages]
	SET
		processed = 2
	WHERE
		message_id = @message_id

END;
GO
/****** Object:  StoredProcedure [dbo].[ProcessProductOnHandMessage]    Script Date: 11/30/2025 12:55:43 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO






/*

-- insert
EXEC [dbo].[ProcessProductOnHandMessage] 
	@message_id=18,
	@operation='r',
	@before=NULL,
	@after='{"product_id": 101, "quantity": 3, "valid_from": 1763475193236155200, "valid_to": -4852116231933722724}',
	@ts_ns=1763475236140697300

-- insert
EXEC [dbo].[ProcessProductOnHandMessage] 
	@message_id=26,
	@operation='r',
	@before=NULL,
	@after='{"product_id": 109, "quantity": 5, "valid_from": 1763475193336277500, "valid_to": -4852116231933722724}',
	@ts_ns=1763475236140697300

-- update
EXEC [dbo].[ProcessProductOnHandMessage] 
	@message_id=32,
	@operation='u',
	@before='{"product_id": 101, "quantity": 3, "valid_from": 1763475193236155200, "valid_to": -4852116231933722724}',
	@after='{"product_id": 101, "quantity": 4, "valid_from": 1763481464602711900, "valid_to": -4852116231933722724}',
	@ts_ns=1763481464603000000

-- delete
EXEC [dbo].[ProcessProductOnHandMessage] 
	@message_id=33,
	@operation='d',
	@before='{"product_id": 109, "quantity": 5, "valid_from": 1763475193336277500, "valid_to": -4852116231933722724}',
	@after=NULL,
	@ts_ns=1763481474910000000

*/
CREATE PROCEDURE [dbo].[ProcessProductOnHandMessage]
(
	@message_id INT,
	@operation NVARCHAR(2),
	@before NVARCHAR(MAX),
	@after NVARCHAR(MAX),
	@ts_ns BIGINT
)
WITH EXECUTE AS OWNER
AS
BEGIN

    SET NOCOUNT ON;
    SET XACT_ABORT ON;

	-- sql template for insert
	DECLARE @InsertSql NVARCHAR(MAX) = N'
		INSERT INTO [dbo].[{{table}}]
		(
			[product_id],
			[quantity],
			[valid_from],
			[valid_to]
			{{deleted_insert}}
		)
		SELECT 
			[product_id],
			[quantity],
			[valid_from] = 
				CASE 
					WHEN @transaction IN (1, 2) THEN
						[dbo].[GetDateTime2FromUnixEpoch](
							P.[valid_from], 
							''NANOSECOND''
						)
					ELSE
						[dbo].[GetDateTime2FromUnixEpoch](
							JSON_VALUE(@before, ''$.valid_from''), 
							''NANOSECOND''
						)
				END,
			[valid_to] = 
				CASE 
					WHEN @transaction = 1 THEN
						[dbo].[GetDateTime2FromUnixEpoch](
							P.[valid_to], 
							''NANOSECOND''
						)
					WHEN @transaction = 2 THEN
						[dbo].[GetDateTime2FromUnixEpoch](
							JSON_VALUE(@before, ''$.valid_from''), 
							''NANOSECOND''
						)
					ELSE
						[dbo].[GetDateTime2FromUnixEpoch](
							@ts_ns, 
							''NANOSECOND''
						)
				END
			{{deleted_select}}
		FROM 
			OPENJSON(@json) WITH 
			(
				[product_id] INT ''$.product_id'',
				[quantity] VARCHAR(255) ''$.quantity'',
				[valid_from] BIGINT ''$.valid_from'',
				[valid_to] BIGINT ''$.valid_to''
			) P
		{{where_insert}}
	'

	-- insert products_on_hand record
	IF @operation = 'c'
	BEGIN
			
		SET @InsertSql = REPLACE(@InsertSql, '{{table}}', 'products_on_hand')
		SET @InsertSql = REPLACE(@InsertSql, '{{deleted_insert}}', '')
		SET @InsertSql = REPLACE(@InsertSql, '{{deleted_select}}', '')
		SET @InsertSql = REPLACE(@InsertSql, '{{where_insert}}', ' WHERE NOT EXISTS (SELECT 1 FROM [dbo].[products_on_hand] P2 WHERE P2.[product_id] = P.[product_id])')

		EXEC sp_executesql @InsertSql, N'@json NVARCHAR(MAX), @transaction TINYINT, @before NVARCHAR(MAX), @ts_ns BIGINT', @after, 1, @before, @ts_ns

	END

	-- update products_on_hand record
	IF @operation = 'u'
	BEGIN
	
		-- update products_on_hand record
		UPDATE 
			dbo.products_on_hand
		SET
			[product_id] = P.[product_id_u],
			[quantity] = P.[quantity_u],
			[valid_from] = [dbo].[GetDateTime2FromUnixEpoch](
				P.[valid_from_u], 
				'NANOSECOND'
			),
			[valid_to] = [dbo].[GetDateTime2FromUnixEpoch](
				P.[valid_to_u], 
				'NANOSECOND'
			)
		FROM 
			OPENJSON(@after) WITH 
			(
				[product_id_u] INT '$.product_id',
				[quantity_u] VARCHAR(255) '$.quantity',
				[valid_from_u] BIGINT '$.valid_from',
				[valid_to_u] BIGINT '$.valid_to'
			) P
		WHERE
			[product_id] = P.[product_id_u]

		-- insert archive record in products_on_hand_archive
		SET @InsertSql = REPLACE(@InsertSql, '{{table}}', 'products_on_hand_archive')
		SET @InsertSql = REPLACE(@InsertSql, '{{deleted_insert}}', ', [deleted]')
		SET @InsertSql = REPLACE(@InsertSql, '{{deleted_select}}', ', 0')
		SET @InsertSql = REPLACE(@InsertSql, '{{where_insert}}', '')

		EXEC sp_executesql @InsertSql, N'@json NVARCHAR(MAX), @transaction TINYINT, @before NVARCHAR(MAX), @ts_ns BIGINT', @before, 2, @after, @ts_ns

	END

	-- delete products_on_hand record
	IF @operation = 'd'
	BEGIN

		DELETE
			dbo.products_on_hand
		WHERE
			product_id IN
			(
				SELECT
					product_id
				FROM
					OPENJSON(@before) WITH 
					(
						[product_id] NVARCHAR(50) '$.product_id'
					)
			)

		-- insert archive record in products_on_hand_archive
		SET @InsertSql = REPLACE(@InsertSql, '{{table}}', 'products_on_hand_archive')
		SET @InsertSql = REPLACE(@InsertSql, '{{deleted_insert}}', ', [deleted]')
		SET @InsertSql = REPLACE(@InsertSql, '{{deleted_select}}', ', 1')
		SET @InsertSql = REPLACE(@InsertSql, '{{where_insert}}', '')

		EXEC sp_executesql @InsertSql, N'@json NVARCHAR(MAX), @transaction TINYINT, @before NVARCHAR(MAX), @ts_ns BIGINT', @before, 3, @before, @ts_ns

	END

	-- update processed status to completed = 2
	UPDATE 
		[dbo].[messages]
	SET
		processed = 2
	WHERE
		message_id = @message_id

END;
GO
USE [master]
GO
ALTER DATABASE [testDBWH] SET  READ_WRITE 
GO
