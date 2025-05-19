-- Accounts data query
SELECT
    account_id,
    name,
    official_name,
    type,
    subtype,
    holder_category,
    current_balance,
    available_balance,
    iso_currency_code
FROM OPENROWSET(
    BULK 'accounts_data/*.csv',
    DATA_SOURCE = 'transformed_data',
    FORMAT = 'CSV',
    PARSER_VERSION = '2.0',
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
    HEADER_ROW = TRUE
)
WITH (
    account_id NVARCHAR(100),
    name NVARCHAR(100),
    official_name NVARCHAR(150),
    type NVARCHAR(50),
    subtype NVARCHAR(50),
    holder_category NVARCHAR(50),
    current_balance FLOAT,
    available_balance FLOAT,
    iso_currency_code NVARCHAR(10)
) AS data;

-- Transactions data query
SELECT *
FROM OPENROWSET(
    BULK 'transactions_data/*.csv',
    DATA_SOURCE = 'transformed_data',
    FORMAT = 'CSV',
    PARSER_VERSION = '2.0',
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
    HEADER_ROW = TRUE
)
WITH (
    transaction_id NVARCHAR(100),
    account_id NVARCHAR(100),
    amount FLOAT,
    iso_currency_code NVARCHAR(10),
    date DATE,
    transaction_type NVARCHAR(50),
    confidence_level NVARCHAR(50),
    pending BIT,
    category NVARCHAR(100),
    merchant_name NVARCHAR(100),
    payment_channel NVARCHAR(50),
    website NVARCHAR(200)
) AS data;