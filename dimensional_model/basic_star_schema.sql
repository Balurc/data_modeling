-- create a new schema in analytics database; MARTS_CREDIT_DEBIT_TRANSACTION
CREATE OR REPLACE DATABASE analytics;
CREATE OR REPLACE SCHEMA analytics.marts_credit_debit_transaction;

-- create dimension table; dim_users
CREATE OR REPLACE TABLE analytics.marts_credit_debit_transaction.dim_users AS (
    SELECT
        DISTINCT user_reference AS user_id, 
        age_band, 
        salary_band, 
        postcode,
        LSOA,
        MSOA,
        derived_gender AS gender
    FROM raw_credit_debit_transaction.public_listing.transactions
);

-- create dimension table; dim_merchants
CREATE OR REPLACE TABLE analytics.marts_credit_debit_transaction.dim_merchants AS (
    SELECT
        DISTINCT HASH(merchant_name, merchant_business_line)::VARCHAR AS merchant_id,
        merchant_name,
        merchant_business_line
    FROM raw_credit_debit_transaction.public_listing.transactions
);

-- create dimension table; dim_accounts
CREATE OR REPLACE TABLE analytics.marts_credit_debit_transaction.dim_accounts AS (
    SELECT
        DISTINCT account_reference AS account_id,
        provider_group_name AS bank_name,
        account_type,
        account_created_date,
        account_last_refreshed
    FROM raw_credit_debit_transaction.public_listing.transactions
);

-- create dimension table; dim_accounts
CREATE OR REPLACE TABLE analytics.marts_credit_debit_transaction.dim_transactions AS (
    SELECT
        transaction_reference AS transaction_id,
        credit_debit AS transaction_type,
        auto_purpose_tag_name AS transaction_purpose
    FROM raw_credit_debit_transaction.public_listing.transactions
);

-- create dimension table; dim_dates
CREATE OR REPLACE TABLE analytics.marts_credit_debit_transaction.dim_dates AS (
    SELECT
        DISTINCT transaction_date,
        DAY(transaction_date)::VARCHAR as day_of_month,
        DAYNAME(transaction_date) as day_name,
        MONTH(transaction_date)::VARCHAR as month_of_year,
        MONTHNAME(transaction_date) as month_name,
        YEAR(transaction_date)::VARCHAR as year
    FROM raw_credit_debit_transaction.public_listing.transactions
);

-- create fact table; fct_transactions
CREATE OR REPLACE TABLE analytics.marts_credit_debit_transaction.fct_transactions AS (
    SELECT
        transaction_date AS transaction_date,
        transaction_reference AS transaction_id,
        user_reference AS user_id,
        account_reference AS account_id,
        HASH(merchant_name, merchant_business_line)::VARCHAR AS merchant_id,
        amount::NUMBER as amount
    FROM raw_credit_debit_transaction.public_listing.transactions
);
