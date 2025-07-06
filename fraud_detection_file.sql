create database fraud_detection;
use fraud_detection;

-- View all transactions in the base table
SELECT * FROM bank_transactions_data ;

-- Declare variables
DECLARE @high_value_limit INT = 5000;
DECLARE @high_value_count INT = 3;
DECLARE @time_window_hours INT = 1;

DECLARE @odd_hour_start INT = 6;
DECLARE @odd_hour_end INT = 22;
DECLARE @location_change_threshold INT = 2;

SELECT 
    SUM(CASE WHEN TransactionID IS NULL THEN 1 ELSE 0 END) AS missing_transactionid,
    SUM(CASE WHEN AccountID IS NULL THEN 1 ELSE 0 END) AS missing_accountid,
    SUM(CASE WHEN TransactionAmount IS NULL THEN 1 ELSE 0 END) AS missing_transactionamount,
    SUM(CASE WHEN TransactionDate IS NULL THEN 1 ELSE 0 END) AS missing_transactiondate,
    SUM(CASE WHEN TransactionType IS NULL THEN 1 ELSE 0 END) AS missing_transactiontype
FROM bank_transactions_data;

-- Drop flagged_transactions if it already exists
IF OBJECT_ID('flagged_transactions', 'U') IS NOT NULL
    DROP TABLE flagged_transactions;

WITH 
high_value_tx AS (
    SELECT 
        t1.AccountID,
        t1.TransactionDate,
        t1.TransactionAmount,
        (
            SELECT COUNT(*)
            FROM bank_transactions_data t2
            WHERE t2.AccountID = t1.AccountID
              AND t2.TransactionDate BETWEEN DATEADD(HOUR, -@time_window_hours, t1.TransactionDate)
                                         AND t1.TransactionDate
        ) AS tx_count,
        (
            SELECT SUM(t2.TransactionAmount)
            FROM bank_transactions_data t2
            WHERE t2.AccountID = t1.AccountID
              AND t2.TransactionDate BETWEEN DATEADD(HOUR, -@time_window_hours, t1.TransactionDate)
                                         AND t1.TransactionDate
        ) AS total_value
    FROM bank_transactions_data t1
),
duplicate_tx AS (
    SELECT 
        TransactionID,
        AccountID,
        TransactionAmount,
        TransactionDate,
        MerchantID,
        COUNT(*) OVER (PARTITION BY AccountID, TransactionAmount, MerchantID, TransactionDate) AS duplicate_count
    FROM bank_transactions_data
),
odd_hour_tx AS (
    SELECT
        t1.TransactionID,
        t1.AccountID,
        t1.TransactionDate,
        t1.TransactionType,
        t1.Location,
        t1.TransactionAmount,
        DATEPART(HOUR, t1.TransactionDate) AS tx_hour,
        (
            SELECT COUNT(DISTINCT t2.Location)
            FROM bank_transactions_data t2
            WHERE t2.AccountID = t1.AccountID
              AND t2.TransactionDate BETWEEN DATEADD(HOUR, -24, t1.TransactionDate)
                                         AND t1.TransactionDate
        ) AS location_changes
    FROM bank_transactions_data t1
)
SELECT * INTO flagged_transactions
FROM (
    SELECT AccountID, TransactionDate, TransactionAmount, 'high_value' AS fraud_type
    FROM high_value_tx
    WHERE tx_count > @high_value_count AND total_value > @high_value_limit

    UNION ALL

    SELECT AccountID, TransactionDate, TransactionAmount, 'duplicate' AS fraud_type
    FROM duplicate_tx
    WHERE duplicate_count > 1

    UNION ALL

    SELECT AccountID, TransactionDate, TransactionAmount, 'odd_hour' AS fraud_type
    FROM odd_hour_tx
    WHERE tx_hour < @odd_hour_start OR tx_hour > @odd_hour_end OR location_changes > @location_change_threshold
) AS combined_fraud;
