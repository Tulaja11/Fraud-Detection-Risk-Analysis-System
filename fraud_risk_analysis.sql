Create database fraud_detection_analysis;
USE fraud_detection_analysis;

-- Overall Fraud Statistics
SELECT 
    Fraud_Label,
    COUNT(*) as Total_Transactions,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM fraud_detection_analysis), 2) as Percentage
FROM fraud_detection_analysis
GROUP BY Fraud_Label;

-- Top locations with highest fraud
SELECT 
    Location,
    COUNT(*) as Total_Transactions,
    SUM(Fraud_Label) as Fraud_Count,
    ROUND(AVG(Transaction_Amount), 2) as Avg_Transaction_Amount
FROM fraud_detection_analysis
WHERE Fraud_Label = 1
GROUP BY Location
ORDER BY Fraud_Count DESC
LIMIT 10;

-- High-risk merchant categories
SELECT 
    Merchant_Category,
    COUNT(*) as Total_Transactions,
    SUM(Fraud_Label) as Fraud_Count,
    ROUND(AVG(Risk_Score), 4) as Avg_Risk_Score,
    ROUND(AVG(Transaction_Amount), 2) as Avg_Amount
FROM fraud_detection_analysis
GROUP BY Merchant_Category
ORDER BY Fraud_Count DESC;

-- Time-based Analysis
-- Weekend vs Weekday fraud 
SELECT 
    CASE 
        WHEN Is_Weekend = 1 THEN 'Weekend'
        ELSE 'Weekday'
    END as Day_Type,
    COUNT(*) as Total_Transactions,
    SUM(Fraud_Label) as Fraud_Count,
    ROUND(SUM(Fraud_Label) * 100.0 / COUNT(*), 2) as Fraud_Percentage
FROM fraud_detection_analysis
GROUP BY Is_Weekend;

-- Fraud by authentication method
SELECT 
    Authentication_Method,
    COUNT(*) as Total_Transactions,
    SUM(Fraud_Label) as Fraud_Cases,
    ROUND(SUM(Fraud_Label) * 100.0 / COUNT(*), 2) as Fraud_Rate,
    ROUND(AVG(Transaction_Amount), 2) as Avg_Transaction_Amount
FROM fraud_detection_analysis
GROUP BY Authentication_Method
ORDER BY Fraud_Rate DESC;

-- Fraud distribution by card type
SELECT 
    Card_Type,
    COUNT(*) as Total_Transactions,
    SUM(Fraud_Label) as Fraud_Count,
    ROUND(AVG(Card_Age), 0) as Avg_Card_Age,
    ROUND(AVG(Transaction_Amount), 2) as Avg_Amount
FROM fraud_detection_analysis
GROUP BY Card_Type
ORDER BY Fraud_Count DESC;

-- high-risk transactions
SELECT 
    Transaction_ID,
    User_ID,
    Transaction_Amount,
    Location,
    Risk_Score,
    Fraud_Label
FROM fraud_detection_analysis
WHERE Risk_Score > 0.7
ORDER BY Risk_Score DESC
LIMIT 100;

-- Impact of previous fraudulent activity
SELECT 
    Previous_Fraudulent_Activity,
    COUNT(*) as Total_Transactions,
    SUM(Fraud_Label) as Current_Fraud_Count,
    ROUND(SUM(Fraud_Label) * 100.0 / COUNT(*), 2) as Current_Fraud_Rate
FROM fraud_detection_analysis
GROUP BY Previous_Fraudulent_Activity;

-- Analyze transaction patterns with running totals
SELECT 
    Transaction_ID,
    User_ID,
    Timestamp,
    Transaction_Amount,
    Fraud_Label,
    ROW_NUMBER() OVER (PARTITION BY User_ID ORDER BY Timestamp) as Transaction_Number,
    SUM(Transaction_Amount) OVER (PARTITION BY User_ID ORDER BY Timestamp) as Running_Total,
    AVG(Transaction_Amount) OVER (PARTITION BY User_ID ORDER BY Timestamp ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) as Moving_Avg_7days
FROM fraud_detection_analysis
ORDER BY User_ID, Timestamp
Limit 5;

-- Account balance distribution for fraud cases
SELECT 
    CASE 
        WHEN Account_Balance < 10000 THEN 'Low (<10K)'
        WHEN Account_Balance < 50000 THEN 'Medium (10K-50K)'
        WHEN Account_Balance < 100000 THEN 'High (50K-100K)'
        ELSE 'Very High (>100K)'
    END AS balance_category,
    COUNT(*) AS transaction_count,
    SUM(CASE WHEN Fraud_Label = 1 THEN 1 ELSE 0 END) AS fraud_count,
    ROUND(AVG(Transaction_Amount), 2) AS avg_transaction_amount
FROM fraud_detection_analysis
GROUP BY balance_category
ORDER BY fraud_count DESC;

-- VIEWS 
CREATE VIEW vw_fraud_detection AS
SELECT
    Transaction_ID,
    Timestamp,
    
    -- Time features for slicers
    YEAR(Timestamp) AS txn_year,
    MONTH(Timestamp) AS txn_month,
    DAY(Timestamp) AS txn_day,
    
    User_ID,
    Transaction_Amount,
    Transaction_Type,
    Account_Balance,
    
    Device_Type,
    Location,
    Merchant_Category,
    
    Card_Type,
    Card_Age,
    Authentication_Method,
    
    -- Behavioral metrics
    Daily_Transaction_Count,
    Avg_Transaction_Amount_7d,
    Failed_Transaction_Count_7d,
    Transaction_Distance,
    
    -- Risk & Fraud
    IP_Address_Flag,
    Previous_Fraudulent_Activity,
    Risk_Score,
    Is_Weekend,
    Fraud_Label

FROM fraud_detection_analysis;

