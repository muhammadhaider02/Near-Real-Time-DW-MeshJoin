USE Metro_DW;

-- ----------------------------------- Query 1 -----------------------------------
-- Top Revenue-Generating Products on Weekdays and Weekends with Monthly Drill-Down
WITH RankedProducts AS (
    SELECT
        pd.productName AS Product,
        td.Month AS Month,
        td.Year AS Year,
        CASE 
            WHEN DAYOFWEEK(td.orderDate) IN (6, 7) THEN 'Weekend'
            ELSE 'Weekday'
        END AS DayType,
        SUM(sf.totalSale) AS TotalRevenue,
        ROW_NUMBER() OVER (
            PARTITION BY td.Month, 
                         CASE 
                             WHEN DAYOFWEEK(td.orderDate) IN (6, 7) THEN 'Weekend'
                             ELSE 'Weekday'
                         END
            ORDER BY SUM(sf.totalSale) DESC
        ) AS `Rank`
    FROM 
        Sales_FactTable sf
    JOIN 
        Time_Dimension td ON sf.timeID = td.timeID
    JOIN 
        Product_Dimension pd ON sf.productID = pd.productID
    WHERE 
        Year = 2019
    GROUP BY 
        pd.productName, td.Month, DayType
)
 /*
SELECT
    Product,
    DayType,
    Month,
    Year,
    TotalRevenue
FROM
    RankedProducts
WHERE
    `Rank` <= 5
ORDER BY
    Month,
    DayType,
    TotalRevenue DESC;
 */

-- Top 5 Revenue Generators Across Weekdays and Weekends For All Months
-- /*
SELECT
    Product,
    DayType,
    Month,
    Year,
    SUM(TotalRevenue) AS TotalRevenue
FROM
    RankedProducts
WHERE
    `Rank` <= 5
GROUP BY
    Product, Month, DayType
ORDER BY
    TotalRevenue DESC
LIMIT 5;
-- */

-- ----------------------------------- Query 2 -----------------------------------
-- Trend Analysis of Store Revenue Growth Rate Quarterly for 2019
WITH QuarterlyRevenue AS (
    SELECT 
        sd.storeName AS Store,
        td.Quarter AS Quarter,
        SUM(sf.totalSale) AS TotalRevenue
    FROM 
        Sales_FactTable sf
    JOIN 
        Store_Dimension sd ON sf.storeID = sd.storeID
    JOIN 
        Time_Dimension td ON sf.timeID = td.timeID
    WHERE 
        td.Year = 2019
    GROUP BY 
        sd.storeName, td.Quarter
),
GrowthRateCalculation AS (
    SELECT 
        q1.Store,
        q1.Quarter AS CurrentQuarter,
        q1.TotalRevenue AS CurrentRevenue,
        q2.TotalRevenue AS PreviousRevenue,
        CASE 
            WHEN q2.TotalRevenue IS NULL THEN '-'
            ELSE CONCAT(ROUND(((q1.TotalRevenue - q2.TotalRevenue) / q2.TotalRevenue) * 100, 2), '%')
        END AS GrowthRate
    FROM 
        QuarterlyRevenue q1
    LEFT JOIN 
        QuarterlyRevenue q2
    ON 
        q1.Store = q2.Store AND q1.Quarter = q2.Quarter + 1
)
SELECT 
    Store,
    CurrentQuarter AS Quarter,
    CurrentRevenue,
    CASE 
        WHEN PreviousRevenue IS NULL THEN '-'
        ELSE PreviousRevenue
    END AS PreviousRevenue,
    GrowthRate
FROM 
    GrowthRateCalculation
ORDER BY 
    Store, CurrentQuarter;

-- ----------------------------------- Query 3 -----------------------------------
--  Detailed Supplier Sales Contribution by Store and Product Category
WITH SupplierSales AS (
    SELECT 
        sd.storeName AS Store,
        sdd.supplierName AS Supplier,
        pd.productName AS ProductName,
        SUM(sf.totalSale) AS TotalSales
    FROM 
        Sales_FactTable sf
    JOIN 
        Store_Dimension sd ON sf.storeID = sd.storeID
    JOIN 
        Supplier_Dimension sdd ON sf.supplierID = sdd.supplierID
    JOIN 
        Product_Dimension pd ON sf.productID = pd.productID
    GROUP BY 
        sd.storeName, sdd.supplierName, pd.productName
)
SELECT 
    Store,
    Supplier,
    ProductName,
    TotalSales
FROM 
    SupplierSales
ORDER BY 
    Store, Supplier, ProductName;

-- ----------------------------------- Query 4 -----------------------------------
-- Seasonal Analysis of Product Sales Using Dynamic Drill-Down
WITH SeasonalSales AS (
    SELECT 
        pd.productName AS Product,
        td.Year AS Year,
        CASE 
            WHEN td.Month IN (3, 4, 5) THEN 'Spring'
            WHEN td.Month IN (6, 7, 8) THEN 'Summer'
            WHEN td.Month IN (9, 10, 11) THEN 'Fall'
            WHEN td.Month IN (12, 1, 2) THEN 'Winter'
        END AS Season,
        SUM(sf.totalSale) AS TotalSales
    FROM 
        Sales_FactTable sf
    JOIN 
        Product_Dimension pd ON sf.productID = pd.productID
    JOIN 
        Time_Dimension td ON sf.timeID = td.timeID
    GROUP BY 
        pd.productName, 
        td.Year,
        CASE 
            WHEN td.Month IN (3, 4, 5) THEN 'Spring'
            WHEN td.Month IN (6, 7, 8) THEN 'Summer'
            WHEN td.Month IN (9, 10, 11) THEN 'Fall'
            WHEN td.Month IN (12, 1, 2) THEN 'Winter'
        END
)
SELECT 
    Product,
    Year,
    Season,
    TotalSales
FROM 
    SeasonalSales
ORDER BY 
    Product, 
    Year, 
    FIELD(Season, 'Spring', 'Summer', 'Fall', 'Winter');
    
-- ----------------------------------- Query 5 -----------------------------------
-- Store-Wise and Supplier-Wise Monthly Revenue Volatility
WITH MonthlyRevenue AS (
    SELECT 
        sd.storeName AS Store,
        sdd.supplierName AS Supplier,
        td.Month AS CurrentMonth,
        td.Year AS Year,
        SUM(sf.totalSale) AS CurrentRevenue
    FROM 
        Sales_FactTable sf
    JOIN 
        Store_Dimension sd ON sf.storeID = sd.storeID
    JOIN 
        Supplier_Dimension sdd ON sf.supplierID = sdd.supplierID
    JOIN 
        Time_Dimension td ON sf.timeID = td.timeID
    GROUP BY 
        sd.storeName, sdd.supplierName, td.Year, td.Month
),
RevenueVolatility AS (
    SELECT 
        mr1.Store,
        mr1.Supplier,
        mr1.Year,
        mr1.CurrentMonth,
        mr1.CurrentRevenue,
        mr2.CurrentRevenue AS PreviousRevenue,
        CASE 
            WHEN mr2.CurrentRevenue IS NULL THEN '-'
            ELSE CONCAT(ROUND(((mr1.CurrentRevenue - mr2.CurrentRevenue) / mr2.CurrentRevenue) * 100, 2), '%')
        END AS Volatility
    FROM 
        MonthlyRevenue mr1
    LEFT JOIN 
        MonthlyRevenue mr2
    ON 
        mr1.Store = mr2.Store 
        AND mr1.Supplier = mr2.Supplier
        AND mr1.Year = mr2.Year
        AND mr1.CurrentMonth = mr2.CurrentMonth + 1
)
SELECT 
    Store,
    Supplier,
    Year,
    CurrentMonth,
    CurrentRevenue,
    Volatility
FROM 
    RevenueVolatility
ORDER BY 
    Store, Supplier, Year, CurrentMonth;
    
-- ----------------------------------- Query 6 -----------------------------------
-- Top 5 Products Purchased Together Across Multiple Orders (Product Affinity Analysis)
-- /*
WITH ProductPairs AS (
    SELECT 
        p1.productName AS Product1,
        p2.productName AS Product2,
        COUNT(DISTINCT sf1.orderID) AS Frequency
    FROM 
        Sales_FactTable sf1
    JOIN 
        Sales_FactTable sf2 ON sf1.orderID = sf2.orderID AND sf1.productID < sf2.productID
    JOIN 
        Product_Dimension p1 ON sf1.productID = p1.productID
    JOIN 
        Product_Dimension p2 ON sf2.productID = p2.productID
    GROUP BY 
        p1.productName, p2.productName
)
SELECT 
    Product1,
    Product2,
    Frequency
FROM 
    ProductPairs
ORDER BY 
    Frequency DESC
LIMIT 5;
 -- */
 
/*
WITH ProductGroups AS (
    SELECT 
        sf.storeID AS Store,
        td.orderDate AS OrderDate,
        td.orderTime AS OrderTime,
        p.productName AS Product
    FROM 
        Sales_FactTable sf
    JOIN 
        Product_Dimension p ON sf.productID = p.productID
    JOIN 
        Time_Dimension td ON sf.timeID = td.timeID
),
ProductPairs AS (
    SELECT 
        pg1.Product AS Product1,
        pg2.Product AS Product2,
        COUNT(*) AS Frequency
    FROM 
        ProductGroups pg1
    JOIN 
        ProductGroups pg2 
        ON pg1.Store = pg2.Store 
        AND pg1.OrderDate = pg2.OrderDate
        AND pg1.OrderTime = pg2.OrderTime
        AND pg1.Product < pg2.Product
    GROUP BY 
        pg1.Product, pg2.Product
)
SELECT 
    Product1,
    Product2,
    Frequency
FROM 
    ProductPairs
ORDER BY 
    Frequency DESC
LIMIT 5;
*/

-- ----------------------------------- Query 7 -----------------------------------
-- Yearly Revenue Trends by Store, Supplier, and Product with ROLLUP
SELECT
    sd.storeName AS Store,
    sdd.supplierName AS Supplier,
    pd.productName AS Product,
    td.Year AS Year,
    SUM(sf.totalSale) AS TotalRevenue
FROM 
    Sales_FactTable sf
JOIN 
    Store_Dimension sd ON sf.storeID = sd.storeID
JOIN 
    Supplier_Dimension sdd ON sf.supplierID = sdd.supplierID
JOIN 
    Product_Dimension pd ON sf.productID = pd.productID
JOIN 
    Time_Dimension td ON sf.timeID = td.timeID
WHERE 
    td.Year IS NOT NULL
GROUP BY 
    ROLLUP(sd.storeName, sdd.supplierName, pd.productName, td.Year)
ORDER BY 
    sd.storeName, 
    sdd.supplierName, 
    pd.productName, 
    td.Year;
    
-- ----------------------------------- Query 8 -----------------------------------
-- Revenue and Volume-Based Sales Analysis for Each Product for H1 and H2
WITH ProductSalesByHalf AS (
    SELECT 
        pd.productName AS Product,
        td.Year AS Year,
        CASE 
            WHEN td.Quarter IN (1, 2) THEN 'H1'
            WHEN td.Quarter IN (3, 4) THEN 'H2'
        END AS HalfYear,
        SUM(sf.totalSale) AS TotalRevenue,
        SUM(sf.quantityOrdered) AS TotalQuantity
    FROM 
        Sales_FactTable sf
    JOIN 
        Product_Dimension pd ON sf.productID = pd.productID
    JOIN 
        Time_Dimension td ON sf.timeID = td.timeID
    GROUP BY 
        pd.productName, 
        td.Year,
        CASE 
            WHEN td.Quarter IN (1, 2) THEN 'H1'
            WHEN td.Quarter IN (3, 4) THEN 'H2'
        END
),
YearlyTotals AS (
    SELECT 
        ps.Product,
        ps.Year,
        'Yearly' AS HalfYear,
        SUM(ps.TotalRevenue) AS TotalRevenue,
        SUM(ps.TotalQuantity) AS TotalQuantity
    FROM 
        ProductSalesByHalf ps
    GROUP BY 
        ps.Product, ps.Year
)
SELECT 
    Product,
    Year,
    HalfYear,
    TotalRevenue,
    TotalQuantity
FROM 
    ProductSalesByHalf
UNION ALL
SELECT 
    Product,
    Year,
    HalfYear,
    TotalRevenue,
    TotalQuantity
FROM 
    YearlyTotals
ORDER BY 
    Product, Year, FIELD(HalfYear, 'H1', 'H2', 'Yearly');

-- ----------------------------------- Query 9 -----------------------------------
-- Identify High Revenue Spikes in Product Sales and Highlight Outliers
WITH DailyProductSales AS (
    SELECT 
        pd.productName AS Product,
        td.orderDate AS SaleDate,
        SUM(sf.totalSale) AS DailySales
    FROM 
        Sales_FactTable sf
    JOIN 
        Product_Dimension pd ON sf.productID = pd.productID
    JOIN 
        Time_Dimension td ON sf.timeID = td.timeID
    GROUP BY 
        pd.productName, td.orderDate
),
ProductDailyAverages AS (
    SELECT 
        Product,
        ROUND(AVG(DailySales), 2) AS DailyAverage
    FROM 
        DailyProductSales
    GROUP BY 
        Product
),
FlaggedSales AS (
    SELECT 
        dps.Product,
        dps.SaleDate,
        dps.DailySales,
        pda.DailyAverage,
        CASE 
            WHEN dps.DailySales > 2 * pda.DailyAverage THEN 'Outlier'
            ELSE 'Normal'
        END AS Flag
    FROM 
        DailyProductSales dps
    JOIN 
        ProductDailyAverages pda ON dps.Product = pda.Product
)
SELECT 
    Product,
    SaleDate,
    DailySales,
    DailyAverage,
    Flag
FROM 
    FlaggedSales
ORDER BY 
    Product, SaleDate;

-- ----------------------------------- Query 10 -----------------------------------
--  Create a View STORE_QUARTERLY_SALES for Optimized Sales Analysis
DROP VIEW IF EXISTS STORE_QUARTERLY_SALES;

CREATE VIEW STORE_QUARTERLY_SALES AS
SELECT 
    sd.storeName AS Store,
    td.Quarter AS Quarter,
    td.Year AS Year,
    SUM(sf.totalSale) AS TotalSales
FROM 
    Sales_FactTable sf
JOIN 
    Store_Dimension sd ON sf.storeID = sd.storeID
JOIN 
    Time_Dimension td ON sf.timeID = td.timeID
WHERE 
    td.Year IS NOT NULL
GROUP BY 
    sd.storeName, td.Quarter, td.Year
ORDER BY 
    sd.storeName, td.Year, td.Quarter;

SELECT * FROM STORE_QUARTERLY_SALES;