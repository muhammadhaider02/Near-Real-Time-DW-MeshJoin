CREATE DATABASE IF NOT EXISTS Metro_DW;
USE Metro_DW;

DROP TABLE IF EXISTS Sales_FactTable;
DROP TABLE IF EXISTS Product_Dimension;
DROP TABLE IF EXISTS Customer_Dimension;
DROP TABLE IF EXISTS Supplier_Dimension;
DROP TABLE IF EXISTS Store_Dimension;
DROP TABLE IF EXISTS Order_Dimension;
DROP TABLE IF EXISTS Time_Dimension;
DROP TABLE IF EXISTS Dummy_Table;

CREATE TABLE Supplier_Dimension (
    supplierID INT PRIMARY KEY,
    supplierName VARCHAR(100) UNIQUE
);

CREATE TABLE Store_Dimension (
    storeID INT PRIMARY KEY,
    storeName VARCHAR(100) UNIQUE
);

CREATE TABLE Product_Dimension (
    productID INT PRIMARY KEY,
    productName VARCHAR(100) UNIQUE,
    productPrice DECIMAL(10, 2)
);

CREATE TABLE Customer_Dimension (
    customerID INT PRIMARY KEY,
    customerName VARCHAR(100) UNIQUE,
    gender VARCHAR(10)
);

CREATE TABLE Time_Dimension (
    timeID INT PRIMARY KEY,
    orderDate DATE NOT NULL,
    orderTime TIME NOT NULL,
    Day INT NOT NULL,
    Month INT NOT NULL,
    Quarter INT NOT NULL,
    Year INT NOT NULL
);

CREATE TABLE Dummy_Table (
    productID INT PRIMARY KEY,
    productName VARCHAR(100) UNIQUE,
    productPrice DECIMAL(10, 2),
    supplierID INT,
    supplierName VARCHAR(100),
    storeID INT,
    storeName VARCHAR(100)
);

CREATE TABLE Sales_FactTable (
    orderID INT,
    productID INT,
    customerID INT,
    supplierID INT,
    storeID INT,
    orderDate DATE NOT NULL,
    timeID INT,
    quantityOrdered INT DEFAULT 0,
    totalSale DECIMAL(10, 2) DEFAULT 0,
    PRIMARY KEY (orderID, productID, customerID),
    FOREIGN KEY (productID) REFERENCES Product_Dimension(productID),
    FOREIGN KEY (customerID) REFERENCES Customer_Dimension(customerID),
    FOREIGN KEY (supplierID) REFERENCES Supplier_Dimension(supplierID),
    FOREIGN KEY (storeID) REFERENCES Store_Dimension(storeID)
);

LOAD DATA INFILE 'X:/5th Semester/DW BI/DW/customers_data.csv'
INTO TABLE Customer_Dimension
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"' 
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(customerID, customerName, gender);

LOAD DATA INFILE 'X:/5th Semester/DW BI/DW/products_data.csv'
INTO TABLE Dummy_Table
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"' 
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(productID, productName, productPrice, supplierID, supplierName, storeID, storeName);

INSERT INTO Product_Dimension (productID, productName, productPrice)
SELECT DISTINCT productID, productName, productPrice
FROM Dummy_Table
WHERE productID IS NOT NULL;

INSERT INTO Supplier_Dimension (supplierID, supplierName)
SELECT DISTINCT supplierID, supplierName
FROM Dummy_Table
WHERE supplierID IS NOT NULL;

INSERT INTO Store_Dimension (storeID, storeName)
SELECT DISTINCT storeID, storeName
FROM Dummy_Table
WHERE storeID IS NOT NULL;

--  Enrichment Table Containing All the Data
/*
DROP TABLE IF EXISTS Enriched_Table;
CREATE TABLE Enriched_Table AS
SELECT 
    sf.orderID,
    sf.productID,
    p.productName,
    p.productPrice,
    sf.customerID,
    c.customerName,
    c.gender,
    sf.supplierID,
    s.supplierName,
    sf.storeID,
    st.storeName,
    sf.timeID,
    t.orderDate,
    t.orderTime,
    t.Day,
    t.Month,
    t.Quarter,
    t.Year,
    sf.quantityOrdered,
    sf.totalSale
FROM 
    Sales_FactTable sf
JOIN 
    Product_Dimension p ON sf.productID = p.productID
JOIN 
    Customer_Dimension c ON sf.customerID = c.customerID
JOIN 
    Supplier_Dimension s ON sf.supplierID = s.supplierID
JOIN 
    Store_Dimension st ON sf.storeID = st.storeID
JOIN 
    Time_Dimension t ON sf.timeID = t.timeID;
*/