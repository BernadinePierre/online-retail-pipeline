# Data Dictionary

## Overview
This document defines the complete data model for the Online Retail analytical database, including table structures, column definitions, and business rules implemented in the dimensional star schema.

---

## Dimension Tables

### dim_date
**Purpose**: Time dimension for date-based analysis  
**Grain**: One row per calendar day  
**Row Count**: 374 records

| Column Name | Data Type | Description | Example | Business Rules |
|------------|-----------|-------------|---------|----------------|
| date_key | INTEGER | Surrogate key (YYYYMMDD format) | 20101201 | Primary key, NOT NULL |
| full_date | DATE | Actual calendar date | 2010-12-01 | NOT NULL, UNIQUE |
| year | INTEGER | Year | 2010 | NOT NULL |
| quarter | INTEGER | Quarter (1-4) | 4 | NOT NULL, 1-4 |
| month | INTEGER | Month number | 12 | NOT NULL, 1-12 |
| month_name | VARCHAR(20) | Month name | December | NOT NULL |
| day | INTEGER | Day of month | 1 | NOT NULL, 1-31 |
| day_of_week | INTEGER | Day of week (0=Monday) | 2 | NOT NULL, 0-6 |
| day_name | VARCHAR(20) | Day name | Wednesday | NOT NULL |
| is_weekend | BOOLEAN | Weekend flag | FALSE | NOT NULL |

---

### dim_product
**Purpose**: Product master data  
**Grain**: One row per unique product (StockCode)  
**Row Count**: 3,938 records

| Column Name | Data Type | Description | Example | Business Rules |
|------------|-----------|-------------|---------|----------------|
| product_key | INTEGER | Surrogate key | 1 | Primary key, NOT NULL |
| stock_code | VARCHAR(20) | Product identifier | 85123A | NOT NULL, UNIQUE |
| description | VARCHAR(500) | Product name | WHITE HANGING HEART T-LIGHT HOLDER | NOT NULL, "Unknown Product" if missing |
| first_seen_date | DATE | First transaction date | 2010-12-01 | NOT NULL |
| last_seen_date | DATE | Most recent transaction | 2011-12-09 | NOT NULL |
| is_active | BOOLEAN | Currently active flag | TRUE | NOT NULL, Default TRUE |

**Notes**:
- StockCode format varies: numeric (85123), alphanumeric (85123A), manual codes (POST, M, D)
- Manual codes represent adjustments, postage, or discounts
- Description may be "Unknown Product" for 0.27% of records

---

### dim_customer
**Purpose**: Customer master data  
**Grain**: One row per unique customer  
**Row Count**: 4,372 records

| Column Name | Data Type | Description | Example | Business Rules |
|------------|-----------|-------------|---------|----------------|
| customer_key | INTEGER | Surrogate key | 1 | Primary key, NOT NULL |
| customer_id | INTEGER | Business key | 17850 | NOT NULL, 0 = Unknown customer |
| country | VARCHAR(100) | Customer country | United Kingdom | NOT NULL |
| first_purchase_date | DATE | First order date | 2010-12-01 | NOT NULL |
| last_purchase_date | DATE | Most recent order | 2011-12-09 | NOT NULL |
| is_unknown_customer | BOOLEAN | Unknown customer flag | FALSE | NOT NULL |

**Notes**:
- customer_id = 0 represents unknown/anonymous customers (24.9% of transactions)
- Unknown customers likely represent B2B wholesale orders or guest checkouts
- Country data is present even for unknown customers

**Countries Represented**: 38 countries, primarily UK-based

---

## Fact Table

### fact_sales
**Purpose**: Transactional sales data  
**Grain**: One row per transaction line item  
**Row Count**: 534,129 records

| Column Name | Data Type | Description | Example | Business Rules |
|------------|-----------|-------------|---------|----------------|
| transaction_key | INTEGER | Surrogate key | 1 | Primary key, NOT NULL |
| invoice_no | VARCHAR(20) | Invoice number | 536365 | NOT NULL, Prefix 'C' = cancellation |
| date_key | INTEGER | Foreign key to dim_date | 20101201 | NOT NULL, Must exist in dim_date |
| product_key | INTEGER | Foreign key to dim_product | 1 | NOT NULL, Must exist in dim_product |
| customer_key | INTEGER | Foreign key to dim_customer | 1 | NOT NULL, Must exist in dim_customer |
| quantity | INTEGER | Units sold | 6 | NOT NULL, Can be negative for returns |
| unit_price | DECIMAL(10,2) | Price per unit (GBP £) | 2.55 | NOT NULL, Must be > 0 |
| line_total | DECIMAL(10,2) | Calculated total | 15.30 | NOT NULL, quantity × unit_price |
| is_cancelled | BOOLEAN | Cancellation flag | FALSE | NOT NULL |
| high_quantity_flag | BOOLEAN | High quantity flag | FALSE | NOT NULL, TRUE if |quantity| > 10,000 |
| created_timestamp | TIMESTAMP | ETL load timestamp | 2025-10-31 00:35:21 | NOT NULL |

**Measures**:
- **quantity**: Additive, can aggregate by SUM
- **unit_price**: Non-additive, use AVG for aggregation
- **line_total**: Additive, primary revenue measure

**Business Rules**:
- line_total = quantity × unit_price (validated in pipeline)
- Cancelled transactions (is_cancelled = TRUE) should be excluded from revenue analysis
- Negative quantities with is_cancelled = FALSE represent returns
- High quantity flag identifies potential wholesale orders for review

---

## Relationships
```
dim_date (1) ----< (M) fact_sales
dim_product (1) ----< (M) fact_sales
dim_customer (1) ----< (M) fact_sales
```

**Referential Integrity**: 100% enforced via ETL validation in `src/data_modeling.py`

---

## Data Quality Metrics

| Metric | Value | Description |
|--------|-------|-------------|
| Total Source Records | 541,909 | Raw Excel file rows |
| Final Fact Records | 534,129 | After cleansing (98.56% pass rate) |
| Duplicates Removed | 5,268 | Exact duplicate rows |
| Invalid Prices Excluded | 2,512 | Zero or negative prices |
| Cancellations | 9,288 | Flagged, not excluded |
| Unknown Customers | 135,080 | Assigned customer_id = 0 |

---

## Analytical Use Cases

### 1. Revenue Analysis
```sql
SELECT 
    SUM(line_total) as total_revenue
FROM fact_sales
WHERE is_cancelled = FALSE;
```

### 2. Product Performance
```sql
SELECT 
    p.description,
    SUM(f.quantity) as units_sold,
    SUM(f.line_total) as revenue
FROM fact_sales f
JOIN dim_product p ON f.product_key = p.product_key
WHERE f.is_cancelled = FALSE
GROUP BY p.description
ORDER BY revenue DESC;
```

### 3. Customer Segmentation
```sql
SELECT 
    c.country,
    COUNT(DISTINCT c.customer_key) as customer_count,
    SUM(f.line_total) as revenue
FROM fact_sales f
JOIN dim_customer c ON f.customer_key = c.customer_key
WHERE f.is_cancelled = FALSE
GROUP BY c.country;
```

### 4. Time-Based Trends
```sql
SELECT 
    d.year,
    d.month_name,
    SUM(f.line_total) as revenue
FROM fact_sales f
JOIN dim_date d ON f.date_key = d.date_key
WHERE f.is_cancelled = FALSE
GROUP BY d.year, d.month, d.month_name
ORDER BY d.year, d.month;
```

---

## Data Refresh Strategy

**Current Implementation**: Full refresh from source

**Recommended Production Approach**:
1. Incremental load based on InvoiceDate
2. Slowly Changing Dimension (Type 2) for product price changes
3. Soft deletes for cancelled transactions
4. Daily refresh at 2 AM UTC

---

## Glossary

| Term | Definition |
|------|------------|
| **Cancellation** | Transaction with invoice number starting with 'C', represents refund/return |
| **Line Item** | Single product within an invoice (invoice can have multiple line items) |
| **Surrogate Key** | System-generated identifier, independent of business keys |
| **Star Schema** | Dimensional model with central fact table and surrounding dimensions |
| **Wholesale Customer** | High-volume buyer, often has missing CustomerID in source data |
| **Unknown Customer** | Customer with missing ID in source, assigned customer_id = 0 |

---

**Document Version**: 2.0  
**Last Updated**: November 3, 2025  