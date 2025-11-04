# Data Transformation Rules

## Overview

This document defines the business logic and transformation rules applied in the Online Retail data pipeline. The rules ensure data quality while preserving business context for analytical use cases.

## Pipeline Architecture Context
The transformations are applied in this sequence:

Raw Data → Data Profiling → Data Cleaning → Dimensional Modeling → Multi-Format Storage

## Business Logic Decisions

### 1. Cancellations
- **Decision**: Keep cancellations but flag them separately
- **Rationale**: Analysts need to track refund patterns
- **Implementation**: Add `is_cancelled` boolean flag

### 2. Missing CustomerIDs
- **Decision**: Keep records but assign surrogate key for "Unknown Customer"
- **Rationale**: These may be B2B/wholesale orders; significant revenue
- **Implementation**: Create CustomerID = 0 for unknown customers and Flag for business team review

### 3. Negative Quantities
- **Decision**: Keep if associated with cancellation invoice, flag others for review
- **Rationale**: Returns are legitimate business transactions
- **Implementation**: Validate against cancellation flag

### 4. Invalid Prices
- **Decision**: Exclude from fact table, log as data quality error
- **Rationale**: Cannot have meaningful transaction without valid price
- **Implementation**: Filter out, maintain error log

### 5. Duplicate Records
- **Decision**: Remove exact duplicates, keep first occurrence
- **Rationale**: Likely data loading errors
- **Implementation**: Drop duplicates based on all columns

### 6. Missing Descriptions
- **Decision**: Keep records, use "Unknown Product" as description
- **Rationale**: StockCode is the key identifier
- **Implementation**: Fill nulls with placeholder

### 7. Extreme Quantities
- **Decision**: Keep but flag for review
- **Rationale**: Wholesale business has legitimate large orders
- **Implementation**: Add `high_quantity_flag` for quantities > 10,000

### 7. Date Standardisation
- **Decision**: Convert all date formats to standardized datetime
- **Rationale**: Enable consistent time-based analysis and dimension joining
- **Implementation**: Parse InvoiceDate to datetime format during cleaning