"""
Data Modelling Module
Creates star schema with fact and dimension tables
"""
import pandas as pd
import logging
from datetime import datetime
import sqlite3
import os

logger = logging.getLogger(__name__)

class DataModeller:
    """Handles creation of dimensional data model"""
    
    def __init__(self, output_dir='data/model'):
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
        self.dim_date = None
        self.dim_product = None
        self.dim_customer = None
        self.fact_sales = None
    
    def create_star_schema(self, df):
        """Create complete star schema from cleaned data"""
        logger.info("Creating dimensional star schema...")
        
        # Create dimension tables
        self._create_date_dimension(df)
        self._create_product_dimension(df)
        self._create_customer_dimension(df)
        
        # Create fact table
        self._create_fact_table(df)
        
        # Ensure data types
        self._ensure_data_types()
        
        # Generate modelling report
        modelling_report = self._generate_modelling_report()
        
        return modelling_report
    
    def _create_date_dimension(self, df):
        """Create date dimension table"""
        logger.info("Creating dim_date...")
        
        # Generate complete date range
        min_date = df['InvoiceDate'].min().date()
        max_date = df['InvoiceDate'].max().date()
        
        dates = pd.DataFrame({
            'full_date': pd.date_range(start=min_date, end=max_date, freq='D')
        })
        
        # Add date attributes
        dates['date_key'] = dates['full_date'].dt.strftime('%Y%m%d').astype(int)
        dates['year'] = dates['full_date'].dt.year
        dates['quarter'] = dates['full_date'].dt.quarter
        dates['month'] = dates['full_date'].dt.month
        dates['month_name'] = dates['full_date'].dt.month_name()
        dates['day'] = dates['full_date'].dt.day
        dates['day_of_week'] = dates['full_date'].dt.dayofweek
        dates['day_name'] = dates['full_date'].dt.day_name()
        dates['is_weekend'] = dates['day_of_week'].isin([5, 6])
        
        self.dim_date = dates[['date_key', 'full_date', 'year', 'quarter', 'month', 
                              'month_name', 'day', 'day_of_week', 'day_name', 'is_weekend']]
        logger.info(f"  Created {len(self.dim_date):,} date records")
    
    def _create_product_dimension(self, df):
        """Create product dimension table"""
        logger.info("Creating dim_product...")
        
        # Ensure StockCode is string before grouping
        df['StockCode'] = df['StockCode'].astype(str)
        
        # Group by StockCode to get unique products
        products = df.groupby('StockCode').agg({
            'Description': 'first',
            'InvoiceDate': ['min', 'max']
        }).reset_index()
        
        # Flatten column names
        products.columns = ['stock_code', 'description', 'first_seen_date', 'last_seen_date']
        
        # Add surrogate key and flags
        products['product_key'] = range(1, len(products) + 1)
        products['is_active'] = True
        
        self.dim_product = products[['product_key', 'stock_code', 'description', 
                                    'first_seen_date', 'last_seen_date', 'is_active']]
        logger.info(f"  Created {len(self.dim_product):,} product records")
    
    def _create_customer_dimension(self, df):
        """Create customer dimension table"""
        logger.info("Creating dim_customer...")
        
        customers = df.groupby('CustomerID').agg({
            'Country': 'first',
            'InvoiceDate': ['min', 'max']
        }).reset_index()
        
        # Flatten column names
        customers.columns = ['customer_id', 'country', 'first_purchase_date', 'last_purchase_date']
        
        # Add surrogate key and flags
        customers['customer_key'] = range(1, len(customers) + 1)
        customers['is_unknown_customer'] = customers['customer_id'] == 0
        
        self.dim_customer = customers[['customer_key', 'customer_id', 'country',
                                      'first_purchase_date', 'last_purchase_date', 'is_unknown_customer']]
        logger.info(f"  Created {len(self.dim_customer):,} customer records")
    
    def _create_fact_table(self, df):
        """Create fact table with foreign keys"""
        logger.info("Creating fact_sales...")
        
        fact_df = df.copy()
        
        # Ensure StockCode is string for merging
        fact_df['StockCode'] = fact_df['StockCode'].astype(str)
        
        # Create date_key for joining
        fact_df['date_key'] = fact_df['InvoiceDate'].dt.strftime('%Y%m%d').astype(int)
        
        # Merge with dimensions to get surrogate keys
        fact_df = fact_df.merge(
            self.dim_product[['product_key', 'stock_code']],
            left_on='StockCode',
            right_on='stock_code',
            how='left'
        )
        
        fact_df = fact_df.merge(
            self.dim_customer[['customer_key', 'customer_id']],
            left_on='CustomerID',
            right_on='customer_id',
            how='left'
        )
        
        # Check for unmapped records
        unmapped_products = fact_df['product_key'].isnull().sum()
        unmapped_customers = fact_df['customer_key'].isnull().sum()
        
        if unmapped_products > 0:
            logger.warning(f"  Found {unmapped_products} unmapped products")
        if unmapped_customers > 0:
            logger.warning(f"  Found {unmapped_customers} unmapped customers")
        
        # Create fact table structure
        self.fact_sales = fact_df[[
            'date_key', 'product_key', 'customer_key',
            'Quantity', 'UnitPrice', 'LineTotal',
            'IsCancelled', 'HighQuantityFlag', 'InvoiceNo'
        ]].copy()
        
        # Rename columns
        self.fact_sales.columns = [
            'date_key', 'product_key', 'customer_key',
            'quantity', 'unit_price', 'line_total',
            'is_cancelled', 'high_quantity_flag', 'invoice_no'
        ]
        
        # Add transaction key and audit timestamp
        self.fact_sales.insert(0, 'transaction_key', range(1, len(self.fact_sales) + 1))
        self.fact_sales['timestamp'] = datetime.now()
        
        logger.info(f"  Created {len(self.fact_sales):,} transaction records")
    
    def _ensure_data_types(self):
        """Ensure correct data types for all tables"""
        logger.info("Ensuring correct data types...")
        
        # dim_date
        self.dim_date['date_key'] = self.dim_date['date_key'].astype(int)
        self.dim_date['year'] = self.dim_date['year'].astype(int)
        self.dim_date['quarter'] = self.dim_date['quarter'].astype(int)
        self.dim_date['month'] = self.dim_date['month'].astype(int)
        self.dim_date['day'] = self.dim_date['day'].astype(int)
        self.dim_date['day_of_week'] = self.dim_date['day_of_week'].astype(int)
        
        # dim_product
        self.dim_product['product_key'] = self.dim_product['product_key'].astype(int)
        self.dim_product['stock_code'] = self.dim_product['stock_code'].astype(str)
        self.dim_product['description'] = self.dim_product['description'].fillna('Unknown Product').astype(str)
        self.dim_product['is_active'] = self.dim_product['is_active'].astype(bool)
        
        # dim_customer
        self.dim_customer['customer_key'] = self.dim_customer['customer_key'].astype(int)
        self.dim_customer['customer_id'] = self.dim_customer['customer_id'].astype(int)
        self.dim_customer['country'] = self.dim_customer['country'].fillna('Unknown').astype(str)
        self.dim_customer['is_unknown_customer'] = self.dim_customer['is_unknown_customer'].astype(bool)
        
        # fact_sales
        self.fact_sales['transaction_key'] = self.fact_sales['transaction_key'].astype(int)
        self.fact_sales['date_key'] = self.fact_sales['date_key'].astype(int)
        self.fact_sales['product_key'] = self.fact_sales['product_key'].astype(int)
        self.fact_sales['customer_key'] = self.fact_sales['customer_key'].astype(int)
        self.fact_sales['quantity'] = self.fact_sales['quantity'].astype(int)
        self.fact_sales['unit_price'] = self.fact_sales['unit_price'].astype(float)
        self.fact_sales['line_total'] = self.fact_sales['line_total'].astype(float)
        self.fact_sales['is_cancelled'] = self.fact_sales['is_cancelled'].astype(bool)
        self.fact_sales['high_quantity_flag'] = self.fact_sales['high_quantity_flag'].astype(bool)
        
        logger.info("  Data types ensured")
    
    def save_to_files(self, format='parquet'):
        """Save dimensional model to files"""
        logger.info(f"Saving dimensional model as {format} files...")
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M")
        
        if format == 'parquet':
            self.dim_date.to_parquet(f'{self.output_dir}/dim_date/dim_date_{timestamp}.parquet', index=False)
            self.dim_product.to_parquet(f'{self.output_dir}/dim_product/dim_product_{timestamp}.parquet', index=False)
            self.dim_customer.to_parquet(f'{self.output_dir}/dim_customer/dim_customer_{timestamp}.parquet', index=False)
            self.fact_sales.to_parquet(f'{self.output_dir}//fact_sales/fact_sales_{timestamp}.parquet', index=False)
        else:
            self.dim_date.to_csv(f'{self.output_dir}/dim_date/dim_date_{timestamp}.csv', index=False)
            self.dim_product.to_csv(f'{self.output_dir}/dim_product/dim_product_{timestamp}.csv', index=False)
            self.dim_customer.to_csv(f'{self.output_dir}/dim_customer/dim_customer_{timestamp}.csv', index=False)
            self.fact_sales.to_csv(f'{self.output_dir}/fact_sales/fact_sales_{timestamp}.csv', index=False)
        
        logger.info(f"  Saved all tables to {self.output_dir}/")
    
    def save_to_sqlite(self, db_name='retail_analytics.db'):
        """Save dimensional model to SQLite database"""
        logger.info(f"Saving to SQLite database: {db_name}")
        
        db_path = f"{self.output_dir}/{db_name}"
        conn = sqlite3.connect(db_path)
        
        try:
            self.dim_date.to_sql('dim_date', conn, if_exists='replace', index=False)
            self.dim_product.to_sql('dim_product', conn, if_exists='replace', index=False)
            self.dim_customer.to_sql('dim_customer', conn, if_exists='replace', index=False)
            self.fact_sales.to_sql('fact_sales', conn, if_exists='replace', index=False)
            
            logger.info("  All tables saved to SQLite database")
        finally:
            conn.close()
        
        return db_path
    
    def _generate_modelling_report(self):
        """Generate modelling summary report"""
        report = {
            'tables_created': {
                'dim_date': len(self.dim_date),
                'dim_product': len(self.dim_product),
                'dim_customer': len(self.dim_customer),
                'fact_sales': len(self.fact_sales)
            },
            'schema_summary': {
                'date_range': f"{self.dim_date['full_date'].min()} to {self.dim_date['full_date'].max()}",
                'unique_products': len(self.dim_product),
                'unique_customers': len(self.dim_customer),
                'unknown_customers': self.dim_customer['is_unknown_customer'].sum()
            }
        }
        
        logger.info("Modelling completed successfully")
        logger.info(f"  dim_date: {report['tables_created']['dim_date']:,} rows")
        logger.info(f"  dim_product: {report['tables_created']['dim_product']:,} rows")
        logger.info(f"  dim_customer: {report['tables_created']['dim_customer']:,} rows")
        logger.info(f"  fact_sales: {report['tables_created']['fact_sales']:,} rows")
        
        return report

def main(input_file_path=None, df=None):
    """Main function for standalone modelling execution"""
    logger.info("=" * 50)
    logger.info("DATA MODELLING MODULE")
    logger.info("=" * 50)
    
    try:
        # Load data if not provided
        if df is None and input_file_path:
            logger.info(f"Loading data from: {input_file_path}")
            df = pd.read_csv(input_file_path)
            # Convert date columns back to datetime if needed
            if 'InvoiceDate' in df.columns:
                df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate'])
        elif df is None:
            raise ValueError("Either provide a DataFrame or input file path")
        
        # Initialise modeller and create schema
        modeller = DataModeller()
        modelling_report = modeller.create_star_schema(df)
        
        # Save to files and database
        modeller.save_to_files(format='parquet')
        db_path = modeller.save_to_sqlite()
        
        logger.info(f"✓ Dimensional model saved to: {db_path}")
        
        return modelling_report, db_path
        
    except Exception as e:
        logger.error(f"Data modelling failed: {str(e)}")
        raise

if __name__ == "__main__":
    # Example standalone execution
    import sys
    if len(sys.argv) > 1:
        input_path = sys.argv[1]
        report, db_path = main(input_file_path=input_path)
    else:
        logger.error("Please provide input file path as argument")