"""
Data Cleaning and Transformation Module
Applies business logic rules to clean and prepare raw data
"""
import pandas as pd
import logging
from config import get_data_path
from datetime import datetime

logger = logging.getLogger(__name__)
  
class DataCleaner:
    """Handles data cleaning and transformation with business logic"""
    
    def __init__(self):
        self.cleaning_report = {}
        self.quality_stats = {}
    
    def clean_data(self, df):
        """Main cleaning method applying all transformation rules"""
        logger.info("Starting data cleaning process...")
        
        df_clean = df.copy()
        self.quality_stats['initial_rows'] = len(df_clean)

        # Ensure InvoiceDate is datetime first
        df_clean = self._convert_to_datetime(df_clean)
        
        # Apply cleaning steps
        df_clean = self._add_cancellation_flag(df_clean)
        df_clean = self._handle_missing_customer_ids(df_clean)
        df_clean = self._handle_missing_descriptions(df_clean)
        df_clean = self._remove_duplicates(df_clean)
        df_clean = self._filter_invalid_prices(df_clean)
        df_clean = self._calculate_line_totals(df_clean)
        df_clean = self._add_quality_flags(df_clean)
        df_clean = self._extract_date_components(df_clean)
        df_clean = self._clean_country_names(df_clean)
        
        # Generate final report
        self._generate_cleaning_report(df_clean)
        
        return df_clean, self.cleaning_report
    
    def _convert_to_datetime(self, df):
        """Convert InvoiceDate to datetime format"""
        logger.info("Converting InvoiceDate to datetime...")
        
        if 'InvoiceDate' in df.columns:
            # Handle potential datetime conversion issues
            df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate'], errors='coerce')
            
            # Check for conversion failures
            failed_conversions = df['InvoiceDate'].isnull().sum()
            if failed_conversions > 0:
                logger.warning(f"  Failed to convert {failed_conversions} dates to datetime")
        
        return df
    
    def _add_cancellation_flag(self, df):
        """Rule 1: Add cancellation flag for InvoiceNo starting with 'C'"""
        logger.info("Adding cancellation flags...")
        df['IsCancelled'] = df['InvoiceNo'].astype(str).str.startswith('C')
        cancelled_count = df['IsCancelled'].sum()
        logger.info(f"  Found {cancelled_count:,} cancelled transactions")
        self.quality_stats['cancelled_transactions'] = cancelled_count
        return df
    
    def _handle_missing_customer_ids(self, df):
        """Rule 2: Handle missing CustomerIDs by assigning surrogate key 0"""
        logger.info("Handling missing CustomerIDs...")
        missing_customers = df['CustomerID'].isnull().sum()
        logger.info(f"  Found {missing_customers:,} records with missing CustomerID")
        df['CustomerID'] = df['CustomerID'].fillna(0).astype(int)
        self.quality_stats['missing_customer_ids'] = missing_customers
        return df
    
    def _handle_missing_descriptions(self, df):
        """Rule 6: Fill missing descriptions with 'Unknown Product'"""
        logger.info("Handling missing descriptions...")
        missing_desc = df['Description'].isnull().sum()
        if missing_desc > 0:
            logger.info(f"  Found {missing_desc:,} missing descriptions")
            df['Description'] = df['Description'].fillna('Unknown Product')
        self.quality_stats['missing_descriptions'] = missing_desc
        return df
    
    def _remove_duplicates(self, df):
        """Rule 5: Remove exact duplicate records"""
        logger.info("Removing duplicates...")
        initial_count = len(df)
        df = df.drop_duplicates()
        duplicates_removed = initial_count - len(df)
        logger.info(f"  Removed {duplicates_removed:,} duplicate rows")
        self.quality_stats['duplicates_removed'] = duplicates_removed
        return df
    
    def _filter_invalid_prices(self, df):
        """Rule 4: Filter out records with invalid prices (≤ 0)"""
        logger.info("Filtering invalid prices...")
        invalid_prices = (df['UnitPrice'] <= 0).sum()
        logger.info(f"  Found {invalid_prices:,} records with invalid prices")
        df_valid = df[df['UnitPrice'] > 0].copy()
        excluded_price = len(df) - len(df_valid)
        logger.info(f"  Excluded {excluded_price:,} records")
        self.quality_stats['invalid_price_exclusions'] = excluded_price
        return df_valid
    
    def _calculate_line_totals(self, df):
        """Calculate line total for each transaction"""
        logger.info("Calculating line totals...")
        df['LineTotal'] = df['Quantity'] * df['UnitPrice']
        return df
    
    def _add_quality_flags(self, df):
        """Rule 8: Add quality flags for unusual quantities"""
        logger.info("Adding data quality flags...")
        df['HighQuantityFlag'] = df['Quantity'].abs() > 10000
        high_qty_count = df['HighQuantityFlag'].sum()
        logger.info(f"  Flagged {high_qty_count:,} high-quantity records for review")
        self.quality_stats['high_quantity_records'] = high_qty_count
        return df
    
    def _extract_date_components(self, df):
        """Extract date components for dimension table preparation"""
        logger.info("Extracting date components...")
        df['InvoiceYear'] = df['InvoiceDate'].dt.year
        df['InvoiceMonth'] = df['InvoiceDate'].dt.month
        df['InvoiceDay'] = df['InvoiceDate'].dt.day
        df['InvoiceDayOfWeek'] = df['InvoiceDate'].dt.dayofweek
        df['InvoiceQuarter'] = df['InvoiceDate'].dt.quarter
        return df
    
    def _clean_country_names(self, df):
        """Standardise country names"""
        logger.info("Standardising country names...")
        df['Country'] = df['Country'].str.strip().str.title()
        return df
    
    def _generate_cleaning_report(self, df_clean):
        """Generate comprehensive cleaning report"""
        self.cleaning_report = {
            'initial_rows': self.quality_stats['initial_rows'],
            'final_rows': len(df_clean),
            'rows_removed': self.quality_stats['initial_rows'] - len(df_clean),
            'data_quality_pass_rate': (len(df_clean) / self.quality_stats['initial_rows']) * 100,
            'cleaning_metrics': self.quality_stats
        }
        
        logger.info("Data cleaning completed successfully")
        logger.info(f"Initial rows: {self.cleaning_report['initial_rows']:,}")
        logger.info(f"Final rows: {self.cleaning_report['final_rows']:,}")
        logger.info(f"Pass rate: {self.cleaning_report['data_quality_pass_rate']:.2f}%")

def main(input_file_path=None, df=None):
    """Main function for standalone cleaning execution"""
    logger.info("=" * 50)
    logger.info("DATA CLEANING MODULE")
    logger.info("=" * 50)
    
    try:
        # Load data if not provided
        if df is None and input_file_path:
            logger.info(f"Loading data from: {input_file_path}")
            df = pd.read_csv(input_file_path)
        elif df is None:
            raise ValueError("Either provide a DataFrame or input file path")
        
        # Initialise cleaner and process data
        cleaner = DataCleaner()
        cleaned_df, cleaning_report = cleaner.clean_data(df)
        
        # Save cleaned data
        timestamp = datetime.now().strftime("%Y%m%d_%H%M")
        output_path = f"data/processed/cleaned_data_{timestamp}.csv"
        cleaned_df.to_csv(output_path, index=False)
        logger.info(f"✓ Cleaned data saved: {output_path}")
        
        return cleaned_df, cleaning_report, output_path
        
    except Exception as e:
        logger.error(f"Data cleaning failed: {str(e)}")
        raise
    
if __name__ == "__main__":
    # Example standalone execution
    import sys
    if len(sys.argv) > 1:
        input_path = sys.argv[1]
        cleaned_df, report, output_path = main(input_file_path=input_path)
    else:
        logger.error("Please provide input file path as argument")