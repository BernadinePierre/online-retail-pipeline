"""
Data Ingestion Module for Online Retail Dataset
Fetches data from UCI ML Repository with local backup
"""
import os
import logging
import pandas as pd
from datetime import datetime
from ucimlrepo import fetch_ucirepo
import threading

logger = logging.getLogger(__name__)

class DataIngestion:

    def __init__(self, dataset_id=352, output_dir='data/raw'):
        self.dataset_id = dataset_id
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
        
    def fetch_data(self, save_local=True, timeout_seconds=300):
        """Fetch data from UCI API first, falls back to local file if needed."""
       
        logger.info("Initiated data ingestion...")
        
        try:
            logger.info(f"Attempting UCI ML Repo API (timeout: {timeout_seconds}s)...")
            
            # Use threading to implement timeout
            result = []
            exception = []
            
            def fetch_worker():
                try:
                    online_retail = fetch_ucirepo(id=self.dataset_id)
                    result.append(online_retail)
                except Exception as e:
                    exception.append(e)
            
            thread = threading.Thread(target=fetch_worker)
            thread.daemon = True
            thread.start()
            thread.join(timeout=timeout_seconds)
            
            if thread.is_alive():
                logger.error(f"UCI API timed out after {timeout_seconds} seconds")
                raise TimeoutError(f"API call exceeded {timeout_seconds} seconds")
            
            if exception:
                raise exception[0]
                
            if not result:
                raise ValueError("No data received from UCI API")
                
            online_retail = result[0]
            
            # Extract dataframe from API response
            df = self._extract_dataframe(online_retail)
            self._validate_dataset(df)
            logger.info(f"UCI API successful: {len(df):,} rows, {len(df.columns)} columns")            
                        
        except (TimeoutError, Exception) as e:
            logger.warning(f"UCI API failed: {str(e)}")
            df = self._load_local_file()            

        # Save backup
        if save_local:
            output_path = self._save_raw_data(df)

        return df, output_path if save_local else None
    
    # ... rest of your methods remain the same
    def _extract_dataframe(self, online_retail):
        """Extract dataframe from UCI API response"""
        if hasattr(online_retail.data, 'original') and online_retail.data.original is not None:
            return online_retail.data.original
        elif hasattr(online_retail.data, 'features') and online_retail.data.features is not None:
            return online_retail.data.features
        else:
            raise ValueError("No data found in UCI API response")         

    def _validate_dataset(self, df):
        """Validate dataset has expected structure"""
        expected_cols = ['InvoiceNo', 'StockCode', 'Description', 'Quantity', 
                        'InvoiceDate', 'UnitPrice', 'CustomerID', 'Country']
        
        missing_cols = [col for col in expected_cols if col not in df.columns]
        if missing_cols:
            raise ValueError(f"Missing expected columns: {missing_cols}")

    def _load_local_file(self):
        """Fallback to local Excel file"""
        logger.info("Using local Excel file as fallback...")
        
        local_file = f"{self.output_dir}/Online_Retail.xlsx"
        if not os.path.exists(local_file):
            raise FileNotFoundError(
                f"Local file not found. Please download manually from:\n"
                f"https://archive.ics.uci.edu/ml/datasets/Online+Retail\n"
                f"and save as: {local_file}"
            )
        
        df = pd.read_excel(local_file)
        logger.info(f"Local file loaded: {len(df):,} rows, {len(df.columns)} columns")
        return df
    
    def _save_raw_data(self, df):
        """Save raw data with timestamp"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = f"{self.output_dir}/Online_Retail_raw_{timestamp}.csv"
        df.to_csv(output_path, index=False)
        logger.info(f"Raw data saved: {output_path}")
        return output_path
        
def main():
    """Main execution function for data ingestion"""
    logger.info("=" * 50)
    logger.info("DATA INGESTION PROCESS")
    logger.info("=" * 50)
    
    try:
        ingestion = DataIngestion()
        df, output_path = ingestion.fetch_data(save_local=True)
        
        logger.info("✓ Data ingestion completed successfully")
        
        return df, output_path
        
    except Exception as e:
        logger.error(f"Data ingestion failed: {str(e)}")
        raise

if __name__ == "__main__":
    df, output_path = main()