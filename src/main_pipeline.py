"""
Main Pipeline Script for Online Retail Data Pipeline
Orchestrates the complete ETL process from ingestion to modelling
"""
import os
import sys
import uuid
import logging
from datetime import datetime

import os
print(f"Script location: {os.path.abspath(__file__)}")
print(f"Current working directory: {os.getcwd()}")
print(f"Project root should be: {os.path.dirname(os.path.abspath(__file__))}")

class PipelineRun:
    def __init__(self, job_id=None):
        self.job_id = job_id or f"{uuid.uuid4().hex[:8]}"
        self.logger = None
        self.setup_logging()
        
    def setup_logging(self):
        """Setup logging with job ID at initialisation"""
        
        logs_dir = 'data/logs'
        os.makedirs(logs_dir, exist_ok=True)

        root_logger = logging.getLogger()
        root_logger.setLevel(logging.INFO)        
      
        # Create formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )

        # File handler with timestamped filename in data/logs
        log_filename = f"pipeline_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        log_filepath = os.path.join(logs_dir, log_filename)
        file_handler = logging.FileHandler(log_filepath)
        file_handler.setFormatter(formatter)
        
        # Stream handler for console
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        
        # Add handlers to root logger
        root_logger.addHandler(file_handler)
        root_logger.addHandler(stream_handler)
        
        self.logger = logging.getLogger(__name__)
        self.logger.info("Pipeline run initialised with Job ID: %s", self.job_id)
        return self.logger

def run_pipeline(job_id=None):
    """Main pipeline execution function"""
    
    # Initialise pipeline run
    pipeline = PipelineRun(job_id)
    logger = pipeline.logger
    
    logger.info("=" * 50)
    logger.info("ONLINE RETAIL DATA PIPELINE - STARTING")
    logger.info("=" * 50)
    
    try:
        # Create directories
        directories = [
            'data/raw',
            'data/profiling',
            'data/processed', 
            'data/model',
            'data/logs'
        ]
        
        for directory in directories:
            os.makedirs(directory, exist_ok=True)
        
        # Step 1: Data Ingestion
        logger.info("=" * 50)
        logger.info("STEP 1: DATA INGESTION")
        logger.info("=" * 50)
        
        from data_ingestion import main as ingest_data
        raw_df, raw_path = ingest_data()
        
        # Step 2: Data Profiling
        logger.info("=" * 50)
        logger.info("STEP 2: DATA PROFILING")
        logger.info("=" * 50)
        
        from data_profiling import DataProfiler
        profiler = DataProfiler(pipeline.job_id)
        quality_summary, history_metrics = profiler.generate_profile_report(raw_df)
        
        # Step 3: Data Cleaning & Transformation
        logger.info("=" * 50)
        logger.info("STEP 3: DATA CLEANING & TRANSFORMATION")
        logger.info("=" * 50)
        
        from data_cleaning import DataCleaner
        cleaner = DataCleaner()
        cleaned_df, cleaning_report = cleaner.clean_data(raw_df)
                    
        # Save cleaned data
        cleaned_path = f"data/processed/cleaned_data_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
        cleaned_df.to_csv(cleaned_path, index=False)
        logger.info("✓ Cleaned data saved: %s", cleaned_path)
        
        # Step 4: Data modelling
        logger.info("=" * 50)
        logger.info("STEP 4: DATA modelling")
        logger.info("=" * 50)
        
        from data_modelling import DataModeller
        modeller = DataModeller()
        modelling_report = modeller.create_star_schema(cleaned_df)
        
        # Save to Parquet and SQLite
        modeller.save_to_files(format='parquet')
        db_path = modeller.save_to_sqlite()
        logger.info("✓ Dimensional model saved to: %s", db_path)

        profile_path = profiler.profile_history_path
        
        # Pipeline Summary
        logger.info("=" * 50)
        logger.info("PIPELINE EXECUTION SUMMARY")
        logger.info("=" * 50)
        logger.info("✓ Ingested data: %s", raw_path)
        logger.info("✓ Profiling history updated: %s", profile_path)
        logger.info("✓ Cleaned data: %s", cleaned_path)
        logger.info("✓ Database with schema: %s", db_path)
        logger.info("Pipeline completed successfully")
        
        return {
            'job_id': pipeline.job_id,
            'raw_data_path': raw_path,
            'cleaned_data_path': cleaned_path,
            'database_path': db_path,
            'quality_summary': quality_summary,
            'history_metrics': history_metrics,
            'success': True
        }
        
    except Exception as e:
        logger.error("Pipeline failed: %s", str(e))
        return {
            'job_id': pipeline.job_id if 'pipeline' in locals() else 'unknown',
            'success': False,
            'error': str(e)
        }

def main():
    """
    Command-line entry point
    Use when running the script directly.
    """
    results = run_pipeline()
    
    print("\n" + "=" * 60)
    print("PIPELINE EXECUTION COMPLETE")
    print("=" * 60)
    
    if results['success']:
        print("All pipeline steps completed successfully")
        print(f"Job ID: {results.get('job_id', 'N/A')}")
    else:
        print("Pipeline failed")
        print(f"Job ID: {results.get('job_id', 'N/A')}")
        print(f"Error: {results.get('error', 'Unknown error')}")
        sys.exit(1)

if __name__ == "__main__":
    main()