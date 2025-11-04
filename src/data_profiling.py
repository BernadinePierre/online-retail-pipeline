"""
Data Profiling Module for Online Retail Dataset
Generates data quality metrics and saves reports to text files
"""

import os
import sys
import pandas as pd
import logging
from datetime import datetime
from config import get_data_path

logger = logging.getLogger(__name__)

class DataProfiler:
    """Handles data quality assessment and metric generation"""
    
    def __init__(self, job_id, output_dir='data/profiling'):
        self.job_id = job_id
        self.output_dir = output_dir
        self.output_dir = get_data_path('profiling')
        self.profile_history_path = get_data_path('profiling', 'profiling_history.csv')
        os.makedirs(self.output_dir, exist_ok=True)

    def save_profile_report_to_file(self, quality_summary):
        """Save detailed profile report to a text file"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        report_filename = f"profile_report_{timestamp}.txt"

        reports_dir = os.path.join(self.output_dir, 'reports')
        os.makedirs(reports_dir, exist_ok=True)
    
        report_path = os.path.join(reports_dir, report_filename)
        
        with open(report_path, 'w') as f:
            f.write("=" * 50 + "\n")
            f.write(f"DATA PROFILE REPORT - Job ID: {self.job_id}\n")
            f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write("=" * 50 + "\n\n")
            
            # Dataset Overview
            f.write("DATASET OVERVIEW\n")
            f.write("-" * 50 + "\n")
            overview = quality_summary['dataset_overview']
            f.write(f"Total Rows: {overview['row_count']:,}\n")
            f.write(f"Total Columns: {overview['column_count']}\n")
            f.write(f"Memory Usage: {overview['memory_usage_mb']} MB\n")
            f.write(f"Overall Completeness Score: {quality_summary['completeness']['completeness_score']}%\n\n")
            
            # Column Types
            f.write("COLUMN DATA TYPES\n")
            f.write("-" * 50 + "\n")
            for col, dtype in quality_summary['column_types'].items():
                f.write(f"{col}: {dtype}\n")
            f.write("\n")
            
            # Completeness Analysis
            f.write("COMPLETENESS ANALYSIS\n")
            f.write("-" * 50 + "\n")
            completeness = quality_summary['completeness']
            f.write("Missing Values by Column:\n")
            for col, missing_count in completeness['missing_values'].items():
                missing_pct = completeness['missing_percentage'][col]
                f.write(f"  {col}: {missing_count:,} ({missing_pct}%)\n")
            f.write("\n")
            
            # Data Quality Issues
            f.write("DATA QUALITY ISSUES\n")
            f.write("-" * 50 + "\n")
            issues = quality_summary['data_quality_issues']
            f.write(f"Duplicate Rows: {issues['duplicate_rows']:,}\n")
            f.write(f"Negative Quantities: {issues['negative_quantities']:,}\n")
            f.write(f"Zero Quantities: {issues['zero_quantities']:,}\n")
            f.write(f"Negative Prices: {issues['negative_prices']:,}\n")
            f.write(f"Zero Prices: {issues['zero_prices']:,}\n")
            f.write(f"Missing Customer IDs: {issues['missing_customer_ids']:,}\n")
            f.write(f"Missing Descriptions: {issues['missing_descriptions']:,}\n\n")
            
            # Business Logic Constraints
            f.write("BUSINESS LOGIC CONSTRAINTS\n")
            f.write("-" * 50 + "\n")
            constraints = quality_summary['business_logic_constraints']
            for constraint in constraints:
                f.write(f"Constraint: {constraint['constraint']}\n")
                f.write(f"  Count: {constraint['count']:,}\n")
                f.write(f"  Action Needed: {constraint['action_needed']}\n")
                if 'percentage' in constraint:
                    f.write(f"  Percentage: {constraint['percentage']}%\n")
                f.write("\n")
        
        logger.info(f"✓ Profile report saved to: {report_path}")
        return report_path

    def save_to_profile_history(self, quality_summary):
        """Append comprehensive profiling results to history CSV"""
        # Extract all relevant metrics for the history
        new_row = {
            'job_id': self.job_id,
            'run_timestamp': datetime.now(),
            'total_rows': quality_summary['dataset_overview']['row_count'],
            'total_columns': quality_summary['dataset_overview']['column_count'],
            'memory_usage_mb': quality_summary['dataset_overview']['memory_usage_mb'],
            'completeness_score': quality_summary['completeness']['completeness_score'],
            
            # Data quality metrics
            'duplicate_rows': quality_summary['data_quality_issues']['duplicate_rows'],
            'negative_quantities': quality_summary['data_quality_issues']['negative_quantities'],
            'invalid_prices': quality_summary['data_quality_issues']['negative_prices'],
            'zero_prices': quality_summary['data_quality_issues']['zero_prices'],
            'missing_customer_ids': quality_summary['data_quality_issues']['missing_customer_ids'],
            'missing_descriptions': quality_summary['data_quality_issues']['missing_descriptions'],
            
            # Missing values percentages
            'missing_customer_pct': quality_summary['completeness']['missing_percentage'].get('CustomerID', 0),
            'missing_description_pct': quality_summary['completeness']['missing_percentage'].get('Description', 0),
            
            # Business constraint counts
            'cancellation_count': self._get_constraint_count(quality_summary, 'Cancellation transactions'),
            'extreme_quantities_count': self._get_constraint_count(quality_summary, 'Extreme Quantities'),
        }
        
        # Create or append to profiling history
        if os.path.exists(self.profile_history_path):
            history_df = pd.read_csv(self.profile_history_path)
            history_df = pd.concat([history_df, pd.DataFrame([new_row])], ignore_index=True)
        else:
            history_df = pd.DataFrame([new_row])
        
        history_df.to_csv(self.profile_history_path, index=False)
        logger.info("✓ Updated profiling history with comprehensive metrics for job: %s", self.job_id)
        
        return new_row
    
    def _get_constraint_count(self, quality_summary, constraint_name):
        """Helper to get count for specific business constraint"""
        for constraint in quality_summary['business_logic_constraints']:
            if constraint['constraint'] == constraint_name:
                return constraint['count']
        return 0

    def generate_profile_report(self, df):
        """Generate data quality metrics and save to files"""
        logger.info("Generating data quality metrics...")
        
        # Generate quality summary
        quality_summary = self.generate_quality_summary(df)
        
        # Save report to file
        report_path = self.save_profile_report_to_file(quality_summary)
        
        # Update profiling history with comprehensive metrics
        history_metrics = self.save_to_profile_history(quality_summary)
        
        # Log quality report
        self.log_quality_report(quality_summary)
        
        logger.info("✓ Data profiling completed successfully")
        logger.info(f"✓ Report saved: {report_path}")
        
        # Return quality_summary and history_metrics for backward compatibility
        return quality_summary, history_metrics
    
    def generate_quality_summary(self, df):
        """Generate detailed data quality summary"""
        logger.info("Generating data quality summary...")
        
        summary = {
            'dataset_overview': {
                'row_count': len(df),
                'column_count': len(df.columns),
                'memory_usage_mb': round(df.memory_usage(deep=True).sum() / 1024**2, 2)
            },
            'column_types': df.dtypes.astype(str).to_dict(),
            'completeness': {
                'missing_values': df.isnull().sum().to_dict(),
                'missing_percentage': (df.isnull().sum() / len(df) * 100).round(2).to_dict(),
                'completeness_score': round((1 - df.isnull().sum().sum() / (len(df) * len(df.columns))) * 100, 2)
            },
            'data_quality_issues': {
                'duplicate_rows': df.duplicated().sum(),
                'negative_quantities': (df['Quantity'] < 0).sum() if 'Quantity' in df.columns else 0,
                'zero_quantities': (df['Quantity'] == 0).sum() if 'Quantity' in df.columns else 0,
                'negative_prices': (df['UnitPrice'] <= 0).sum() if 'UnitPrice' in df.columns else 0,
                'zero_prices': (df['UnitPrice'] == 0).sum() if 'UnitPrice' in df.columns else 0,
                'missing_customer_ids': df['CustomerID'].isnull().sum() if 'CustomerID' in df.columns else 0,
                'missing_descriptions': df['Description'].isnull().sum() if 'Description' in df.columns else 0
            },
            'business_logic_constraints': self._identify_business_constraints(df)
        }
        
        return summary
    
    def _identify_business_constraints(self, df):
        """Identify business logic constraints that need to be applied"""
        constraints = []
        
        # Rule 1: Cancellations handling
        if 'InvoiceNo' in df.columns:
            cancellation_count = df['InvoiceNo'].astype(str).str.startswith('C').sum()
            constraints.append({
                'constraint': 'Cancellation transactions',
                'count': int(cancellation_count),
                'action_needed': 'Flag as cancellations but keep for refund analysis'
            })
        
        # Rule 2: Missing CustomerIDs
        if 'CustomerID' in df.columns:
            missing_customers = df['CustomerID'].isnull().sum()
            constraints.append({
                'constraint': 'Missing CustomerIDs',
                'count': int(missing_customers),
                'percentage': round((missing_customers / len(df)) * 100, 2),
                'action_needed': 'Assign surrogate key (0) for unknown customers'
            })
        
        # Rule 3: Negative Quantities
        if 'Quantity' in df.columns:
            negative_qty = (df['Quantity'] < 0).sum()
            constraints.append({
                'constraint': 'Negative Quantities',
                'count': int(negative_qty),
                'action_needed': 'Validate against cancellation flag; keep legitimate returns'
            })
        
        # Rule 4: Invalid Prices
        if 'UnitPrice' in df.columns:
            invalid_prices = (df['UnitPrice'] <= 0).sum()
            constraints.append({
                'constraint': 'Invalid Prices (≤ 0)',
                'count': int(invalid_prices),
                'action_needed': 'Exclude from fact table as they represent data errors'
            })
        
        # Rule 5: Extreme Quantities
        if 'Quantity' in df.columns:
            high_quantities = (df['Quantity'].abs() > 10000).sum()
            constraints.append({
                'constraint': 'Extreme Quantities',
                'count': int(high_quantities),
                'action_needed': 'Flag for business review but keep for wholesale analysis'
            })
        
        # Rule 6: Missing Descriptions
        if 'Description' in df.columns:
            missing_desc = df['Description'].isnull().sum()
            if missing_desc > 0:
                constraints.append({
                    'constraint': 'Missing Product Descriptions',
                    'count': int(missing_desc),
                    'action_needed': 'Fill with "Unknown Product" placeholder'
                })
        
        return constraints
    
    def log_quality_report(self, quality_summary):
        """Log comprehensive quality report"""
        logger.info("=" * 50)
        logger.info("DATA QUALITY ASSESSMENT REPORT")
        logger.info("=" * 50)
        
        logger.info("Dataset Overview:")
        logger.info("  Rows: %s", f"{quality_summary['dataset_overview']['row_count']:,}")
        logger.info("  Columns: %s", quality_summary['dataset_overview']['column_count'])
        logger.info("  Completeness: %s", f"{quality_summary['completeness']['completeness_score']}%")
        
        logger.info("Key Data Quality Issues:")
        issues = quality_summary['data_quality_issues']
        logger.info("  Missing CustomerIDs: %s", f"{issues['missing_customer_ids']:,}")
        logger.info("  Duplicate Rows: %s", f"{issues['duplicate_rows']:,}")
        logger.info("  Invalid Prices: %s", f"{issues['negative_prices']:,}")
        
        logger.info("Business Constraints Identified: %s", len(quality_summary['business_logic_constraints']))

def main(input_file_path=None, df=None, job_id=None):
    """Main function for standalone profiling execution"""
    if job_id is None:
        job_id = f"standalone_{datetime.now().strftime('%Y%m%d_%H%M')}"
    
    logger.info("=" * 50)
    logger.info("DATA PROFILING MODULE")
    logger.info("=" * 50)
    
    try:
        # Load data if not provided
        if df is None and input_file_path:
            logger.info("Loading data from: %s", input_file_path)
            df = pd.read_csv(input_file_path)
        elif df is None:
            raise ValueError("Either provide a DataFrame or input file path")
        
        # Initialise profiler
        profiler = DataProfiler(job_id)
        
        # Generate quality metrics
        quality_summary, history_metrics = profiler.generate_profile_report(df)
        
        return quality_summary, history_metrics
        
    except Exception as e:
        logger.error("Data profiling failed: %s", str(e))
        raise

if __name__ == "__main__":
    # Standalone execution for testing
    if len(sys.argv) > 1:
        input_path = sys.argv[1]
        quality_summary, history_metrics = main(input_file_path=input_path)
    else:
        raw_data_dir = 'data/raw'
        if os.path.exists(raw_data_dir):
            files = [f for f in os.listdir(raw_data_dir) if f.startswith('Online_Retail_raw')]
            if files:
                latest_file = sorted(files)[-1]
                input_path = os.path.join(raw_data_dir, latest_file)
                quality_summary, history_metrics = main(input_file_path=input_path)
            else:
                logger.error("No raw data files found. Run data_ingestion.py first.")
        else:
            logger.error("Raw data directory not found. Run data_ingestion.py first.")
    
    print("\n" + "=" * 50)
    print("PROFILING RESULTS")
    print("=" * 50)
    print(f"Profiling history updated")
    print(f"Key issues found: {len(quality_summary['business_logic_constraints'])}")
    print(f"Report saved to: data/profiling/reports")