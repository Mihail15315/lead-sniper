"""
Main module for data collection pipeline
"""
import sys
import os
import logging
from datetime import datetime
import pandas as pd

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.config import Config
from src.collector import CompanyCollector
from src.parser import CompanyParser
from src.cat_detector import CATDetector
from src.utils import Utils


class DataCollectionPipeline:
    """Main pipeline for collecting and processing company data"""
    
    def __init__(self, config_path: str = "config.json"):
        """Initialize the pipeline"""
        self.config = Config(config_path)
        self.collector = CompanyCollector(self.config)
        self.parser = CompanyParser(self.config)
        self.cat_detector = CATDetector(self.config)
        self.companies = []
        
        # Setup output directory
        os.makedirs('data', exist_ok=True)
        
        logging.info("=" * 60)
        logging.info("Data Collection Pipeline Initialized")
        logging.info(f"Minimum revenue threshold: {self.config.search_params.min_revenue:,} ₽")
        logging.info(f"Target year: {self.config.search_params.revenue_year}")
        logging.info("=" * 60)
    
    def run(self):
        """Run the complete data collection pipeline"""
        try:
            # Step 1: Collect companies
            self.collect_companies()
            
            # Step 2: Parse and normalize data
            self.parse_companies()
            
            # Step 3: Filter by revenue threshold
            self.filter_companies()
            
            # Step 4: Detect CAT system usage
            self.detect_cat_systems()
            
            # Step 5: Filter companies with CAT evidence
            self.filter_by_cat_evidence()
            
            # Step 6: Save results
            self.save_results()
            
            # Step 7: Generate report
            self.generate_report()
            
            logging.info("=" * 60)
            logging.info("Pipeline completed successfully!")
            logging.info("=" * 60)
            
        except Exception as e:
            logging.error(f"Pipeline failed: {e}")
            raise
    
    def collect_companies(self):
        """Collect companies from various sources"""
        logging.info("Step 1: Collecting companies...")
        
        collected_companies = []
        
        # Method 1: Search by OKVED codes
        logging.info("Searching companies by OKVED codes...")
        okved_companies = self.collector.search_companies_by_okved([], limit=100)
        collected_companies.extend(okved_companies)
        logging.info(f"Found {len(okved_companies)} companies by OKVED")
        
        Utils.delay(3, 5)
        
        # Method 2: Search by revenue threshold
        logging.info("Searching companies by revenue threshold...")
        revenue_companies = self.collector.get_companies_by_revenue_threshold(
            self.config.search_params.min_revenue
        )
        collected_companies.extend(revenue_companies)
        logging.info(f"Found {len(revenue_companies)} companies by revenue")
        
        Utils.delay(3, 5)
        
        # Method 3: Get from list-org
        logging.info("Collecting companies from list-org...")
        listorg_companies = self.collector.get_companies_from_listorg(limit=50)
        collected_companies.extend(listorg_companies)
        logging.info(f"Found {len(listorg_companies)} companies from list-org")
        
        # Remove duplicates by INN
        unique_companies = self.parser.deduplicate_companies(collected_companies)
        logging.info(f"Total unique companies collected: {len(unique_companies)}")
        
        self.companies = unique_companies
    
    def parse_companies(self):
        """Parse and normalize company data"""
        logging.info("Step 2: Parsing and normalizing company data...")
        
        if not self.companies:
            logging.warning("No companies to parse")
            return
        
        parsed_companies = self.parser.parse_multiple_companies(self.companies)
        
        # Filter by criteria (revenue threshold, valid INN, etc.)
        filtered_companies = self.parser.filter_companies_by_criteria(parsed_companies)
        
        # Enrich with additional data
        enriched_companies = self.parser.enrich_with_additional_data(filtered_companies)
        
        self.companies = enriched_companies
        logging.info(f"Parsed and filtered to {len(self.companies)} companies")
    
    def filter_companies(self):
        """Filter companies by revenue threshold"""
        logging.info("Step 3: Filtering companies by revenue threshold...")
        
        if not self.companies:
            return
        
        initial_count = len(self.companies)
        filtered_companies = [
            c for c in self.companies 
            if c.get('revenue', 0) >= self.config.search_params.min_revenue
        ]
        
        self.companies = filtered_companies
        logging.info(f"Filtered from {initial_count} to {len(self.companies)} companies with revenue ≥ {self.config.search_params.min_revenue:,} ₽")
    
    def detect_cat_systems(self):
        """Detect CAT system usage in companies"""
        logging.info("Step 4: Detecting CAT system usage...")
        
        if not self.companies:
            logging.warning("No companies to analyze for CAT systems")
            return
        
        # Analyze websites for CAT evidence
        companies_with_cat = self.cat_detector.analyze_multiple_companies(self.companies)
        
        self.companies = companies_with_cat
        logging.info(f"Analyzed {len(self.companies)} companies for CAT system usage")
    
    def filter_by_cat_evidence(self):
        """Filter companies with valid CAT evidence"""
        logging.info("Step 5: Filtering companies with CAT evidence...")
        
        if not self.companies:
            return
        
        initial_count = len(self.companies)
        
        # Define criteria for valid CAT evidence
        def has_valid_cat_evidence(company):
            evidence = company.get('cat_evidence', '').lower()
            cat_score = company.get('cat_score', 0)
            
            # Skip if no evidence or error messages
            if not evidence or 'не обнаружены' in evidence or 'ошибка' in evidence or 'не удалось' in evidence:
                return False
            
            # Require minimum score or specific keywords
            if cat_score >= 2:
                return True
            
            # Check for strong keywords even with low score
            strong_keywords = ['trados', 'memoq', 'smartcat', 'memsource', 'phrase']
            if any(keyword in evidence for keyword in strong_keywords):
                return True
            
            return False
        
        filtered_companies = [c for c in self.companies if has_valid_cat_evidence(c)]
        
        self.companies = filtered_companies
        logging.info(f"Filtered from {initial_count} to {len(self.companies)} companies with CAT evidence")
    
    def save_results(self):
        """Save results to CSV file"""
        logging.info("Step 6: Saving results...")
        
        if not self.companies:
            logging.warning("No companies to save")
            return
        
        # Prepare data for CSV
        csv_data = []
        for company in self.companies:
            csv_row = {
                'inn': company.get('inn', ''),
                'name': company.get('name', ''),
                'revenue': company.get('revenue', ''),
                'site': company.get('site', ''),
                'cat_evidence': company.get('cat_evidence', ''),
                'source': company.get('source', ''),
                'cat_product': company.get('cat_product', ''),
                'employees': company.get('employees', ''),
                'okved_main': company.get('okved_main', ''),
                'detail_url': company.get('detail_url', ''),
                'cat_score': company.get('cat_score', 0),
                'size_category': company.get('size_category', ''),
                'revenue_category': company.get('revenue_category', ''),
                'parsed_at': company.get('parsed_at', '')
            }
            csv_data.append(csv_row)
        
        # Create DataFrame
        df = pd.DataFrame(csv_data)
        
        # Reorder columns for better readability
        column_order = [
            'inn', 'name', 'revenue', 'site', 'cat_evidence', 'cat_product',
            'cat_score', 'employees', 'okved_main', 'source', 'detail_url',
            'size_category', 'revenue_category', 'parsed_at'
        ]
        
        # Ensure all columns exist
        existing_columns = [col for col in column_order if col in df.columns]
        df = df[existing_columns]
        
        # Save to CSV
        output_path = 'data/companies.csv'
        df.to_csv(output_path, 
                 index=False, 
                 encoding=self.config.output.csv_encoding,
                 sep=self.config.output.csv_separator)
        
        logging.info(f"Saved {len(df)} companies to {output_path}")
        
        # Also save as Excel for easier viewing
        excel_path = 'data/companies.xlsx'
        df.to_excel(excel_path, index=False)
        logging.info(f"Saved to Excel: {excel_path}")
    
    def generate_report(self):
        """Generate a summary report"""
        logging.info("Step 7: Generating report...")
        
        if not self.companies:
            logging.warning("No data for report")
            return
        
        report = {
            'total_companies': len(self.companies),
            'total_revenue_sum': sum(c.get('revenue', 0) for c in self.companies),
            'avg_revenue': sum(c.get('revenue', 0) for c in self.companies) / len(self.companies) if self.companies else 0,
            'min_revenue': min(c.get('revenue', 0) for c in self.companies) if self.companies else 0,
            'max_revenue': max(c.get('revenue', 0) for c in self.companies) if self.companies else 0,
            'total_employees': sum(c.get('employees', 0) for c in self.companies if c.get('employees')),
            'sources': {},
            'top_cat_products': {},
            'okved_distribution': {}
        }
        
        # Analyze sources
        for company in self.companies:
            source = company.get('source', 'unknown')
            report['sources'][source] = report['sources'].get(source, 0) + 1
        
        # Analyze CAT products
        for company in self.companies:
            products = company.get('cat_product', '')
            if products:
                for product in products.split(','):
                    product = product.strip()
                    if product:
                        report['top_cat_products'][product] = report['top_cat_products'].get(product, 0) + 1
        
        # Analyze OKVED distribution
        for company in self.companies:
            okved = company.get('okved_main', '')
            if okved:
                report['okved_distribution'][okved] = report['okved_distribution'].get(okved, 0) + 1
        
        # Print report
        logging.info("\n" + "=" * 60)
        logging.info("DATA COLLECTION REPORT")
        logging.info("=" * 60)
        logging.info(f"Total companies found: {report['total_companies']}")
        logging.info(f"Total revenue sum: {report['total_revenue_sum']:,.0f} ₽")
        logging.info(f"Average revenue: {report['avg_revenue']:,.0f} ₽")
        logging.info(f"Revenue range: {report['min_revenue']:,.0f} - {report['max_revenue']:,.0f} ₽")
        logging.info(f"Total employees: {report['total_employees']:,}")
        
        logging.info("\nData sources:")
        for source, count in sorted(report['sources'].items(), key=lambda x: x[1], reverse=True):
            logging.info(f"  {source}: {count} companies")
        
        if report['top_cat_products']:
            logging.info("\nTop CAT products found:")
            for product, count in sorted(report['top_cat_products'].items(), key=lambda x: x[1], reverse=True)[:10]:
                logging.info(f"  {product}: {count} companies")
        
        if report['okved_distribution']:
            logging.info("\nTop OKVED codes:")
            for okved, count in sorted(report['okved_distribution'].items(), key=lambda x: x[1], reverse=True)[:5]:
                logging.info(f"  {okved}: {count} companies")
        
        # Save report to file
        report_path = 'data/report.txt'
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write("=" * 60 + "\n")
            f.write("DATA COLLECTION REPORT\n")
            f.write("=" * 60 + "\n\n")
            f.write(f"Generated at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            f.write(f"Total companies found: {report['total_companies']}\n")
            f.write(f"Total revenue sum: {report['total_revenue_sum']:,.0f} ₽\n")
            f.write(f"Average revenue: {report['avg_revenue']:,.0f} ₽\n")
            f.write(f"Revenue range: {report['min_revenue']:,.0f} - {report['max_revenue']:,.0f} ₽\n")
            f.write(f"Total employees: {report['total_employees']:,}\n\n")
            
            f.write("Data sources:\n")
            for source, count in sorted(report['sources'].items(), key=lambda x: x[1], reverse=True):
                f.write(f"  {source}: {count} companies\n")
            
            if report['top_cat_products']:
                f.write("\nTop CAT products found:\n")
                for product, count in sorted(report['top_cat_products'].items(), key=lambda x: x[1], reverse=True)[:10]:
                    f.write(f"  {product}: {count} companies\n")
        
        logging.info(f"\nDetailed report saved to: {report_path}")


def main():
    """Main entry point"""
    try:
        # Initialize and run pipeline
        pipeline = DataCollectionPipeline()
        pipeline.run()
        
    except KeyboardInterrupt:
        logging.info("\nPipeline interrupted by user")
        sys.exit(0)
    except Exception as e:
        logging.error(f"Fatal error in pipeline: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()