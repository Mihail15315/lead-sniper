"""
Module for parsing and normalizing company data
"""
import logging
import re
from typing import List, Dict, Any, Optional
from datetime import datetime
from tqdm import tqdm
from .utils import Utils
from .config import Config


class CompanyParser:
    """Parses and normalizes company data"""
    
    def __init__(self, config: Config):
        self.config = config
    
    def parse_company_data(self, raw_company: Dict) -> Dict[str, Any]:
        """Parse and normalize single company data"""
        try:
            # Normalize INN
            inn = raw_company.get('inn', '')
            if inn and isinstance(inn, str):
                inn = Utils.extract_inn(inn) or inn
            inn = str(inn).strip() if inn else ''
            
            # Validate INN
            if not Utils.validate_inn(inn):
                logging.warning(f"Invalid INN: {inn} for company {raw_company.get('name')}")
                return {}
            
            # Normalize name
            name = Utils.normalize_text(raw_company.get('name', ''))
            if not name:
                return {}
            
            # Parse revenue
            revenue = self.parse_revenue_field(raw_company.get('revenue'))
            if revenue is None or revenue < self.config.search_params.min_revenue:
                logging.debug(f"Company {name} revenue {revenue} below threshold")
                return {}
            
            # Normalize website
            site = raw_company.get('site', '')
            if site and isinstance(site, str):
                site = site.strip()
                # Ensure URL has protocol
                if site and not site.startswith(('http://', 'https://')):
                    site = 'https://' + site
            
            # Parse employees
            employees = self.parse_employees_field(raw_company.get('employees'))
            
            # Normalize OKVED
            okved_main = Utils.normalize_okved(raw_company.get('okved_main', ''))
            
            # Get source
            source = raw_company.get('source', 'unknown')
            
            # Prepare parsed company
            parsed_company = {
                'inn': inn,
                'name': name,
                'revenue': revenue,
                'site': site,
                'okved_main': okved_main,
                'employees': employees,
                'source': source,
                'detail_url': raw_company.get('detail_url', ''),
                'parsed_at': datetime.now().isoformat()
            }
            
            return parsed_company
            
        except Exception as e:
            logging.error(f"Error parsing company data: {e}")
            return {}
    
    def parse_revenue_field(self, revenue_input) -> Optional[int]:
        """Parse revenue from various input formats"""
        if revenue_input is None:
            return None
        
        try:
            if isinstance(revenue_input, (int, float)):
                return int(revenue_input)
            
            if isinstance(revenue_input, str):
                return Utils.parse_revenue(revenue_input)
            
            # Try to convert if it's another type
            return int(float(revenue_input))
            
        except (ValueError, TypeError) as e:
            logging.debug(f"Error parsing revenue {revenue_input}: {e}")
            return None
    
    def parse_employees_field(self, employees_input) -> Optional[int]:
        """Parse employees count from various input formats"""
        if employees_input is None:
            return None
        
        try:
            if isinstance(employees_input, int):
                return employees_input
            
            if isinstance(employees_input, str):
                return Utils.parse_employees(employees_input)
            
            # Try to convert if it's another type
            return int(float(employees_input))
            
        except (ValueError, TypeError) as e:
            logging.debug(f"Error parsing employees {employees_input}: {e}")
            return None
    
    def parse_multiple_companies(self, raw_companies: List[Dict]) -> List[Dict]:
        """Parse and normalize multiple companies"""
        parsed_companies = []
        
        for raw_company in tqdm(raw_companies, desc="Parsing company data"):
            try:
                parsed_company = self.parse_company_data(raw_company)
                if parsed_company:  # Only add if parsing succeeded
                    parsed_companies.append(parsed_company)
            except Exception as e:
                logging.error(f"Error parsing company: {e}")
                continue
        
        return parsed_companies
    
    def filter_companies_by_criteria(self, companies: List[Dict]) -> List[Dict]:
        """Filter companies based on criteria"""
        filtered_companies = []
        
        for company in companies:
            try:
                # Check revenue threshold
                revenue = company.get('revenue', 0)
                if revenue < self.config.search_params.min_revenue:
                    continue
                
                # Check for valid INN
                inn = company.get('inn', '')
                if not Utils.validate_inn(inn):
                    continue
                
                # Check for company name
                if not company.get('name'):
                    continue
                
                filtered_companies.append(company)
                
            except Exception as e:
                logging.error(f"Error filtering company: {e}")
                continue
        
        logging.info(f"Filtered {len(filtered_companies)} companies meeting criteria")
        return filtered_companies
    
    def deduplicate_companies(self, companies: List[Dict]) -> List[Dict]:
        """Remove duplicate companies by INN"""
        seen_inns = set()
        unique_companies = []
        
        for company in companies:
            inn = company.get('inn', '')
            if inn and inn not in seen_inns:
                seen_inns.add(inn)
                unique_companies.append(company)
            elif inn:
                logging.debug(f"Duplicate INN found: {inn}")
        
        logging.info(f"Removed {len(companies) - len(unique_companies)} duplicates")
        return unique_companies
    
    def enrich_with_additional_data(self, companies: List[Dict]) -> List[Dict]:
        """Enrich companies with additional data if available"""
        enriched_companies = []
        
        for company in tqdm(companies, desc="Enriching company data"):
            try:
                enriched_company = company.copy()
                
                # Add company size category based on employees
                employees = company.get('employees')
                if employees:
                    if employees <= 15:
                        size_category = "micro"
                    elif employees <= 100:
                        size_category = "small"
                    elif employees <= 250:
                        size_category = "medium"
                    else:
                        size_category = "large"
                    enriched_company['size_category'] = size_category
                
                # Add revenue category
                revenue = company.get('revenue', 0)
                if revenue >= 1_000_000_000:  # 1 billion
                    revenue_category = "large"
                elif revenue >= 500_000_000:  # 500 million
                    revenue_category = "medium"
                else:
                    revenue_category = "small"
                enriched_company['revenue_category'] = revenue_category
                
                # Clean website URL
                site = company.get('site', '')
                if site:
                    # Remove tracking parameters
                    site = re.sub(r'\?.*$', '', site)
                    enriched_company['site'] = site
                
                enriched_companies.append(enriched_company)
                
            except Exception as e:
                logging.error(f"Error enriching company {company.get('name')}: {e}")
                enriched_companies.append(company)
        
        return enriched_companies