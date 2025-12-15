"""
Module for collecting company data from various sources
"""
import logging
import time
import re
from typing import List, Dict, Any, Optional
from tqdm import tqdm
from .utils import Utils, WebScraper
from .config import Config


class CompanyCollector(WebScraper):
    """Collects company data from Russian business directories"""
    
    def __init__(self, config: Config):
        super().__init__(config)
        self.config = config
    
    def search_companies_by_okved(self, okved_codes: List[str], limit: int = 200) -> List[Dict]:
        """
        Search companies by OKVED codes
        Focus on translation and localization services
        """
        companies = []
        
        # Вместо поиска по OKVED, ищем по ключевым словам
        search_keywords = [
            "переводческие услуги",
            "локализация",
            "перевод текстов", 
            "лингвистические услуги",
            "translation services",
            "переводчик"
        ]
        
        logging.info(f"Searching for companies by keywords: {search_keywords}")
        
        for keyword in tqdm(search_keywords, desc="Searching by keywords"):
            try:
                # Новый URL для поиска
                search_url = f"{self.config.data_sources.rusprofile_base_url}/search"
                params = {
                    'query': keyword,
                    'type': 'ul'
                }
                
                soup = self.fetch_page(search_url, params)
                if not soup:
                    continue
                
                # Пробуем новый способ извлечения данных
                company_links = soup.select('a[href*="/id/"]')
                for link in company_links[:20]:  # Ограничиваем
                    try:
                        company_url = f"{self.config.data_sources.rusprofile_base_url}{link['href']}"
                        company_data = self.get_company_details_by_url(company_url)
                        if company_data:
                            companies.append(company_data)
                            
                            if len(companies) >= limit:
                                logging.info(f"Reached limit of {limit} companies")
                                return companies
                                
                    except Exception as e:
                        logging.debug(f"Error processing company link: {e}")
                        continue
                        
                Utils.delay(3, 5)  # Увеличиваем задержку
                
            except Exception as e:
                logging.error(f"Error searching keyword '{keyword}': {e}")
                continue
        
        return companies
    
    def extract_company_from_card(self, card) -> Optional[Dict]:
        """Extract company data from search result card"""
        try:
            # Extract basic info
            name_elem = card.select_one('.company-name')
            inn_elem = card.select_one('.inn')
            
            if not name_elem or not inn_elem:
                return None
            
            name = Utils.normalize_text(name_elem.text)
            inn_text = Utils.normalize_text(inn_elem.text)
            inn = Utils.extract_inn(inn_text)
            
            if not inn or not Utils.validate_inn(inn):
                return None
            
            # Extract revenue if available
            revenue = None
            revenue_elem = card.select_one('.revenue')
            if revenue_elem:
                revenue = Utils.parse_revenue(revenue_elem.text)
            
            # Extract employees if available
            employees = None
            employees_elem = card.select_one('.employees')
            if employees_elem:
                employees = Utils.parse_employees(employees_elem.text)
            
            # Get company detail page URL
            link_elem = card.select_one('a[href*="/id/"]')
            detail_url = None
            if link_elem and 'href' in link_elem.attrs:
                detail_url = f"{self.config.data_sources.rusprofile_base_url}{link_elem['href']}"
            
            return {
                'inn': inn,
                'name': name,
                'revenue': revenue,
                'employees': employees,
                'detail_url': detail_url,
                'source': 'rusprofile_search'
            }
            
        except Exception as e:
            logging.debug(f"Error extracting company from card: {e}")
            return None
    
    def get_companies_by_revenue_threshold(self, min_revenue: int) -> List[Dict]:
        """
        Get companies with revenue above threshold from pre-filtered sources
        This is a simplified approach - in production you'd use paid APIs
        """
        companies = []
        
        # Search for companies in translation/localization sector
        search_terms = [
            "переводческ",
            "локализация",
            "лингвистическ",
            "translation",
            "localization",
            "языков",
            "перевод"
        ]
        
        for term in tqdm(search_terms, desc="Searching by keywords"):
            try:
                search_url = f"{self.config.data_sources.rusprofile_base_url}/search"
                params = {
                    'query': term,
                    'type': 'ul',
                    'sort': 'revenue_desc'
                }
                
                soup = self.fetch_page(search_url, params)
                if not soup:
                    continue
                
                # Get all company links
                company_links = soup.select('a[href*="/id/"]')
                for link in company_links[:30]:  # Limit per term
                    try:
                        company_url = f"{self.config.data_sources.rusprofile_base_url}{link['href']}"
                        company_data = self.get_company_details_by_url(company_url)
                        
                        if company_data and company_data.get('revenue', 0) >= min_revenue:
                            companies.append(company_data)
                            
                    except Exception as e:
                        logging.debug(f"Error processing company link: {e}")
                        continue
                
                Utils.delay(2, 3)
                
            except Exception as e:
                logging.error(f"Error searching term '{term}': {e}")
                continue
        
        return companies
    
    def get_company_details_by_url(self, url: str) -> Optional[Dict]:
        """Get detailed company information from company page"""
        try:
            soup = self.fetch_page(url)
            if not soup:
                return None
            
            # Extract company name
            name_elem = soup.select_one('h1')
            name = Utils.normalize_text(name_elem.text) if name_elem else ""
            
            # Extract INN
            inn = None
            inn_elements = soup.find_all(text=re.compile(r'ИНН\s*\d+'))
            for elem in inn_elements:
                extracted = Utils.extract_inn(elem)
                if extracted:
                    inn = extracted
                    break
            
            if not inn:
                # Try to find in data attributes
                data_inn = soup.select_one('[data-inn]')
                if data_inn:
                    inn = data_inn.get('data-inn')
            
            if not inn or not Utils.validate_inn(inn):
                return None
            
            # Extract revenue
            revenue = None
            revenue_section = soup.find(text=re.compile(r'Выручка|Revenue|Общие доходы', re.IGNORECASE))
            if revenue_section:
                # Look for revenue value in nearby elements
                for sibling in revenue_section.parent.find_next_siblings():
                    revenue_text = Utils.normalize_text(sibling.text)
                    parsed = Utils.parse_revenue(revenue_text)
                    if parsed:
                        revenue = parsed
                        break
            
            # Extract employees
            employees = None
            employees_section = soup.find(text=re.compile(r'Сотрудники|Employees|Численность', re.IGNORECASE))
            if employees_section:
                for sibling in employees_section.parent.find_next_siblings():
                    employees_text = Utils.normalize_text(sibling.text)
                    parsed = Utils.parse_employees(employees_text)
                    if parsed:
                        employees = parsed
                        break
            
            # Extract OKVED
            okved = ""
            okved_section = soup.find(text=re.compile(r'ОКВЭД|Основной вид деятельности', re.IGNORECASE))
            if okved_section:
                for sibling in okved_section.parent.find_next_siblings():
                    okved_text = Utils.normalize_text(sibling.text)
                    if re.search(r'\d{2}\.?\d{0,2}', okved_text):
                        okved = Utils.normalize_okved(okved_text)
                        break
            
            # Extract website
            website = ""
            website_elem = soup.select_one('a[href*="http"]:not([href*="rusprofile"])')
            if website_elem and 'href' in website_elem.attrs:
                website = website_elem['href'].strip()
                # Ensure it's a valid URL
                if not Utils.is_valid_url(website):
                    website = ""
            
            return {
                'inn': inn,
                'name': name,
                'revenue': revenue,
                'employees': employees,
                'okved_main': okved,
                'site': website,
                'detail_url': url,
                'source': 'rusprofile_detail'
            }
            
        except Exception as e:
            logging.error(f"Error getting company details from {url}: {e}")
            return None
    
    def get_companies_from_listorg(self, limit: int = 100) -> List[Dict]:
        """Get companies from list-org.com"""
        companies = []
        
        try:
            # Search for translation companies
            search_url = f"{self.config.data_sources.listorg_base_url}/search"
            params = {
                'val': 'перевод',
                'type': 'all'
            }
            
            soup = self.fetch_page(search_url, params)
            if not soup:
                return companies
            
            # Extract company links
            company_links = soup.select('a[href*="/company/"]')
            for link in tqdm(company_links[:limit], desc="Processing list-org companies"):
                try:
                    company_url = f"{self.config.data_sources.listorg_base_url}{link['href']}"
                    company_data = self.parse_listorg_company(company_url)
                    
                    if company_data:
                        companies.append(company_data)
                    
                    Utils.delay(1, 2)
                    
                except Exception as e:
                    logging.debug(f"Error processing list-org company: {e}")
                    continue
        
        except Exception as e:
            logging.error(f"Error getting companies from list-org: {e}")
        
        return companies
    
    def parse_listorg_company(self, url: str) -> Optional[Dict]:
        """Parse company page from list-org.com"""
        try:
            soup = self.fetch_page(url)
            if not soup:
                return None
            
            # Extract company name
            name_elem = soup.select_one('h1')
            name = Utils.normalize_text(name_elem.text) if name_elem else ""
            
            # Extract INN
            inn = None
            inn_elem = soup.find('td', text=re.compile(r'ИНН'))
            if inn_elem and inn_elem.find_next_sibling('td'):
                inn_text = inn_elem.find_next_sibling('td').text
                inn = Utils.extract_inn(inn_text)
            
            if not inn or not Utils.validate_inn(inn):
                return None
            
            # Extract revenue
            revenue = None
            revenue_elem = soup.find('td', text=re.compile(r'Выручка'))
            if revenue_elem and revenue_elem.find_next_sibling('td'):
                revenue_text = revenue_elem.find_next_sibling('td').text
                revenue = Utils.parse_revenue(revenue_text)
            
            # Extract employees
            employees = None
            employees_elem = soup.find('td', text=re.compile(r'Сотрудник'))
            if employees_elem and employees_elem.find_next_sibling('td'):
                employees_text = employees_elem.find_next_sibling('td').text
                employees = Utils.parse_employees(employees_text)
            
            # Extract OKVED
            okved = ""
            okved_elem = soup.find('td', text=re.compile(r'ОКВЭД'))
            if okved_elem and okved_elem.find_next_sibling('td'):
                okved_text = okved_elem.find_next_sibling('td').text
                okved = Utils.normalize_okved(okved_text)
            
            # Extract website
            website = ""
            website_elem = soup.select_one('a[href^="http"]')
            if website_elem:
                website = website_elem['href'].strip()
            
            return {
                'inn': inn,
                'name': name,
                'revenue': revenue,
                'employees': employees,
                'okved_main': okved,
                'site': website,
                'detail_url': url,
                'source': 'listorg'
            }
            
        except Exception as e:
            logging.error(f"Error parsing list-org company {url}: {e}")
            return None