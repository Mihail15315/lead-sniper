"""
Utility functions for data collection and processing
"""
import re
import time
import random
import logging
from typing import Optional, Dict, Any, List
from datetime import datetime
from fake_useragent import UserAgent
import requests
from bs4 import BeautifulSoup


class Utils:
    """Utility class for common operations"""
    
    @staticmethod
    def normalize_text(text: Optional[str]) -> str:
        """Normalize text: remove extra spaces, normalize case"""
        if not text:
            return ""
        # Remove extra whitespace
        text = re.sub(r'\s+', ' ', text.strip())
        return text
    
    @staticmethod
    def parse_revenue(revenue_text: str) -> Optional[int]:
        """
        Parse revenue from text format to integer
        Supports formats: "100 млн ₽", "1 234 567 ₽", "10.5 млрд"
        """
        if not revenue_text:
            return None
        
        try:
            # Remove all spaces and special characters except dots and commas
            clean_text = re.sub(r'[^\d.,]', '', revenue_text)
            
            # Convert comma to dot for decimal parsing
            clean_text = clean_text.replace(',', '.')
            
            # Check for миллион/миллиард suffixes
            if 'млн' in revenue_text.lower() or 'million' in revenue_text.lower():
                multiplier = 1_000_000
            elif 'млрд' in revenue_text.lower() or 'billion' in revenue_text.lower():
                multiplier = 1_000_000_000
            elif 'тыс' in revenue_text.lower() or 'thousand' in revenue_text.lower():
                multiplier = 1_000
            else:
                multiplier = 1
            
            # Parse the number
            if clean_text:
                value = float(clean_text)
                return int(value * multiplier)
            else:
                return None
                
        except (ValueError, AttributeError) as e:
            logging.warning(f"Error parsing revenue '{revenue_text}': {e}")
            return None
    
    @staticmethod
    def parse_employees(employees_text: str) -> Optional[int]:
        """Parse number of employees from text"""
        if not employees_text:
            return None
        
        try:
            # Extract first number found
            match = re.search(r'(\d[\d\s]*)', employees_text)
            if match:
                # Remove spaces from numbers like "1 234"
                num_str = match.group(1).replace(' ', '')
                return int(num_str)
        except (ValueError, AttributeError) as e:
            logging.warning(f"Error parsing employees '{employees_text}': {e}")
        
        return None
    
    @staticmethod
    def extract_inn(text: str) -> Optional[str]:
        """Extract INN (ИНН) from text"""
        if not text:
            return None
        
        # ИНН может быть 10 или 12 цифр
        inn_patterns = [
            r'ИНН\s*[:=]?\s*(\d{10}|\d{12})',
            r'(\d{10}|\d{12})(?=\D|$)',
            r'inn\s*[:=]?\s*(\d{10}|\d{12})',
        ]
        
        for pattern in inn_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                return match.group(1)
        
        return None
    
    @staticmethod
    def validate_inn(inn: str) -> bool:
        """Validate INN format"""
        if not inn or not isinstance(inn, str):
            return False
        
        # Check if it's all digits
        if not inn.isdigit():
            return False
        
        # Check length
        if len(inn) not in [10, 12]:
            return False
        
        return True
    
    @staticmethod
    def normalize_okved(okved_text: str) -> str:
        """Normalize OKVED code"""
        if not okved_text:
            return ""
        
        # Extract just the code part
        match = re.search(r'(\d{2}\.?\d{0,2}\.?\d{0,2})', okved_text)
        if match:
            code = match.group(1)
            # Remove dots for consistency
            return code.replace('.', '')
        
        return okved_text.strip()
    
    @staticmethod
    def get_random_user_agent() -> str:
        """Get random user agent for request rotation"""
        ua = UserAgent()
        return ua.random
    
    @staticmethod
    def delay(min_seconds: float = 1, max_seconds: float = 3):
        """Add random delay between requests"""
        time.sleep(random.uniform(min_seconds, max_seconds))
    
    @staticmethod
    def extract_domain(url: str) -> str:
        """Extract domain from URL"""
        if not url:
            return ""
        
        # Remove protocol and path
        domain = re.sub(r'^https?://', '', url)
        domain = re.sub(r'/.*$', '', domain)
        return domain
    
    @staticmethod
    def is_valid_url(url: str) -> bool:
        """Check if URL is valid"""
        if not url:
            return False
        
        url_pattern = re.compile(
            r'^https?://'  # http:// or https://
            r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+[A-Z]{2,6}\.?|'  # domain...
            r'localhost|'  # localhost...
            r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'  # ...or ip
            r'(?::\d+)?'  # optional port
            r'(?:/?|[/?]\S+)$', re.IGNORECASE)
        
        return bool(url_pattern.match(url))


class WebScraper:
    """Base class for web scraping operations"""
    
    def __init__(self, config):
        self.config = config
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': Utils.get_random_user_agent(),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
        })
    
    def fetch_page(self, url: str, params: Optional[Dict] = None) -> Optional[BeautifulSoup]:
        """Fetch and parse webpage with retries"""
        for attempt in range(self.config.parsing.max_retries):
            try:
                Utils.delay(0.5, 1.5)  # Respectful delay
                
                if self.config.parsing.user_agent_rotation:
                    self.session.headers['User-Agent'] = Utils.get_random_user_agent()
                
                response = self.session.get(
                    url,
                    params=params,
                    timeout=self.config.parsing.timeout
                )
                response.raise_for_status()
                
                # Check if we got HTML
                if 'text/html' not in response.headers.get('Content-Type', ''):
                    logging.warning(f"Non-HTML response from {url}")
                    return None
                
                return BeautifulSoup(response.content, 'lxml')
                
            except requests.exceptions.RequestException as e:
                logging.warning(f"Attempt {attempt + 1} failed for {url}: {e}")
                if attempt < self.config.parsing.max_retries - 1:
                    time.sleep(2 ** attempt)  # Exponential backoff
                else:
                    logging.error(f"Failed to fetch {url} after {self.config.parsing.max_retries} attempts")
        
        return None