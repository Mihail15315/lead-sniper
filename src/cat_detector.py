"""
Module for detecting CAT system usage in companies
"""
import re
import logging
from typing import Dict, Any, Optional, List, Tuple
from urllib.parse import urljoin
from tqdm import tqdm
from .utils import Utils, WebScraper
from .config import Config


class CATDetector(WebScraper):
    """Detects CAT system usage in company websites"""
    
    def __init__(self, config: Config):
        super().__init__(config)
        self.config = config
        
        # Extended list of CAT-related keywords
        self.cat_keywords = self.config.search_params.keywords_cat + [
            # Russian terms
            "память переводов", "tm-память", "tm память",
            "управление переводами", "система управления переводами",
            "платформа локализации", "локализационная платформа",
            "машинный перевод", "mt", "mt-система",
            "qa проверка", "quality assurance", "lqa",
            "терминологическое управление", "терминологическая база данных",
            "tbx", "xliff", "tmx", "sdlxliff",
            "переводческие технологии", "инструменты переводчика",
            
            # English terms
            "computer-assisted translation", "computer aided translation",
            "translation management system", "localization platform",
            "translation workflow", "translation project management",
            "translation memory system", "terminology management",
            "translation quality assurance", "translation environment tool",
            "multilingual content management",
            
            # Product names
            "trados studio", "sdl trados", "memoq server", "memoq web",
            "smartcat.ai", "smartcat platform", "memsource editor",
            "phrase tms", "xtm cloud", "xtm workflow", "wordfast pro",
            "transit nxt", "across language server", "lingotek",
            "prome", "promt", "translate.ru"
        ]
        
        # Common sections to check on websites
        self.target_sections = [
            "/services", "/technology", "/solutions", "/products",
            "/translation", "/localization", "/technology-stack",
            "/about", "/company", "/tools", "/software",
            "/methodology", "/process", "/workflow",
            "/careers", "/jobs", "/vacancies"  # For job descriptions
        ]
    
    def analyze_company_website(self, company: Dict) -> Dict[str, Any]:
        """
        Analyze company website for CAT system usage
        Returns enriched company data with CAT evidence
        """
        website = company.get('site', '')
        if not website or not Utils.is_valid_url(website):
            return {
                **company,
                'cat_evidence': 'Нет веб-сайта или неверный URL',
                'cat_product': '',
                'cat_score': 0
            }
        
        logging.info(f"Analyzing website: {website}")
        
        try:
            # Fetch main page
            main_page_soup = self.fetch_page(website)
            if not main_page_soup:
                return {
                    **company,
                    'cat_evidence': 'Не удалось загрузить сайт',
                    'cat_product': '',
                    'cat_score': 0
                }
            
            # Analyze main page
            main_page_text = main_page_soup.get_text(separator=' ', strip=True).lower()
            main_page_evidence = self.search_cat_evidence(main_page_text)
            
            # Check for job postings (often mention CAT tools)
            jobs_evidence = self.analyze_job_postings(website, main_page_soup)
            
            # Check target sections
            sections_evidence = self.analyze_website_sections(website)
            
            # Combine all evidence
            all_evidence = main_page_evidence + jobs_evidence + sections_evidence
            
            if not all_evidence:
                return {
                    **company,
                    'cat_evidence': 'Признаки CAT-систем не обнаружены',
                    'cat_product': '',
                    'cat_score': 0
                }
            
            # Calculate CAT score
            cat_score = len(all_evidence)
            
            # Extract CAT product names
            cat_products = self.extract_cat_products(all_evidence)
            
            # Prepare evidence description
            evidence_description = self.prepare_evidence_description(all_evidence)
            
            return {
                **company,
                'cat_evidence': evidence_description,
                'cat_product': ', '.join(cat_products) if cat_products else '',
                'cat_score': cat_score,
                'cat_keywords_found': '; '.join(set([e['keyword'] for e in all_evidence]))
            }
            
        except Exception as e:
            logging.error(f"Error analyzing website {website}: {e}")
            return {
                **company,
                'cat_evidence': f'Ошибка анализа: {str(e)}',
                'cat_product': '',
                'cat_score': 0
            }
    
    def search_cat_evidence(self, text: str) -> List[Dict]:
        """Search for CAT-related keywords in text"""
        evidence = []
        text_lower = text.lower()
        
        for keyword in self.cat_keywords:
            keyword_lower = keyword.lower()
            
            # Use regex to find whole word matches
            pattern = rf'\b{re.escape(keyword_lower)}\b'
            matches = re.finditer(pattern, text_lower)
            
            for match in matches:
                # Get context around the match
                start = max(0, match.start() - 50)
                end = min(len(text), match.end() + 50)
                context = text[start:end]
                
                evidence.append({
                    'keyword': keyword,
                    'context': context.strip(),
                    'section': 'main_page',
                    'confidence': self.get_keyword_confidence(keyword)
                })
        
        return evidence
    
    def get_keyword_confidence(self, keyword: str) -> float:
        """Get confidence score for a keyword"""
        # Product names have higher confidence
        product_keywords = ['trados', 'memoq', 'smartcat', 'memsource', 'phrase']
        for product in product_keywords:
            if product in keyword.lower():
                return 0.9
        
        # General CAT terms have medium confidence
        general_terms = ['translation memory', 'tm', 'tms', 'локализация']
        for term in general_terms:
            if term in keyword.lower():
                return 0.7
        
        # Other terms have lower confidence
        return 0.5
    
    def analyze_job_postings(self, base_url: str, main_soup) -> List[Dict]:
        """Analyze job postings for CAT tool mentions"""
        evidence = []
        
        try:
            # Find career/jobs section
            career_links = []
            for link in main_soup.find_all('a', href=True):
                link_text = link.get_text().lower()
                link_href = link['href'].lower()
                
                if any(word in link_text or word in link_href 
                      for word in ['career', 'job', 'vacancy', 'ваканс', 'карьер', 'работа']):
                    career_links.append(link['href'])
            
            # Analyze found career pages
            for link in career_links[:3]:  # Limit to first 3 links
                try:
                    full_url = urljoin(base_url, link)
                    jobs_soup = self.fetch_page(full_url)
                    
                    if jobs_soup:
                        jobs_text = jobs_soup.get_text(separator=' ', strip=True).lower()
                        job_evidence = self.search_cat_evidence(jobs_text)
                        
                        for ev in job_evidence:
                            ev['section'] = 'job_posting'
                            ev['confidence'] = min(1.0, ev['confidence'] + 0.1)  # Boost confidence for job postings
                        
                        evidence.extend(job_evidence)
                        
                except Exception as e:
                    logging.debug(f"Error analyzing job page {link}: {e}")
                    continue
        
        except Exception as e:
            logging.debug(f"Error finding career links: {e}")
        
        return evidence
    
    def analyze_website_sections(self, base_url: str) -> List[Dict]:
        """Analyze specific sections of the website"""
        evidence = []
        
        for section in self.target_sections[:5]:  # Limit to first 5 sections
            try:
                section_url = urljoin(base_url, section)
                section_soup = self.fetch_page(section_url)
                
                if section_soup:
                    section_text = section_soup.get_text(separator=' ', strip=True).lower()
                    section_evidence = self.search_cat_evidence(section_text)
                    
                    for ev in section_evidence:
                        ev['section'] = section
                    
                    evidence.extend(section_evidence)
                    
                    Utils.delay(0.5, 1)  # Small delay between sections
                    
            except Exception as e:
                logging.debug(f"Error analyzing section {section}: {e}")
                continue
        
        return evidence
    
    def extract_cat_products(self, evidence_list: List[Dict]) -> List[str]:
        """Extract CAT product names from evidence"""
        products_found = set()
        
        # Check evidence for known product names
        for ev in evidence_list:
            keyword = ev['keyword'].lower()
            context = ev['context'].lower()
            
            # Check against known products
            for product in self.config.search_params.cat_products:
                product_lower = product.lower()
                
                # Check if product name appears in keyword or context
                if (product_lower in keyword or 
                    re.search(rf'\b{re.escape(product_lower)}\b', context)):
                    products_found.add(product)
        
        return list(products_found)
    
    def prepare_evidence_description(self, evidence_list: List[Dict]) -> str:
        """Prepare human-readable evidence description"""
        if not evidence_list:
            return "Признаки CAT-систем не обнаружены"
        
        # Group by section
        sections = {}
        for ev in evidence_list:
            section = ev['section']
            if section not in sections:
                sections[section] = []
            sections[section].append(ev['keyword'])
        
        # Build description
        descriptions = []
        
        # Add strongest evidence first (product mentions)
        product_keywords = [ev['keyword'] for ev in evidence_list 
                          if any(p in ev['keyword'].lower() 
                                for p in ['trados', 'memoq', 'smartcat', 'memsource'])]
        
        if product_keywords:
            unique_products = list(set(product_keywords))[:3]  # Limit to 3
            descriptions.append(f"Упоминание продуктов: {', '.join(unique_products)}")
        
        # Add section evidence
        for section, keywords in sections.items():
            unique_keywords = list(set(keywords))[:5]  # Limit to 5 per section
            if unique_keywords:
                section_name = self.translate_section_name(section)
                descriptions.append(f"{section_name}: {', '.join(unique_keywords)}")
        
        # Limit total description length
        full_description = "; ".join(descriptions)
        if len(full_description) > 500:
            full_description = full_description[:497] + "..."
        
        return full_description
    
    def translate_section_name(self, section: str) -> str:
        """Translate section name to Russian"""
        translations = {
            'main_page': 'Главная страница',
            'job_posting': 'Вакансии',
            '/services': 'Раздел услуг',
            '/technology': 'Раздел технологий',
            '/products': 'Раздел продуктов',
            '/translation': 'Раздел переводов',
            '/about': 'О компании',
            '/careers': 'Карьера'
        }
        return translations.get(section, section)
    
    def analyze_multiple_companies(self, companies: List[Dict]) -> List[Dict]:
        """Analyze multiple companies for CAT system usage"""
        enriched_companies = []
        
        for company in tqdm(companies, desc="Analyzing websites for CAT systems"):
            try:
                enriched_company = self.analyze_company_website(company)
                enriched_companies.append(enriched_company)
                
                # Respectful delay between analyses
                Utils.delay(1, 2)
                
            except Exception as e:
                logging.error(f"Error analyzing company {company.get('name', 'Unknown')}: {e}")
                # Add company without CAT analysis
                enriched_companies.append({
                    **company,
                    'cat_evidence': f'Ошибка анализа: {str(e)}',
                    'cat_product': '',
                    'cat_score': 0
                })
        
        return enriched_companies
    
    def use_llm_for_analysis(self, website_content: str) -> Optional[str]:
        """
        Use LLM for advanced analysis of website content
        This requires Ollama or similar LLM service running locally
        """
        try:
            # Check if Ollama is available
            import requests as req
            
            ollama_url = "http://localhost:11434/api/generate"
            
            prompt = f"""
            Analyze this website content and determine if the company uses CAT (Computer-Assisted Translation) systems.
            Look for evidence of: translation memory, TMS, localization platforms, translation workflow tools.
            
            Content: {website_content[:2000]}  # Limit content length
            
            Provide analysis in Russian with:
            1. Yes/No answer about CAT usage
            2. Key evidence found
            3. Confidence level (0-100%)
            """
            
            payload = {
                "model": "llama3.2",
                "prompt": prompt,
                "stream": False
            }
            
            response = req.post(ollama_url, json=payload, timeout=30)
            if response.status_code == 200:
                result = response.json()
                return result.get('response', '')
            
        except Exception as e:
            logging.debug(f"LLM analysis not available: {e}")
        
        return None