"""
Configuration module for the data collection system
"""
import json
import os
from dataclasses import dataclass
from typing import List, Dict, Any
import logging


@dataclass
class DataSourceConfig:
    """Configuration for data sources"""
    rusprofile_base_url: str
    spark_base_url: str
    listorg_base_url: str
    hh_api_url: str


@dataclass
class SearchConfig:
    """Configuration for search parameters"""
    min_revenue: int
    revenue_year: int
    keywords_cat: List[str]
    cat_products: List[str]


@dataclass
class ParsingConfig:
    """Configuration for parsing settings"""
    timeout: int
    max_retries: int
    delay_between_requests: float
    user_agent_rotation: bool


@dataclass
class OutputConfig:
    """Configuration for output settings"""
    csv_encoding: str
    csv_separator: str
    log_level: str


class Config:
    """Main configuration class"""
    
    def __init__(self, config_path: str = "config.json"):
        self.config_path = config_path
        self.data_sources = None
        self.search_params = None
        self.parsing = None
        self.output = None
        self.load_config()
        self.setup_logging()
    
    def load_config(self):
        """Load configuration from JSON file"""
        try:
            if os.path.exists(self.config_path):
                with open(self.config_path, 'r', encoding='utf-8') as f:
                    config_data = json.load(f)
            else:
                # Use default config if file doesn't exist
                config_data = self.get_default_config()
            
            self.data_sources = DataSourceConfig(**config_data['data_sources'])
            self.search_params = SearchConfig(**config_data['search_params'])
            self.parsing = ParsingConfig(**config_data['parsing'])
            self.output = OutputConfig(**config_data['output'])
            
        except Exception as e:
            logging.error(f"Error loading config: {e}")
            raise
    
    def get_default_config(self) -> Dict[str, Any]:
        """Return default configuration"""
        return {
            "data_sources": {
                "rusprofile_base_url": "https://www.rusprofile.ru",
                "spark_base_url": "https://spark-interfax.ru",
                "listorg_base_url": "https://www.list-org.com",
                "hh_api_url": "https://api.hh.ru"
            },
            "search_params": {
                "min_revenue": 100000000,
                "revenue_year": 2023,
                "keywords_cat": [
                    "translation memory", "tm", "tms", "cat-систем", "cat tool",
                    "локализация", "компьютерная поддержка перевода",
                    "терминологическая база", "memoq", "trados", "smartcat",
                    "memsource", "phrase", "xtm", "wordfast", "переводческ"
                ],
                "cat_products": [
                    "SDL Trados", "memoQ", "Smartcat", "Memsource", "Phrase",
                    "XTM", "Wordfast", "Transit", "Across", "Лингвотек", "Промт"
                ]
            },
            "parsing": {
                "timeout": 10,
                "max_retries": 3,
                "delay_between_requests": 1,
                "user_agent_rotation": True
            },
            "output": {
                "csv_encoding": "utf-8-sig",
                "csv_separator": ",",
                "log_level": "INFO"
            }
        }
    
    def setup_logging(self):
        """Setup logging configuration"""
        log_level = getattr(logging, self.output.log_level)
        
        # Решение для Windows кодировки
        import sys
        if sys.platform == "win32":
            # Для Windows используем только файловый логгер
            logging.basicConfig(
                level=log_level,
                format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                handlers=[
                    logging.FileHandler('data_collection.log', encoding='utf-8')
                    # Убираем StreamHandler для Windows
                ]
            )
        else:
            # Для Linux/Mac оставляем как есть
            logging.basicConfig(
                level=log_level,
                format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                handlers=[
                    logging.FileHandler('data_collection.log', encoding='utf-8'),
                    logging.StreamHandler()
                ]
            )