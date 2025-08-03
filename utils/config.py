import os
from typing import Dict, Optional
from dotenv import load_dotenv

class Config:
    """Configuration management for the inventory sync app."""
    
    def __init__(self):
        # Load environment variables from .env file
        load_dotenv()
        
        # Try to import streamlit for cloud deployment
        try:
            import streamlit as st
            # Use Streamlit secrets if available (for cloud deployment)
            try:
                if hasattr(st, 'secrets') and 'shopify' in st.secrets:
                    self.shopify_shop_name = st.secrets.shopify.get('SHOP_NAME', '')
                    self.shopify_access_token = st.secrets.shopify.get('ACCESS_TOKEN', '')
                    self.shopify_api_version = st.secrets.shopify.get('API_VERSION', '2025-01')
                    # Build store URL from shop name
                    if self.shopify_shop_name:
                        self.shopify_store_url = f"https://{self.shopify_shop_name}.myshopify.com"
                    else:
                        self.shopify_store_url = ""
                else:
                    # Fallback to environment variables
                    self._load_from_env()
            except Exception:
                # Secrets not available or error reading them, use environment variables
                self._load_from_env()
        except ImportError:
            # Streamlit not available, use environment variables
            self._load_from_env()
        
        # Initialize application settings (same for both cloud and local)
        self._init_app_settings()
    
    def _load_from_env(self):
        """Load configuration from environment variables."""
        # Legacy support - check for both old and new variable names
        self.shopify_store_url = os.getenv('SHOPIFY_STORE_URL') or os.getenv('SHOPIFY_SHOP_URL')
        self.shopify_shop_name = os.getenv('SHOPIFY_SHOP_NAME')
        self.shopify_access_token = os.getenv('SHOPIFY_ACCESS_TOKEN')
        self.shopify_api_version = os.getenv('SHOPIFY_API_VERSION', '2025-01')
        
        # Build store URL from shop name if not provided directly
        if not self.shopify_store_url and self.shopify_shop_name:
            self.shopify_store_url = f"https://{self.shopify_shop_name}.myshopify.com"
    
    def _init_app_settings(self):
        """Initialize application settings."""
        # Application settings
        self.fuzzy_match_threshold = int(os.getenv('FUZZY_MATCH_THRESHOLD', '85'))
        self.max_file_size_mb = int(os.getenv('MAX_FILE_SIZE_MB', '100'))
        self.batch_size = int(os.getenv('BATCH_SIZE', '10'))
        self.rate_limit_delay = float(os.getenv('RATE_LIMIT_DELAY', '0.5'))
        
        # UI Configuration
        self.page_title = os.getenv('PAGE_TITLE', 'Inventory Sync App')
        self.theme = os.getenv('THEME', 'light')
        
        # Debug settings
        self.debug_mode = os.getenv('DEBUG_MODE', 'False').lower() == 'true'
        self.log_level = os.getenv('LOG_LEVEL', 'INFO')
    
    def validate_shopify_config(self) -> bool:
        """
        Validate Shopify configuration.
        
        Returns:
            bool: True if configuration is valid
        """
        required_fields = [
            self.shopify_store_url,
            self.shopify_access_token
        ]
        
        return all(field and field.strip() for field in required_fields)
    
    def get_shopify_config(self) -> Dict[str, str]:
        """
        Get Shopify configuration dictionary.
        
        Returns:
            Dict[str, str]: Shopify configuration
        """
        return {
            'store_url': self.shopify_store_url,
            'access_token': self.shopify_access_token,
            'api_version': self.shopify_api_version
        }
    
    def get_app_settings(self) -> Dict:
        """
        Get application settings dictionary.
        
        Returns:
            Dict: Application settings
        """
        return {
            'fuzzy_match_threshold': self.fuzzy_match_threshold,
            'max_file_size_mb': self.max_file_size_mb,
            'batch_size': self.batch_size,
            'rate_limit_delay': self.rate_limit_delay,
            'page_title': self.page_title,
            'theme': self.theme,
            'debug_mode': self.debug_mode,
            'log_level': self.log_level
        }
    
    def validate_file_size(self, file_size_bytes: int) -> bool:
        """
        Validate uploaded file size.
        
        Args:
            file_size_bytes: File size in bytes
            
        Returns:
            bool: True if file size is acceptable
        """
        max_size_bytes = self.max_file_size_mb * 1024 * 1024
        return file_size_bytes <= max_size_bytes
    
    def get_supported_file_types(self) -> list:
        """
        Get list of supported file types.
        
        Returns:
            list: Supported file extensions
        """
        return ['.csv', '.xlsx', '.xls']
    
    def create_env_template(self) -> str:
        """
        Create environment template string.
        
        Returns:
            str: Environment template
        """
        template = """# Shopify Configuration
SHOPIFY_STORE_URL=your-store.myshopify.com
SHOPIFY_ACCESS_TOKEN=your_access_token_here
SHOPIFY_API_VERSION=2023-10

# Application Settings
FUZZY_MATCH_THRESHOLD=85
MAX_FILE_SIZE_MB=100
BATCH_SIZE=10
RATE_LIMIT_DELAY=0.5

# UI Configuration
PAGE_TITLE=Inventory Sync App
THEME=light

# Debug Settings
DEBUG_MODE=False
LOG_LEVEL=INFO
"""
        return template
    
    def get_missing_config(self) -> list:
        """
        Get list of missing required configuration items.
        
        Returns:
            list: Missing configuration items
        """
        missing = []
        
        if not self.shopify_store_url:
            missing.append('SHOPIFY_STORE_URL')
        
        if not self.shopify_access_token:
            missing.append('SHOPIFY_ACCESS_TOKEN')
        
        return missing
    
    def update_config(self, **kwargs) -> None:
        """
        Update configuration values.
        
        Args:
            **kwargs: Configuration key-value pairs
        """
        for key, value in kwargs.items():
            if hasattr(self, key):
                setattr(self, key, value)
    
    def __str__(self) -> str:
        """
        String representation of configuration (sensitive values hidden).
        
        Returns:
            str: Configuration summary
        """
        config_summary = f"""
Configuration Summary:
- Shopify Store: {self.shopify_store_url if self.shopify_store_url else 'Not Set'}
- Access Token: {'***' + self.shopify_access_token[-4:] if self.shopify_access_token else 'Not Set'}
- API Version: {self.shopify_api_version}
- Fuzzy Match Threshold: {self.fuzzy_match_threshold}%
- Max File Size: {self.max_file_size_mb}MB
- Batch Size: {self.batch_size}
- Debug Mode: {self.debug_mode}
        """.strip()
        
        return config_summary