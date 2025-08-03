import streamlit as st
import time
import json
import hashlib
from typing import Dict, Any, Optional, Callable
from datetime import datetime, timedelta

class CacheManager:
    """Manages caching for expensive operations like API calls."""
    
    def __init__(self, default_ttl: int = 300):  # 5 minutes default
        self.default_ttl = default_ttl
        if 'cache_store' not in st.session_state:
            st.session_state.cache_store = {}
    
    def _generate_key(self, func_name: str, *args, **kwargs) -> str:
        """Generate a unique cache key from function name and parameters."""
        key_data = {
            'func': func_name,
            'args': args,
            'kwargs': kwargs
        }
        key_string = json.dumps(key_data, sort_keys=True, default=str)
        return hashlib.md5(key_string.encode()).hexdigest()
    
    def get(self, key: str) -> Optional[Any]:
        """Get cached value if not expired."""
        if key not in st.session_state.cache_store:
            return None
        
        cached_item = st.session_state.cache_store[key]
        
        # Check if expired
        if time.time() > cached_item['expires_at']:
            del st.session_state.cache_store[key]
            return None
        
        return cached_item['data']
    
    def set(self, key: str, value: Any, ttl: int = None) -> None:
        """Cache a value with TTL (time to live)."""
        if ttl is None:
            ttl = self.default_ttl
        
        st.session_state.cache_store[key] = {
            'data': value,
            'cached_at': time.time(),
            'expires_at': time.time() + ttl,
            'ttl': ttl
        }
    
    def cached_call(self, func: Callable, func_name: str, ttl: int = None, *args, **kwargs) -> Any:
        """Execute function with caching."""
        cache_key = self._generate_key(func_name, *args, **kwargs)
        
        # Try to get from cache first
        cached_result = self.get(cache_key)
        if cached_result is not None:
            return cached_result
        
        # Execute function and cache result
        try:
            result = func(*args, **kwargs)
            self.set(cache_key, result, ttl)
            return result
        except Exception as e:
            # Don't cache errors, just raise them
            raise e
    
    def invalidate(self, pattern: str = None) -> None:
        """Invalidate cache entries matching pattern."""
        if pattern is None:
            # Clear all cache
            st.session_state.cache_store = {}
        else:
            # Clear entries matching pattern
            keys_to_remove = []
            for key in st.session_state.cache_store.keys():
                if pattern in key:
                    keys_to_remove.append(key)
            
            for key in keys_to_remove:
                del st.session_state.cache_store[key]
    
    def get_cache_info(self) -> Dict:
        """Get cache statistics."""
        cache_store = st.session_state.cache_store
        total_entries = len(cache_store)
        expired_entries = 0
        current_time = time.time()
        
        for item in cache_store.values():
            if current_time > item['expires_at']:
                expired_entries += 1
        
        return {
            'total_entries': total_entries,
            'active_entries': total_entries - expired_entries,
            'expired_entries': expired_entries
        }
    
    def cleanup_expired(self) -> int:
        """Remove expired cache entries."""
        current_time = time.time()
        keys_to_remove = []
        
        for key, item in st.session_state.cache_store.items():
            if current_time > item['expires_at']:
                keys_to_remove.append(key)
        
        for key in keys_to_remove:
            del st.session_state.cache_store[key]
        
        return len(keys_to_remove)

# Global cache instance
cache_manager = CacheManager()