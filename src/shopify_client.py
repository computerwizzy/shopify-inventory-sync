import requests
import time
from typing import List, Dict, Optional
import streamlit as st
from urllib.parse import urljoin
import os
import logging
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.api_resilience import (
    ResilientAPIClient, 
    create_resilient_session,
    APIOverloadError,
    RateLimitError
)
from utils.config import Config

class ShopifyClient:
    """Handles Shopify API integration for inventory management."""
    
    def __init__(self, store_url: str = None, access_token: str = None, api_version: str = None):
        """
        Initialize Shopify client.
        
        Args:
            store_url: Shopify store URL (e.g., 'your-store.myshopify.com')
            access_token: Shopify private app access token
            api_version: Shopify API version
        """
        # Use Config class to handle both environment variables and Streamlit secrets
        config = Config()
        
        self.store_url = store_url or config.shopify_store_url
        self.access_token = access_token or config.shopify_access_token
        self.api_version = api_version or config.shopify_api_version
        
        if not self.store_url or not self.access_token:
            raise ValueError("Shopify store URL and access token are required")
        
        # Ensure store URL format
        if not self.store_url.startswith('https://'):
            self.store_url = f"https://{self.store_url}"
        if not self.store_url.endswith('.myshopify.com'):
            if not self.store_url.endswith('.myshopify.com/'):
                self.store_url = f"{self.store_url}.myshopify.com"
        
        self.base_url = f"{self.store_url}/admin/api/{self.api_version}/"
        
        # Setup logging
        self.logger = logging.getLogger(f'{__name__}.ShopifyClient')
        
        # Request tracking for rate limiting
        self.last_request_time = 0
        self.min_request_interval = 0.5
        
        # Statistics tracking
        self._requests_made = 0
        self._failures = 0
        self._rate_limits = 0
        
        # SKU lookup cache for performance optimization
        self._sku_to_product_cache = {}
    
    def _make_request(self, method: str, endpoint: str, params: Dict = None, json_data: Dict = None) -> Dict:
        """
        Make request to Shopify API with basic retry logic.
        
        Args:
            method: HTTP method
            endpoint: API endpoint
            params: URL parameters
            json_data: JSON payload
            
        Returns:
            Dict: API response
            
        Raises:
            Exception: If request fails after all retries
        """
        url = urljoin(self.base_url, endpoint)
        
        # Simple rate limiting
        current_time = time.time()
        if current_time - self.last_request_time < self.min_request_interval:
            time.sleep(self.min_request_interval - (current_time - self.last_request_time))
        
        headers = {
            'X-Shopify-Access-Token': self.access_token,
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }
        
        max_retries = 3
        for attempt in range(max_retries):
            try:
                # Make direct request
                response = requests.request(
                    method=method,
                    url=url,
                    params=params,
                    json=json_data,
                    headers=headers,
                    timeout=30
                )
                
                self.last_request_time = time.time()
                self._requests_made += 1
                
                # Handle rate limiting
                if response.status_code == 429:
                    self._rate_limits += 1
                    retry_after = int(response.headers.get('Retry-After', 2))
                    if attempt < max_retries - 1:
                        self.logger.warning(f"Rate limited, waiting {retry_after} seconds...")
                        time.sleep(retry_after)
                        continue
                    else:
                        raise Exception(f"Rate limited after {max_retries} attempts")
                
                # Check for success
                response.raise_for_status()
                
                # Parse JSON response
                if response.content:
                    return response.json()
                else:
                    return {}
                    
            except requests.exceptions.RequestException as e:
                self._failures += 1
                if attempt < max_retries - 1:
                    wait_time = 2 ** attempt  # Exponential backoff
                    self.logger.warning(f"Request failed (attempt {attempt + 1}/{max_retries}), retrying in {wait_time}s...")
                    time.sleep(wait_time)
                    continue
                else:
                    self.logger.error(f"Shopify API request failed after {max_retries} attempts: {str(e)}")
                    raise Exception(f"Shopify API request failed: {str(e)}")
            
            except Exception as e:
                self._failures += 1
                self.logger.error(f"Unexpected error in Shopify API request: {str(e)}")
                raise Exception(f"Shopify API unexpected error: {str(e)}")
        
        raise Exception("Maximum retry attempts exceeded")
    
    def _get_paginated_results(self, endpoint: str, data_key: str, limit: int = 250) -> List[Dict]:
        """
        Get all results from a paginated Shopify API endpoint.
        
        Args:
            endpoint: API endpoint (e.g., 'products.json')
            data_key: Key in response containing the data array
            limit: Number of items per page (max 250)
            
        Returns:
            List[Dict]: All results from all pages
        """
        all_results = []
        params = {'limit': limit}
        
        while True:
            try:
                response = self._make_request('GET', endpoint, params=params)
                
                if data_key not in response:
                    self.logger.warning(f"No '{data_key}' key found in response for {endpoint}")
                    break
                
                items = response[data_key]
                if not items:
                    # No more items
                    break
                
                all_results.extend(items)
                self.logger.info(f"Retrieved {len(items)} items from {endpoint} (total: {len(all_results)})")
                
                # Check for pagination info
                # Shopify uses 'Link' header for pagination in newer APIs
                if len(items) < limit:
                    # Got fewer items than requested, probably last page
                    break
                
                # For older pagination, use the last item's ID as since_id
                if items:
                    last_id = items[-1].get('id')
                    if last_id:
                        params['since_id'] = last_id
                    else:
                        # No ID found, can't paginate further
                        break
                else:
                    break
                    
            except Exception as e:
                self.logger.error(f"Error fetching paginated results from {endpoint}: {str(e)}")
                break
        
        return all_results
    
    def test_connection(self) -> bool:
        """
        Test connection to Shopify API.
        
        Returns:
            bool: True if connection successful
        """
        try:
            response = self._make_request('GET', 'shop.json')
            return 'shop' in response
        except Exception:
            return False
    
    def get_shop_info(self) -> Dict:
        """
        Get shop information.
        
        Returns:
            Dict: Shop information
        """
        response = self._make_request('GET', 'shop.json')
        return response.get('shop', {})
    
    def get_all_collections(self) -> List[Dict]:
        """
        Get all smart and custom collections from Shopify store with pagination.
        
        Returns:
            List[Dict]: All collections
        """
        all_collections = []
        
        # Get smart collections with pagination
        smart_collections = self._get_paginated_results('smart_collections.json', 'smart_collections')
        all_collections.extend(smart_collections)
        
        # Get custom collections with pagination
        custom_collections = self._get_paginated_results('custom_collections.json', 'custom_collections')
        all_collections.extend(custom_collections)
        
        self.logger.info(f"Retrieved {len(all_collections)} total collections from Shopify")
        return all_collections

    def get_products_by_collection(self, collection_ids: List[int], limit: int = 250) -> List[Dict]:
        """
        Get all products for a list of collection IDs.

        Args:
            collection_ids: List of collection IDs
            limit: Number of products per page (max 250)

        Returns:
            List[Dict]: All products in the specified collections
        """
        all_products = []
        for collection_id in collection_ids:
            params = {
                'collection_id': collection_id,
                'limit': min(limit, 250),
                'fields': 'id,title,handle,variants'
            }
            
            # Get first page
            response = self._make_request('GET', 'products.json', params=params)
            products = response.get('products', [])
            all_products.extend(products)
            
            # Get remaining pages using pagination
            while len(products) == params['limit']:
                # Get next page using the last product's ID
                params['since_id'] = products[-1]['id']
                response = self._make_request('GET', 'products.json', params=params)
                products = response.get('products', [])
                all_products.extend(products)
        
        return all_products

    def get_all_products(self, limit: int = 250) -> List[Dict]:
        """
        Get all products from Shopify store with pagination.
        
        Args:
            limit: Number of products per page (max 250)
            
        Returns:
            List[Dict]: All products with variants
        """
        # Use the paginated helper with custom fields
        all_products = self._get_paginated_results('products.json?fields=id,title,handle,variants', 'products', limit)
        
        self.logger.info(f"Retrieved {len(all_products)} total products from Shopify")
        return all_products
    
    def get_product_variants(self, product_id: int) -> List[Dict]:
        """
        Get all variants for a specific product.
        
        Args:
            product_id: Shopify product ID
            
        Returns:
            List[Dict]: Product variants
        """
        response = self._make_request('GET', f'products/{product_id}/variants.json')
        return response.get('variants', [])
    
    def get_inventory_levels(self, inventory_item_ids: List[int]) -> List[Dict]:
        """
        Get inventory levels for specific inventory items.
        
        Args:
            inventory_item_ids: List of inventory item IDs
            
        Returns:
            List[Dict]: Inventory levels
        """
        if not inventory_item_ids:
            return []
        
        # Shopify limits to 50 inventory item IDs per request
        all_levels = []
        chunk_size = 50
        
        for i in range(0, len(inventory_item_ids), chunk_size):
            chunk_ids = inventory_item_ids[i:i + chunk_size]
            params = {
                'inventory_item_ids': ','.join(map(str, chunk_ids))
            }
            
            response = self._make_request('GET', 'inventory_levels.json', params=params)
            levels = response.get('inventory_levels', [])
            all_levels.extend(levels)
        
        return all_levels
    
    def update_inventory(self, variant_id: int, quantity: int, location_id: int = None) -> Dict:
        """
        Update inventory quantity for a variant.
        
        Args:
            variant_id: Shopify variant ID
            quantity: New quantity
            location_id: Location ID (optional, uses primary location if not provided)
            
        Returns:
            Dict: Update response
        """
        # Get variant info to find inventory item ID
        variant_response = self._make_request('GET', f'variants/{variant_id}.json')
        variant = variant_response.get('variant', {})
        inventory_item_id = variant.get('inventory_item_id')
        
        if not inventory_item_id:
            raise Exception(f"No inventory item found for variant {variant_id}")
        
        # Get location ID if not provided
        if not location_id:
            location_id = self._get_primary_location_id()
        
        # Update inventory level
        json_data = {
            'location_id': location_id,
            'inventory_item_id': inventory_item_id,
            'available': quantity
        }
        
        response = self._make_request('POST', 'inventory_levels/set.json', json_data=json_data)
        return response.get('inventory_level', {})
    
    def update_product_fields(self, product_id: int, variant_id: int, update_data: Dict, sync_fields: Dict) -> Dict:
        """
        Update specific fields on a product and its variant based on sync_fields configuration.
        
        Args:
            product_id: Shopify product ID
            variant_id: Shopify variant ID  
            update_data: Data to update with (from feed)
            sync_fields: Dictionary indicating which fields to sync
            
        Returns:
            Dict: Update results
        """
        results = {'product': None, 'variant': None, 'inventory': None}
        
        # Prepare product update data
        product_updates = {}
        if sync_fields.get('product_title') and 'title' in update_data:
            product_updates['title'] = update_data['title']
        if sync_fields.get('product_description') and 'body_html' in update_data:
            product_updates['body_html'] = update_data['body_html']
        if sync_fields.get('product_vendor') and 'vendor' in update_data:
            product_updates['vendor'] = update_data['vendor']
        if sync_fields.get('product_type') and 'product_type' in update_data:
            product_updates['product_type'] = update_data['product_type']
        if sync_fields.get('product_status') and 'status' in update_data:
            product_updates['status'] = update_data['status']
        
        # Update product if there are product-level changes
        if product_updates:
            json_data = {'product': product_updates}
            response = self._make_request('PUT', f'products/{product_id}.json', json_data=json_data)
            results['product'] = response.get('product', {})
        
        # Prepare variant update data
        variant_updates = {}
        if sync_fields.get('variant_price') and 'price' in update_data:
            variant_updates['price'] = str(update_data['price'])
        if sync_fields.get('compare_at_price') and 'compare_at_price' in update_data:
            variant_updates['compare_at_price'] = str(update_data['compare_at_price'])
        if sync_fields.get('variant_weight') and 'weight' in update_data:
            variant_updates['weight'] = update_data['weight']
        if sync_fields.get('variant_sku') and 'sku' in update_data:
            variant_updates['sku'] = update_data['sku']
        if sync_fields.get('track_inventory') and 'inventory_management' in update_data:
            variant_updates['inventory_management'] = update_data['inventory_management']
        
        # Update variant if there are variant-level changes
        if variant_updates:
            json_data = {'variant': variant_updates}
            response = self._make_request('PUT', f'variants/{variant_id}.json', json_data=json_data)
            results['variant'] = response.get('variant', {})
        
        # Update inventory quantity if selected
        if sync_fields.get('inventory_quantity') and 'quantity' in update_data:
            try:
                inventory_result = self.update_inventory(variant_id, int(update_data['quantity']))
                results['inventory'] = inventory_result
            except Exception as e:
                results['inventory'] = {'error': str(e)}
        
        return results
    
    def bulk_update_inventory(self, updates: List[Dict], location_id: int = None) -> List[Dict]:
        """
        Update multiple inventory items in batch.
        
        Args:
            updates: List of update dictionaries with 'variant_id' and 'quantity'
            location_id: Location ID (optional)
            
        Returns:
            List[Dict]: Update results
        """
        if not location_id:
            location_id = self._get_primary_location_id()
        
        results = []
        
        for update in updates:
            try:
                result = self.update_inventory(
                    update['variant_id'], 
                    update['quantity'], 
                    location_id
                )
                results.append({
                    'variant_id': update['variant_id'],
                    'success': True,
                    'result': result
                })
            except Exception as e:
                results.append({
                    'variant_id': update['variant_id'],
                    'success': False,
                    'error': str(e)
                })
        
        return results
    
    def _get_primary_location_id(self) -> int:
        """
        Get the primary location ID.
        
        Returns:
            int: Primary location ID
        """
        response = self._make_request('GET', 'locations.json')
        locations = response.get('locations', [])
        
        # Find primary location
        for location in locations:
            if location.get('primary', False):
                return location['id']
        
        # If no primary location, return first location
        if locations:
            return locations[0]['id']
        
        raise Exception("No locations found in store")
    
    def search_products(self, query: str, limit: int = 50) -> List[Dict]:
        """
        Search products by title, SKU, or other fields.
        
        Args:
            query: Search query
            limit: Maximum number of results
            
        Returns:
            List[Dict]: Matching products
        """
        params = {
            'limit': min(limit, 250),
            'title': query,
            'fields': 'id,title,handle,variants'
        }
        
        response = self._make_request('GET', 'products.json', params=params)
        return response.get('products', [])
    
    def get_product_by_sku(self, sku: str) -> Optional[Dict]:
        """
        Find product variant by SKU with caching optimization.
        
        Args:
            sku: Product SKU
            
        Returns:
            Dict or None: Product and variant info
        """
        # Check cache first
        if sku in self._sku_to_product_cache:
            return self._sku_to_product_cache[sku]
        
        # Build cache if empty (only do this once)
        if not self._sku_to_product_cache:
            self._build_sku_cache()
        
        return self._sku_to_product_cache.get(sku)
    
    def _build_sku_cache(self):
        """Build SKU to product mapping cache for efficient lookups."""
        try:
            all_products = self.get_all_products()
            
            for product in all_products:
                if 'variants' in product:
                    for variant in product['variants']:
                        sku = variant.get('sku')
                        if sku:
                            self._sku_to_product_cache[sku] = {
                                'product': product,
                                'variant': variant
                            }
        except Exception as e:
            self.logger.error(f"Failed to build SKU cache: {str(e)}")
            # Don't let cache building failure break the operation
            pass
    
    def get_api_stats(self) -> Dict:
        """
        Get API client statistics for monitoring.
        
        Returns:
            Dict: API client statistics
        """
        return {
            'requests_made': getattr(self, '_requests_made', 0),
            'failures': getattr(self, '_failures', 0),
            'rate_limits': getattr(self, '_rate_limits', 0),
            'last_request_time': self.last_request_time,
            'base_url': self.base_url,
            'circuit_breaker_state': 'CLOSED'
        }
    
    def reset_resilience(self):
        """Reset all resilience patterns (circuit breaker, rate limiter)."""
        self._requests_made = 0
        self._failures = 0
        self._rate_limits = 0
        self.logger.info("Shopify API client statistics reset")
    
    def close(self):
        """Close any resources."""
        pass