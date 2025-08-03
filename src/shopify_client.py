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
    
    def search_products(self, query: str = None, limit: int = 250, 
                       product_type: str = None, vendor: str = None) -> List[Dict]:
        """
        Search products with filters.
        
        Args:
            query: Search query for title
            limit: Maximum number of products to return
            product_type: Filter by product type
            vendor: Filter by vendor
            
        Returns:
            List[Dict]: Filtered products
        """
        params = {
            'limit': min(limit, 250),
            'fields': 'id,title,handle,variants,product_type,vendor,tags,created_at,updated_at'
        }
        
        if query:
            params['title'] = query
        if product_type:
            params['product_type'] = product_type
        if vendor:
            params['vendor'] = vendor
        
        return self._get_paginated_results('products.json', 'products', limit)
    
    def get_product_by_id(self, product_id: int) -> Dict:
        """
        Get a specific product by ID.
        
        Args:
            product_id: Shopify product ID
            
        Returns:
            Dict: Product data
        """
        response = self._make_request('GET', f'products/{product_id}.json')
        return response.get('product', {})
    
    def create_product(self, product_data: Dict) -> Dict:
        """
        Create a new product.
        
        Args:
            product_data: Product data dictionary
            
        Returns:
            Dict: Created product data
        """
        json_data = {'product': product_data}
        response = self._make_request('POST', 'products.json', json_data=json_data)
        return response.get('product', {})
    
    def update_product(self, product_id: int, product_data: Dict) -> Dict:
        """
        Update an existing product.
        
        Args:
            product_id: Shopify product ID
            product_data: Updated product data
            
        Returns:
            Dict: Updated product data
        """
        json_data = {'product': product_data}
        response = self._make_request('PUT', f'products/{product_id}.json', json_data=json_data)
        return response.get('product', {})
    
    def delete_product(self, product_id: int) -> bool:
        """
        Delete a product.
        
        Args:
            product_id: Shopify product ID
            
        Returns:
            bool: True if successful
        """
        try:
            self._make_request('DELETE', f'products/{product_id}.json')
            return True
        except Exception as e:
            self.logger.error(f"Failed to delete product {product_id}: {str(e)}")
            return False
    
    def update_variant_inventory(self, variant_id: int, quantity: int, location_id: int = None) -> bool:
        """
        Update inventory for a specific variant.
        
        Args:
            variant_id: Shopify variant ID
            quantity: New inventory quantity
            location_id: Location ID (optional)
            
        Returns:
            bool: True if successful
        """
        try:
            if not location_id:
                location_id = self._get_primary_location_id()
            
            # Get current inventory item
            inventory_response = self._make_request('GET', f'variants/{variant_id}.json')
            variant = inventory_response.get('variant', {})
            inventory_item_id = variant.get('inventory_item_id')
            
            if not inventory_item_id:
                self.logger.error(f"No inventory item found for variant {variant_id}")
                return False
            
            # Update inventory level
            json_data = {
                'location_id': location_id,
                'inventory_item_id': inventory_item_id,
                'available': quantity
            }
            
            self._make_request('POST', 'inventory_levels/set.json', json_data=json_data)
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to update inventory for variant {variant_id}: {str(e)}")
            return False
    
    def get_inventory_levels(self, location_id: int = None) -> List[Dict]:
        """
        Get inventory levels for a location.
        
        Args:
            location_id: Location ID (optional)
            
        Returns:
            List[Dict]: Inventory levels
        """
        if not location_id:
            location_id = self._get_primary_location_id()
        
        params = {'location_ids': location_id, 'limit': 250}
        return self._get_paginated_results('inventory_levels.json', 'inventory_levels', 250)
    
    def get_low_stock_products(self, threshold: int = 5) -> List[Dict]:
        """
        Get products with low stock.
        
        Args:
            threshold: Stock level threshold
            
        Returns:
            List[Dict]: Products with low stock
        """
        products = self.get_all_products()
        low_stock = []
        
        for product in products:
            for variant in product.get('variants', []):
                inventory_qty = variant.get('inventory_quantity', 0)
                if 0 < inventory_qty <= threshold:
                    low_stock.append({
                        'product_id': product['id'],
                        'variant_id': variant['id'],
                        'title': product['title'],
                        'variant_title': variant.get('title', ''),
                        'sku': variant.get('sku', ''),
                        'inventory_quantity': inventory_qty,
                        'price': variant.get('price', 0)
                    })
        
        return low_stock
    
    def get_out_of_stock_products(self) -> List[Dict]:
        """
        Get products that are out of stock.
        
        Returns:
            List[Dict]: Out of stock products
        """
        products = self.get_all_products()
        out_of_stock = []
        
        for product in products:
            for variant in product.get('variants', []):
                inventory_qty = variant.get('inventory_quantity', 0)
                if inventory_qty == 0:
                    out_of_stock.append({
                        'product_id': product['id'],
                        'variant_id': variant['id'],
                        'title': product['title'],
                        'variant_title': variant.get('title', ''),
                        'sku': variant.get('sku', ''),
                        'price': variant.get('price', 0)
                    })
        
        return out_of_stock
    
    def bulk_price_update(self, updates: List[Dict]) -> List[Dict]:
        """
        Update prices for multiple variants.
        
        Args:
            updates: List of {variant_id, price} dictionaries
            
        Returns:
            List[Dict]: Update results
        """
        results = []
        
        for update in updates:
            try:
                variant_id = update['variant_id']
                new_price = update['price']
                
                json_data = {'variant': {'price': str(new_price)}}
                response = self._make_request('PUT', f'variants/{variant_id}.json', json_data=json_data)
                
                results.append({
                    'variant_id': variant_id,
                    'success': True,
                    'updated_price': new_price
                })
                
            except Exception as e:
                results.append({
                    'variant_id': update.get('variant_id'),
                    'success': False,
                    'error': str(e)
                })
        
        return results
    
    def get_product_analytics(self) -> Dict:
        """
        Get basic product analytics.
        
        Returns:
            Dict: Analytics data
        """
        products = self.get_all_products()
        
        if not products:
            return {}
        
        total_products = len(products)
        total_variants = 0
        total_inventory = 0
        total_value = 0
        out_of_stock_count = 0
        low_stock_count = 0
        product_types = {}
        vendors = {}
        
        for product in products:
            product_type = product.get('product_type', 'Unknown')
            vendor = product.get('vendor', 'Unknown')
            
            product_types[product_type] = product_types.get(product_type, 0) + 1
            vendors[vendor] = vendors.get(vendor, 0) + 1
            
            for variant in product.get('variants', []):
                total_variants += 1
                inventory_qty = variant.get('inventory_quantity', 0)
                price = float(variant.get('price', 0))
                
                total_inventory += inventory_qty
                total_value += inventory_qty * price
                
                if inventory_qty == 0:
                    out_of_stock_count += 1
                elif inventory_qty <= 5:
                    low_stock_count += 1
        
        return {
            'total_products': total_products,
            'total_variants': total_variants,
            'total_inventory': total_inventory,
            'total_value': total_value,
            'out_of_stock_count': out_of_stock_count,
            'low_stock_count': low_stock_count,
            'well_stocked_count': total_variants - out_of_stock_count - low_stock_count,
            'product_types': dict(sorted(product_types.items(), key=lambda x: x[1], reverse=True)),
            'vendors': dict(sorted(vendors.items(), key=lambda x: x[1], reverse=True))
        }
    
    def get_products_count(self) -> int:
        """
        Get total product count efficiently.
        
        Returns:
            int: Total number of products
        """
        try:
            # Use the count endpoint for efficiency
            response = self._make_request('GET', 'products/count.json')
            return response.get('count', 0)
        except Exception as e:
            self.logger.error(f"Failed to get product count: {str(e)}")
            return 0
    
    def get_quick_metrics_with_sample(self, sample_size: int = 50) -> Dict:
        """
        Get quick dashboard metrics with customizable sample size.
        
        Args:
            sample_size: Number of products to sample
            
        Returns:
            Dict: Quick metrics based on sample
        """
        try:
            # Get basic counts
            shop_info = self.get_shop_info()
            product_count = self.get_products_count()
            
            # Get sample of products with customizable size
            sample_products = self._get_paginated_results(
                f'products.json?limit={min(sample_size, 250)}&fields=id,variants', 
                'products', 
                min(sample_size, 250)
            )
            
            sample_variants = 0
            sample_inventory = 0
            sample_value = 0
            low_stock_sample = 0
            out_of_stock_sample = 0
            
            for product in sample_products:
                for variant in product.get('variants', []):
                    sample_variants += 1
                    inventory_qty = variant.get('inventory_quantity', 0)
                    price = float(variant.get('price', 0))
                    
                    sample_inventory += inventory_qty
                    sample_value += inventory_qty * price
                    
                    if inventory_qty == 0:
                        out_of_stock_sample += 1
                    elif inventory_qty <= 5:
                        low_stock_sample += 1
            
            # Estimate totals based on sample
            if sample_variants > 0 and len(sample_products) > 0:
                avg_variants_per_product = sample_variants / len(sample_products)
                estimated_variants = int(product_count * avg_variants_per_product)
                
                avg_inventory_per_variant = sample_inventory / sample_variants if sample_variants > 0 else 0
                estimated_inventory = int(estimated_variants * avg_inventory_per_variant)
                
                avg_value_per_variant = sample_value / sample_variants if sample_variants > 0 else 0
                estimated_value = estimated_variants * avg_value_per_variant
                
                # Estimate stock status
                low_stock_ratio = low_stock_sample / sample_variants if sample_variants > 0 else 0
                out_of_stock_ratio = out_of_stock_sample / sample_variants if sample_variants > 0 else 0
                
                estimated_low_stock = int(estimated_variants * low_stock_ratio)
                estimated_out_of_stock = int(estimated_variants * out_of_stock_ratio)
            else:
                estimated_variants = 0
                estimated_inventory = 0
                estimated_value = 0
                estimated_low_stock = 0
                estimated_out_of_stock = 0
            
            return {
                'shop_name': shop_info.get('name', 'Your Store'),
                'total_products': product_count,
                'estimated_variants': estimated_variants,
                'estimated_inventory': estimated_inventory,
                'estimated_value': estimated_value,
                'estimated_low_stock': estimated_low_stock,
                'estimated_out_of_stock': estimated_out_of_stock,
                'estimated_well_stocked': estimated_variants - estimated_low_stock - estimated_out_of_stock,
                'sample_size': len(sample_products),
                'sample_variants': sample_variants,
                'is_estimate': True
            }
            
        except Exception as e:
            self.logger.error(f"Failed to get quick metrics with sample: {str(e)}")
            return {
                'shop_name': 'Your Store',
                'total_products': 0,
                'estimated_variants': 0,
                'estimated_inventory': 0,
                'estimated_value': 0,
                'estimated_low_stock': 0,
                'estimated_out_of_stock': 0,
                'estimated_well_stocked': 0,
                'sample_size': 0,
                'is_estimate': True,
                'error': str(e)
            }

    def get_quick_metrics(self) -> Dict:
        """
        Get quick dashboard metrics without loading all products.
        
        Returns:
            Dict: Quick metrics for dashboard
        """
        try:
            # Get basic counts
            shop_info = self.get_shop_info()
            product_count = self.get_products_count()
            
            # Get a sample of recent products for quick analysis
            sample_products = self._get_paginated_results('products.json?limit=50&fields=id,variants', 'products', 50)
            
            sample_variants = 0
            sample_inventory = 0
            sample_value = 0
            low_stock_sample = 0
            out_of_stock_sample = 0
            
            for product in sample_products:
                for variant in product.get('variants', []):
                    sample_variants += 1
                    inventory_qty = variant.get('inventory_quantity', 0)
                    price = float(variant.get('price', 0))
                    
                    sample_inventory += inventory_qty
                    sample_value += inventory_qty * price
                    
                    if inventory_qty == 0:
                        out_of_stock_sample += 1
                    elif inventory_qty <= 5:
                        low_stock_sample += 1
            
            # Estimate totals based on sample (rough approximation)
            if sample_variants > 0:
                avg_variants_per_product = sample_variants / len(sample_products) if sample_products else 1
                estimated_variants = int(product_count * avg_variants_per_product)
                
                avg_inventory_per_variant = sample_inventory / sample_variants if sample_variants > 0 else 0
                estimated_inventory = int(estimated_variants * avg_inventory_per_variant)
                
                avg_value_per_variant = sample_value / sample_variants if sample_variants > 0 else 0
                estimated_value = estimated_variants * avg_value_per_variant
                
                # Estimate stock status
                low_stock_ratio = low_stock_sample / sample_variants if sample_variants > 0 else 0
                out_of_stock_ratio = out_of_stock_sample / sample_variants if sample_variants > 0 else 0
                
                estimated_low_stock = int(estimated_variants * low_stock_ratio)
                estimated_out_of_stock = int(estimated_variants * out_of_stock_ratio)
            else:
                estimated_variants = 0
                estimated_inventory = 0
                estimated_value = 0
                estimated_low_stock = 0
                estimated_out_of_stock = 0
            
            return {
                'shop_name': shop_info.get('name', 'Your Store'),
                'total_products': product_count,
                'estimated_variants': estimated_variants,
                'estimated_inventory': estimated_inventory,
                'estimated_value': estimated_value,
                'estimated_low_stock': estimated_low_stock,
                'estimated_out_of_stock': estimated_out_of_stock,
                'estimated_well_stocked': estimated_variants - estimated_low_stock - estimated_out_of_stock,
                'sample_size': len(sample_products),
                'is_estimate': True
            }
            
        except Exception as e:
            self.logger.error(f"Failed to get quick metrics: {str(e)}")
            return {
                'shop_name': 'Your Store',
                'total_products': 0,
                'estimated_variants': 0,
                'estimated_inventory': 0,
                'estimated_value': 0,
                'estimated_low_stock': 0,
                'estimated_out_of_stock': 0,
                'estimated_well_stocked': 0,
                'sample_size': 0,
                'is_estimate': True,
                'error': str(e)
            }
    
    def get_products_paginated(self, page: int = 1, limit: int = 50, 
                              search: str = None, stock_filter: str = None) -> Dict:
        """
        Get products with pagination for better performance.
        
        Args:
            page: Page number (1-indexed)
            limit: Products per page
            search: Search term
            stock_filter: Filter by stock level
            
        Returns:
            Dict: Paginated products data
        """
        try:
            params = {
                'limit': min(limit, 250),
                'fields': 'id,title,handle,variants,product_type,vendor,created_at,updated_at'
            }
            
            # Calculate offset for pagination
            if page > 1:
                # For pagination, we need to use since_id approach
                # This is a simplified version - in production you'd want better pagination
                pass
            
            if search:
                params['title'] = search
            
            # Get products
            response = self._make_request('GET', 'products.json', params=params)
            products = response.get('products', [])
            
            # Process products for display
            product_data = []
            for product in products:
                for variant in product.get('variants', []):
                    inventory_qty = variant.get('inventory_quantity', 0)
                    
                    # Apply stock filter
                    if stock_filter == "In Stock" and inventory_qty <= 5:
                        continue
                    elif stock_filter == "Low Stock (≤5)" and not (0 < inventory_qty <= 5):
                        continue
                    elif stock_filter == "Out of Stock" and inventory_qty != 0:
                        continue
                    
                    product_data.append({
                        'product_id': product['id'],
                        'variant_id': variant['id'],
                        'title': product['title'],
                        'variant_title': variant.get('title', 'Default Title'),
                        'sku': variant.get('sku', ''),
                        'price': float(variant.get('price', 0)),
                        'inventory_quantity': inventory_qty,
                        'product_type': product.get('product_type', ''),
                        'vendor': product.get('vendor', ''),
                        'updated_at': product.get('updated_at', ''),
                        'stock_status': 'out' if inventory_qty == 0 else 'low' if inventory_qty <= 5 else 'good'
                    })
            
            return {
                'products': product_data,
                'page': page,
                'limit': limit,
                'has_more': len(products) == limit,  # Simplified check
                'total_on_page': len(product_data)
            }
            
        except Exception as e:
            self.logger.error(f"Failed to get paginated products: {str(e)}")
            return {
                'products': [],
                'page': page,
                'limit': limit,
                'has_more': False,
                'total_on_page': 0,
                'error': str(e)
            }