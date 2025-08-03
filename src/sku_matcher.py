import pandas as pd
import numpy as np
from fuzzywuzzy import fuzz, process
from typing import List, Dict, Tuple, Optional
import streamlit as st

class SKUMatcher:
    """Handles SKU matching between file data and Shopify products."""
    
    def __init__(self, shopify_client, fuzzy_threshold: int = 85):
        self.shopify_client = shopify_client
        self.fuzzy_threshold = fuzzy_threshold
    
    def match_skus(self, df: pd.DataFrame, column_mapping: Dict[str, str], shopify_products: List[Dict]) -> List[Dict]:
        """
        Match SKUs from uploaded file with Shopify products.
        
        Args:
            df: DataFrame with uploaded data
            column_mapping: Column mapping dictionary
            shopify_products: List of Shopify products
            
        Returns:
            List[Dict]: Matched data with metadata
        """
        # Get mapped data
        sku_column = column_mapping.get('SKU')
        quantity_column = column_mapping.get('Quantity')
        
        if not sku_column or not quantity_column:
            raise ValueError("SKU and Quantity columns must be mapped")
        
        # Create Shopify SKU lookup
        shopify_sku_map = self._create_shopify_sku_map(shopify_products)
        
        matched_data = []
        
        # Process each row
        for index, row in df.iterrows():
            file_sku = str(row[sku_column]).strip()
            quantity = self._parse_quantity(row[quantity_column])
            
            if pd.isna(file_sku) or file_sku == '' or file_sku == 'nan':
                continue
            
            # Try exact match first
            match_result = self._find_exact_match(file_sku, shopify_sku_map)
            
            if not match_result:
                # Try fuzzy match
                match_result = self._find_fuzzy_match(file_sku, shopify_sku_map)
            
            if match_result:
                matched_item = {
                    'file_sku': file_sku,
                    'shopify_sku': match_result['sku'],
                    'variant_id': match_result['variant_id'],
                    'product_id': match_result['product_id'],
                    'product_title': match_result['product_title'],
                    'current_quantity': match_result['current_quantity'],
                    'new_quantity': quantity,
                    'match_type': match_result['match_type'],
                    'confidence': match_result.get('confidence'),
                    'row_index': index
                }
                matched_data.append(matched_item)
            else:
                # No match found
                matched_item = {
                    'file_sku': file_sku,
                    'shopify_sku': None,
                    'variant_id': None,
                    'product_id': None,
                    'product_title': 'No Match Found',
                    'current_quantity': 0,
                    'new_quantity': quantity,
                    'match_type': 'no_match',
                    'confidence': None,
                    'row_index': index
                }
                matched_data.append(matched_item)
        
        return matched_data
    
    def _create_shopify_sku_map(self, shopify_products: List[Dict]) -> Dict[str, Dict]:
        """
        Create a mapping of SKUs to product information.
        
        Args:
            shopify_products: List of Shopify products
            
        Returns:
            Dict: SKU to product info mapping
        """
        sku_map = {}
        
        for product in shopify_products:
            if 'variants' in product:
                for variant in product['variants']:
                    sku = variant.get('sku', '').strip()
                    
                    if sku:  # Only add if SKU exists
                        sku_map[sku] = {
                            'sku': sku,
                            'variant_id': variant.get('id'),
                            'product_id': product.get('id'),
                            'product_title': product.get('title', ''),
                            'current_quantity': variant.get('inventory_quantity', 0),
                            'variant_title': variant.get('title', ''),
                            'price': variant.get('price', '0.00')
                        }
        
        return sku_map
    
    def _find_exact_match(self, file_sku: str, shopify_sku_map: Dict) -> Optional[Dict]:
        """
        Find exact SKU match.
        
        Args:
            file_sku: SKU from uploaded file
            shopify_sku_map: Shopify SKU mapping
            
        Returns:
            Dict or None: Match result
        """
        # Try exact match (case-sensitive)
        if file_sku in shopify_sku_map:
            result = shopify_sku_map[file_sku].copy()
            result['match_type'] = 'exact'
            result['confidence'] = 1.0
            return result
        
        # Try case-insensitive exact match
        for sku, product_info in shopify_sku_map.items():
            if file_sku.lower() == sku.lower():
                result = product_info.copy()
                result['match_type'] = 'exact'
                result['confidence'] = 1.0
                return result
        
        return None
    
    def _find_fuzzy_match(self, file_sku: str, shopify_sku_map: Dict) -> Optional[Dict]:
        """
        Find fuzzy SKU match using string similarity.
        
        Args:
            file_sku: SKU from uploaded file
            shopify_sku_map: Shopify SKU mapping
            
        Returns:
            Dict or None: Match result
        """
        if not shopify_sku_map:
            return None
        
        # Get all Shopify SKUs
        shopify_skus = list(shopify_sku_map.keys())
        
        # Find best match using fuzzy string matching
        best_match = process.extractOne(
            file_sku,
            shopify_skus,
            scorer=fuzz.ratio
        )
        
        if best_match and best_match[1] >= self.fuzzy_threshold:
            matched_sku = best_match[0]
            confidence = best_match[1] / 100.0
            
            result = shopify_sku_map[matched_sku].copy()
            result['match_type'] = 'fuzzy'
            result['confidence'] = confidence
            return result
        
        return None
    
    def _parse_quantity(self, quantity_value) -> int:
        """
        Parse quantity value to integer.
        
        Args:
            quantity_value: Raw quantity value
            
        Returns:
            int: Parsed quantity (defaults to 0 if invalid)
        """
        try:
            if pd.isna(quantity_value):
                return 0
            
            # Convert to string and clean
            qty_str = str(quantity_value).strip().replace(',', '')
            
            # Try to convert to float first, then int
            qty_float = float(qty_str)
            return int(qty_float)
            
        except (ValueError, TypeError):
            return 0
    
    def get_matching_statistics(self, matched_data: List[Dict]) -> Dict:
        """
        Get statistics about the matching results.
        
        Args:
            matched_data: List of matched data
            
        Returns:
            Dict: Matching statistics
        """
        total_skus = len(matched_data)
        exact_matches = len([m for m in matched_data if m['match_type'] == 'exact'])
        fuzzy_matches = len([m for m in matched_data if m['match_type'] == 'fuzzy'])
        no_matches = len([m for m in matched_data if m['match_type'] == 'no_match'])
        
        # Calculate confidence statistics for fuzzy matches
        fuzzy_confidences = [m['confidence'] for m in matched_data if m['match_type'] == 'fuzzy']
        avg_fuzzy_confidence = np.mean(fuzzy_confidences) if fuzzy_confidences else 0
        
        return {
            'total_skus': total_skus,
            'exact_matches': exact_matches,
            'fuzzy_matches': fuzzy_matches,
            'no_matches': no_matches,
            'match_rate': (exact_matches + fuzzy_matches) / total_skus if total_skus > 0 else 0,
            'exact_match_rate': exact_matches / total_skus if total_skus > 0 else 0,
            'fuzzy_match_rate': fuzzy_matches / total_skus if total_skus > 0 else 0,
            'avg_fuzzy_confidence': avg_fuzzy_confidence
        }
    
    def filter_matches(self, matched_data: List[Dict], 
                      include_exact: bool = True,
                      include_fuzzy: bool = True,
                      min_confidence: float = 0.0,
                      exclude_zero_qty: bool = False) -> List[Dict]:
        """
        Filter matched data based on criteria.
        
        Args:
            matched_data: List of matched data
            include_exact: Include exact matches
            include_fuzzy: Include fuzzy matches  
            min_confidence: Minimum confidence for fuzzy matches
            exclude_zero_qty: Exclude zero quantity items
            
        Returns:
            List[Dict]: Filtered matched data
        """
        filtered_data = []
        
        for item in matched_data:
            # Skip no matches
            if item['match_type'] == 'no_match':
                continue
            
            # Check match type filters
            if item['match_type'] == 'exact' and not include_exact:
                continue
            
            if item['match_type'] == 'fuzzy' and not include_fuzzy:
                continue
            
            # Check confidence threshold for fuzzy matches
            if (item['match_type'] == 'fuzzy' and 
                item['confidence'] is not None and 
                item['confidence'] < min_confidence):
                continue
            
            # Check quantity filter
            if exclude_zero_qty and item['new_quantity'] == 0:
                continue
            
            filtered_data.append(item)
        
        return filtered_data
    
    def export_matching_report(self, matched_data: List[Dict]) -> pd.DataFrame:
        """
        Create a detailed matching report as DataFrame.
        
        Args:
            matched_data: List of matched data
            
        Returns:
            pd.DataFrame: Matching report
        """
        report_data = []
        
        for item in matched_data:
            report_data.append({
                'File_SKU': item['file_sku'],
                'Shopify_SKU': item['shopify_sku'] or 'No Match',
                'Product_Title': item['product_title'],
                'Match_Type': item['match_type'].title(),
                'Confidence': f"{item['confidence']:.1%}" if item['confidence'] else 'N/A',
                'Current_Quantity': item['current_quantity'],
                'New_Quantity': item['new_quantity'],
                'Quantity_Change': item['new_quantity'] - item['current_quantity'],
                'Variant_ID': item['variant_id'] or 'N/A',
                'Product_ID': item['product_id'] or 'N/A'
            })
        
        return pd.DataFrame(report_data)