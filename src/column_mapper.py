import streamlit as st
import pandas as pd
from typing import List, Dict, Optional

class ColumnMapper:
    """Handles column mapping interface for inventory data."""
    
    def __init__(self, file_columns: List[str]):
        self.file_columns = file_columns
        self.required_fields = {
            'SKU': 'Product SKU or unique identifier',
            'Quantity': 'Available inventory quantity'
        }
        self.optional_fields = {
            'Product Title': 'Product name or title',
            'Barcode': 'Product barcode',
            'Price': 'Product price',
            'Compare At Price': 'Original/compare at price',
            'Cost': 'Product cost',
            'Weight': 'Product weight',
            'Inventory Policy': 'deny or continue when out of stock',
            'Fulfillment Service': 'manual or other fulfillment service',
            'Inventory Management': 'shopify or other inventory tracker'
        }
    
    def create_mapping_interface(self) -> Dict[str, str]:
        """
        Create interactive column mapping interface.
        
        Returns:
            Dict[str, str]: Mapping of required fields to file columns
        """
        st.subheader("🔗 Column Mapping")
        
        # Add helpful info
        with st.expander("ℹ️ Column Mapping Help"):
            st.markdown("""
            **Required Fields:**
            - **SKU**: Must contain unique product identifiers that match your Shopify products
            - **Quantity**: Must contain numeric inventory quantities
            
            **Optional Fields:**
            - Map additional fields if available to enhance the sync process
            - Unmapped fields will be ignored during sync
            """)
        
        # Auto-suggest mappings
        auto_mapping = self._auto_suggest_mapping()
        
        if auto_mapping:
            st.info("💡 Auto-suggested mappings based on column names:")
            for field, suggested_column in auto_mapping.items():
                st.write(f"**{field}** → {suggested_column}")
        
        # Create mapping interface
        mapping = {}
        
        # Required fields section
        st.markdown("### 📋 Required Fields")
        
        col1, col2 = st.columns(2)
        
        for i, (field, description) in enumerate(self.required_fields.items()):
            with col1 if i % 2 == 0 else col2:
                # Pre-select auto-suggested column if available
                default_index = 0
                if field in auto_mapping and auto_mapping[field] in self.file_columns:
                    default_index = self.file_columns.index(auto_mapping[field]) + 1
                
                selected_column = st.selectbox(
                    f"**{field}**",
                    ["-- Select Column --"] + self.file_columns,
                    index=default_index,
                    help=description,
                    key=f"required_{field}"
                )
                
                mapping[field] = selected_column
        
        # Optional fields section
        st.markdown("### 📝 Optional Fields")
        
        with st.expander("Map Optional Fields"):
            col1, col2 = st.columns(2)
            
            for i, (field, description) in enumerate(self.optional_fields.items()):
                with col1 if i % 2 == 0 else col2:
                    # Pre-select auto-suggested column if available
                    default_index = 0
                    if field in auto_mapping and auto_mapping[field] in self.file_columns:
                        default_index = self.file_columns.index(auto_mapping[field]) + 1
                    
                    selected_column = st.selectbox(
                        f"**{field}**",
                        ["-- Select Column --"] + self.file_columns,
                        index=default_index,
                        help=description,
                        key=f"optional_{field}"
                    )
                    
                    if selected_column != "-- Select Column --":
                        mapping[field] = selected_column
        
        # Validation
        self._validate_mapping(mapping)
        
        return mapping
    
    def _auto_suggest_mapping(self) -> Dict[str, str]:
        """
        Auto-suggest column mappings based on column names.
        
        Returns:
            Dict[str, str]: Suggested mappings
        """
        suggestions = {}
        
        # Common variations for each field
        field_variations = {
            'SKU': ['sku', 'product_sku', 'item_sku', 'variant_sku', 'code', 'item_code', 'product_code'],
            'Quantity': ['quantity', 'qty', 'stock', 'inventory', 'available', 'on_hand', 'in_stock'],
            'Product Title': ['title', 'name', 'product_name', 'product_title', 'item_name', 'description'],
            'Barcode': ['barcode', 'upc', 'ean', 'gtin', 'isbn'],
            'Price': ['price', 'unit_price', 'selling_price', 'retail_price'],
            'Compare At Price': ['compare_at_price', 'msrp', 'list_price', 'original_price'],
            'Cost': ['cost', 'unit_cost', 'wholesale_price', 'cost_price'],
            'Weight': ['weight', 'product_weight', 'item_weight', 'shipping_weight'],
            'Inventory Policy': ['inventory_policy', 'out_of_stock_policy'],
            'Fulfillment Service': ['fulfillment_service', 'fulfillment'],
            'Inventory Management': ['inventory_management', 'inventory_tracking']
        }
        
        # Convert file columns to lowercase for comparison
        lower_columns = [col.lower().replace(' ', '_').replace('-', '_') for col in self.file_columns]
        
        for field, variations in field_variations.items():
            for variation in variations:
                if variation in lower_columns:
                    original_column = self.file_columns[lower_columns.index(variation)]
                    suggestions[field] = original_column
                    break
        
        return suggestions
    
    def _validate_mapping(self, mapping: Dict[str, str]) -> None:
        """
        Validate the column mapping and show warnings/errors.
        
        Args:
            mapping: Current column mapping
        """
        # Check for required fields
        missing_required = []
        for field in self.required_fields.keys():
            if mapping.get(field) == "-- Select Column --" or not mapping.get(field):
                missing_required.append(field)
        
        if missing_required:
            st.error(f"❌ Missing required fields: {', '.join(missing_required)}")
        
        # Check for duplicate mappings
        used_columns = [col for col in mapping.values() if col != "-- Select Column --"]
        duplicates = [col for col in used_columns if used_columns.count(col) > 1]
        
        if duplicates:
            st.warning(f"⚠️ Warning: Column(s) mapped multiple times: {', '.join(set(duplicates))}")
        
        # Show mapping summary
        valid_mappings = {k: v for k, v in mapping.items() if v != "-- Select Column --"}
        if valid_mappings:
            st.success(f"✅ {len(valid_mappings)} field(s) mapped successfully")
    
    def get_mapped_data(self, df: pd.DataFrame, mapping: Dict[str, str]) -> pd.DataFrame:
        """
        Apply column mapping to DataFrame.
        
        Args:
            df: Original DataFrame
            mapping: Column mapping dictionary
            
        Returns:
            pd.DataFrame: DataFrame with mapped columns
        """
        # Create new DataFrame with mapped columns
        mapped_df = pd.DataFrame()
        
        for field, column in mapping.items():
            if column != "-- Select Column --" and column in df.columns:
                mapped_df[field] = df[column].copy()
        
        return mapped_df
    
    def validate_mapped_data(self, mapped_df: pd.DataFrame) -> Dict:
        """
        Validate the mapped data for common issues.
        
        Args:
            mapped_df: DataFrame with mapped columns
            
        Returns:
            Dict: Validation results
        """
        validation_result = {
            'valid': True,
            'errors': [],
            'warnings': []
        }
        
        # Check for required columns
        if 'SKU' not in mapped_df.columns:
            validation_result['valid'] = False
            validation_result['errors'].append("SKU column is required")
        
        if 'Quantity' not in mapped_df.columns:
            validation_result['valid'] = False
            validation_result['errors'].append("Quantity column is required")
        
        if not validation_result['valid']:
            return validation_result
        
        # Validate SKU column
        if mapped_df['SKU'].isnull().any():
            validation_result['warnings'].append("Some SKU values are missing")
        
        if mapped_df['SKU'].duplicated().any():
            validation_result['warnings'].append("Duplicate SKU values found")
        
        # Validate Quantity column
        try:
            # Try to convert quantity to numeric
            quantity_numeric = pd.to_numeric(mapped_df['Quantity'], errors='coerce')
            
            if quantity_numeric.isnull().any():
                validation_result['warnings'].append("Some quantity values are not numeric")
            
            if (quantity_numeric < 0).any():
                validation_result['warnings'].append("Negative quantity values found")
            
        except Exception as e:
            validation_result['warnings'].append(f"Issue with quantity data: {str(e)}")
        
        # Validate optional numeric columns
        numeric_fields = ['Price', 'Compare At Price', 'Cost', 'Weight']
        for field in numeric_fields:
            if field in mapped_df.columns:
                try:
                    pd.to_numeric(mapped_df[field], errors='coerce')
                except:
                    validation_result['warnings'].append(f"{field} contains non-numeric values")
        
        return validation_result