import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import json
import os
import sys
from typing import Dict, List, Optional
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.shopify_client import ShopifyClient
from utils.config import Config

st.set_page_config(
    page_title="Product Management",
    page_icon="📦",
    layout="wide"
)

# Initialize Shopify client
if 'shopify_client' not in st.session_state:
    try:
        config = Config()
        if not config.validate_shopify_config():
            st.error("⚠️ **Shopify Configuration Missing**")
            st.info("Please configure your Shopify credentials in the main app.")
            st.stop()
        st.session_state.shopify_client = ShopifyClient()
    except Exception as e:
        st.error(f"Failed to initialize Shopify Client: {e}")
        st.stop()

def main():
    st.title("📦 Product & Inventory Management")
    st.markdown("Complete product and inventory management for your Shopify store.")
    
    # Main tabs
    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
        "📊 Dashboard", 
        "🔍 Browse Products", 
        "✏️ Bulk Operations", 
        "📈 Analytics", 
        "⚠️ Alerts", 
        "⚙️ Settings"
    ])
    
    with tab1:
        dashboard_tab()
    
    with tab2:
        browse_products_tab()
    
    with tab3:
        bulk_operations_tab()
    
    with tab4:
        analytics_tab()
    
    with tab5:
        alerts_tab()
    
    with tab6:
        settings_tab()

def dashboard_tab():
    """Main dashboard with key metrics and quick actions."""
    st.header("📊 Inventory Dashboard")
    
    try:
        with st.spinner("Loading dashboard data..."):
            # Get shop info
            shop_info = st.session_state.shopify_client.get_shop_info()
            
            # Get products with inventory data
            products = st.session_state.shopify_client.get_all_products()
            
            if not products:
                st.warning("No products found in your store.")
                return
            
            # Process inventory data
            inventory_data = []
            total_value = 0
            low_stock_count = 0
            out_of_stock_count = 0
            
            for product in products:
                for variant in product.get('variants', []):
                    inventory_qty = variant.get('inventory_quantity', 0)
                    price = float(variant.get('price', 0))
                    cost = float(variant.get('cost', 0)) if variant.get('cost') else price * 0.6  # Estimate if no cost
                    
                    inventory_data.append({
                        'product_id': product['id'],
                        'variant_id': variant['id'],
                        'title': product['title'],
                        'variant_title': variant.get('title', ''),
                        'sku': variant.get('sku', ''),
                        'price': price,
                        'cost': cost,
                        'inventory_quantity': inventory_qty,
                        'inventory_value': inventory_qty * cost
                    })
                    
                    total_value += inventory_qty * cost
                    
                    if inventory_qty == 0:
                        out_of_stock_count += 1
                    elif inventory_qty <= 5:  # Low stock threshold
                        low_stock_count += 1
            
            df = pd.DataFrame(inventory_data)
            
            # Key metrics
            col1, col2, col3, col4, col5 = st.columns(5)
            
            with col1:
                st.metric(
                    "Total Products", 
                    len(products),
                    help="Total number of products in your store"
                )
            
            with col2:
                st.metric(
                    "Total Variants", 
                    len(df),
                    help="Total number of product variants"
                )
            
            with col3:
                total_inventory = df['inventory_quantity'].sum()
                st.metric(
                    "Total Inventory", 
                    f"{total_inventory:,}",
                    help="Total inventory quantity across all variants"
                )
            
            with col4:
                st.metric(
                    "Inventory Value", 
                    f"${total_value:,.2f}",
                    help="Total value of inventory at cost"
                )
            
            with col5:
                st.metric(
                    "Shop Name", 
                    shop_info.get('name', 'Unknown'),
                    help="Your Shopify store name"
                )
            
            # Alert metrics
            st.markdown("---")
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.metric(
                    "⚠️ Low Stock Items", 
                    low_stock_count,
                    help="Items with 5 or fewer units in stock"
                )
            
            with col2:
                st.metric(
                    "🚫 Out of Stock", 
                    out_of_stock_count,
                    help="Items with zero inventory"
                )
            
            with col3:
                in_stock_count = len(df) - out_of_stock_count - low_stock_count
                st.metric(
                    "✅ Well Stocked", 
                    in_stock_count,
                    help="Items with healthy stock levels"
                )
            
            # Quick actions
            st.markdown("---")
            st.subheader("🚀 Quick Actions")
            
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                if st.button("📤 Export Inventory", help="Export current inventory to CSV"):
                    csv = df.to_csv(index=False)
                    st.download_button(
                        label="⬇️ Download CSV",
                        data=csv,
                        file_name=f"inventory_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                        mime="text/csv"
                    )
            
            with col2:
                if st.button("🔄 Sync Inventory", help="Sync inventory from connected feeds"):
                    st.info("Navigate to Scheduled Sync to run inventory synchronization")
            
            with col3:
                if st.button("📊 View Analytics", help="Open detailed analytics dashboard"):
                    st.switch_page("pages/4_📦_Product_Management.py")  # Switch to analytics tab
            
            with col4:
                if st.button("⚙️ Bulk Update", help="Perform bulk operations on products"):
                    st.info("Use the Bulk Operations tab for mass updates")
            
            # Recent activity or top products
            if len(df) > 0:
                st.markdown("---")
                col1, col2 = st.columns(2)
                
                with col1:
                    st.subheader("📈 Top Products by Value")
                    top_products = df.nlargest(10, 'inventory_value')[['title', 'sku', 'inventory_quantity', 'inventory_value']]
                    st.dataframe(top_products, use_container_width=True, hide_index=True)
                
                with col2:
                    st.subheader("⚠️ Low Stock Alert")
                    low_stock_items = df[df['inventory_quantity'] <= 5][['title', 'variant_title', 'sku', 'inventory_quantity']]
                    if not low_stock_items.empty:
                        st.dataframe(low_stock_items, use_container_width=True, hide_index=True)
                    else:
                        st.success("✅ No low stock items!")
            
    except Exception as e:
        st.error(f"❌ Error loading dashboard: {str(e)}")

def browse_products_tab():
    """Browse and search products with advanced filtering."""
    st.header("🔍 Browse Products")
    
    try:
        with st.spinner("Loading products..."):
            products = st.session_state.shopify_client.get_all_products()
            
            if not products:
                st.warning("No products found in your store.")
                return
            
            # Create DataFrame for easier manipulation
            product_data = []
            for product in products:
                for variant in product.get('variants', []):
                    product_data.append({
                        'product_id': product['id'],
                        'variant_id': variant['id'],
                        'title': product['title'],
                        'variant_title': variant.get('title', 'Default Title'),
                        'sku': variant.get('sku', ''),
                        'price': float(variant.get('price', 0)),
                        'inventory_quantity': variant.get('inventory_quantity', 0),
                        'inventory_policy': variant.get('inventory_policy', 'deny'),
                        'fulfillment_service': variant.get('fulfillment_service', 'manual'),
                        'weight': variant.get('weight', 0),
                        'created_at': product.get('created_at', ''),
                        'updated_at': product.get('updated_at', ''),
                        'product_type': product.get('product_type', ''),
                        'vendor': product.get('vendor', ''),
                        'tags': product.get('tags', '')
                    })
            
            df = pd.DataFrame(product_data)
            
            # Search and filter controls
            col1, col2, col3 = st.columns([2, 1, 1])
            
            with col1:
                search_term = st.text_input(
                    "🔍 Search Products", 
                    placeholder="Search by title, SKU, or product type..."
                )
            
            with col2:
                stock_filter = st.selectbox(
                    "📦 Stock Status",
                    ["All", "In Stock", "Low Stock (≤5)", "Out of Stock"]
                )
            
            with col3:
                sort_by = st.selectbox(
                    "📊 Sort By",
                    ["Title", "Price", "Inventory", "Updated"]
                )
            
            # Apply filters
            filtered_df = df.copy()
            
            if search_term:
                mask = (
                    filtered_df['title'].str.contains(search_term, case=False, na=False) |
                    filtered_df['sku'].str.contains(search_term, case=False, na=False) |
                    filtered_df['product_type'].str.contains(search_term, case=False, na=False)
                )
                filtered_df = filtered_df[mask]
            
            if stock_filter == "In Stock":
                filtered_df = filtered_df[filtered_df['inventory_quantity'] > 5]
            elif stock_filter == "Low Stock (≤5)":
                filtered_df = filtered_df[(filtered_df['inventory_quantity'] > 0) & (filtered_df['inventory_quantity'] <= 5)]
            elif stock_filter == "Out of Stock":
                filtered_df = filtered_df[filtered_df['inventory_quantity'] == 0]
            
            # Sort results
            if sort_by == "Title":
                filtered_df = filtered_df.sort_values('title')
            elif sort_by == "Price":
                filtered_df = filtered_df.sort_values('price', ascending=False)
            elif sort_by == "Inventory":
                filtered_df = filtered_df.sort_values('inventory_quantity', ascending=False)
            elif sort_by == "Updated":
                filtered_df = filtered_df.sort_values('updated_at', ascending=False)
            
            # Display results
            st.write(f"**Showing {len(filtered_df)} of {len(df)} products**")
            
            # Product cards or table view
            view_mode = st.radio("View Mode", ["Table", "Cards"], horizontal=True)
            
            if view_mode == "Table":
                # Prepare display columns
                display_cols = [
                    'title', 'variant_title', 'sku', 'price', 
                    'inventory_quantity', 'product_type', 'vendor'
                ]
                
                # Add color coding for stock levels
                def highlight_stock(val):
                    if val == 0:
                        return 'background-color: #ffebee'  # Light red
                    elif val <= 5:
                        return 'background-color: #fff3e0'  # Light orange
                    else:
                        return 'background-color: #e8f5e8'  # Light green
                
                styled_df = filtered_df[display_cols].style.applymap(
                    highlight_stock, 
                    subset=['inventory_quantity']
                )
                
                st.dataframe(styled_df, use_container_width=True, hide_index=True)
                
            else:  # Cards view
                # Display as cards
                for i in range(0, len(filtered_df), 3):
                    cols = st.columns(3)
                    for j, col in enumerate(cols):
                        if i + j < len(filtered_df):
                            row = filtered_df.iloc[i + j]
                            with col:
                                with st.container():
                                    st.markdown(f"**{row['title']}**")
                                    if row['variant_title'] != 'Default Title':
                                        st.write(f"*{row['variant_title']}*")
                                    
                                    col_a, col_b = st.columns(2)
                                    with col_a:
                                        st.write(f"**Price:** ${row['price']:.2f}")
                                        st.write(f"**SKU:** {row['sku']}")
                                    with col_b:
                                        # Stock status with color
                                        stock = row['inventory_quantity']
                                        if stock == 0:
                                            st.write(f"**Stock:** :red[{stock}] (Out)")
                                        elif stock <= 5:
                                            st.write(f"**Stock:** :orange[{stock}] (Low)")
                                        else:
                                            st.write(f"**Stock:** :green[{stock}] (Good)")
                                        
                                        st.write(f"**Type:** {row['product_type']}")
                                    
                                    if st.button(f"Edit", key=f"edit_{row['variant_id']}"):
                                        edit_product_modal(row)
            
    except Exception as e:
        st.error(f"❌ Error loading products: {str(e)}")

def edit_product_modal(product_data):
    """Modal for editing individual product."""
    st.subheader(f"✏️ Edit: {product_data['title']}")
    
    with st.form(f"edit_product_{product_data['variant_id']}"):
        col1, col2 = st.columns(2)
        
        with col1:
            new_price = st.number_input(
                "Price", 
                value=float(product_data['price']), 
                min_value=0.01, 
                format="%.2f"
            )
            new_inventory = st.number_input(
                "Inventory Quantity", 
                value=int(product_data['inventory_quantity']), 
                min_value=0
            )
        
        with col2:
            new_sku = st.text_input("SKU", value=product_data['sku'])
            inventory_policy = st.selectbox(
                "Inventory Policy",
                ["deny", "continue"],
                index=0 if product_data['inventory_policy'] == 'deny' else 1
            )
        
        if st.form_submit_button("💾 Update Product"):
            try:
                with st.spinner("Updating product..."):
                    # Update variant
                    update_data = {
                        'price': new_price,
                        'inventory_quantity': new_inventory,
                        'sku': new_sku,
                        'inventory_policy': inventory_policy
                    }
                    
                    success = st.session_state.shopify_client.update_variant_inventory(
                        product_data['variant_id'], 
                        new_inventory
                    )
                    
                    if success:
                        st.success("✅ Product updated successfully!")
                        st.rerun()
                    else:
                        st.error("❌ Failed to update product")
                        
            except Exception as e:
                st.error(f"❌ Error updating product: {str(e)}")

# Continue with other tab functions...
def bulk_operations_tab():
    """Bulk operations for products."""
    st.header("✏️ Bulk Operations")
    st.info("🚧 Bulk operations features coming soon! This will include bulk price updates, inventory adjustments, and CSV imports.")

def analytics_tab():
    """Analytics and reporting."""
    st.header("📈 Analytics & Reports")
    st.info("🚧 Advanced analytics coming soon! This will include inventory trends, sales forecasting, and custom reports.")

def alerts_tab():
    """Inventory alerts and notifications."""
    st.header("⚠️ Inventory Alerts")
    st.info("🚧 Alert system coming soon! This will include low stock notifications, reorder points, and automated alerts.")

def settings_tab():
    """App settings and preferences."""
    st.header("⚙️ Settings")
    st.info("🚧 Settings panel coming soon! This will include alert thresholds, currency settings, and export preferences.")

if __name__ == "__main__":
    main()