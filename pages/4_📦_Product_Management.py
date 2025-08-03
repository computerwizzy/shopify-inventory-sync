import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import json
import os
import sys
from typing import Dict, List, Optional
import time
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.shopify_client import ShopifyClient
from src.cache_manager import cache_manager
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

# Initialize pagination state
if 'current_page' not in st.session_state:
    st.session_state.current_page = 1
if 'products_per_page' not in st.session_state:
    st.session_state.products_per_page = 25

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
    """Fast-loading dashboard with key metrics and quick actions."""
    st.header("📊 Inventory Dashboard")
    
    # Cache control buttons
    col1, col2, col3 = st.columns([1, 1, 4])
    with col1:
        if st.button("🔄 Refresh Data", help="Clear cache and reload data"):
            cache_manager.invalidate("quick_metrics")
            cache_manager.invalidate("low_stock")
            st.rerun()
    
    with col2:
        cache_info = cache_manager.get_cache_info()
        st.caption(f"Cache: {cache_info['active_entries']} entries")
    
    try:
        # Use cached quick metrics for fast loading
        with st.spinner("Loading dashboard metrics..."):
            metrics = cache_manager.cached_call(
                st.session_state.shopify_client.get_quick_metrics,
                "quick_metrics",
                ttl=300  # 5 minutes cache
            )
        
        if 'error' in metrics:
            st.error(f"❌ Error loading metrics: {metrics['error']}")
            return
        
        # Display performance indicator
        if metrics.get('is_estimate'):
            st.info(f"📊 **Quick Dashboard** - Showing estimates based on {metrics['sample_size']} recent products. Use 'Browse Products' for detailed view.")
        
        # Key metrics row
        col1, col2, col3, col4, col5 = st.columns(5)
        
        with col1:
            st.metric(
                "Total Products", 
                f"{metrics['total_products']:,}",
                help="Total number of products in your store"
            )
        
        with col2:
            st.metric(
                "Total Variants", 
                f"{metrics['estimated_variants']:,}",
                help="Estimated total number of product variants"
            )
        
        with col3:
            st.metric(
                "Total Inventory", 
                f"{metrics['estimated_inventory']:,}",
                help="Estimated total inventory quantity"
            )
        
        with col4:
            st.metric(
                "Inventory Value", 
                f"${metrics['estimated_value']:,.2f}",
                help="Estimated total inventory value"
            )
        
        with col5:
            st.metric(
                "Store", 
                metrics['shop_name'],
                help="Your Shopify store name"
            )
        
        # Stock status alerts
        st.markdown("---")
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric(
                "⚠️ Low Stock", 
                f"{metrics['estimated_low_stock']:,}",
                help="Estimated items with 5 or fewer units in stock"
            )
        
        with col2:
            st.metric(
                "🚫 Out of Stock", 
                f"{metrics['estimated_out_of_stock']:,}",
                help="Estimated items with zero inventory"
            )
        
        with col3:
            st.metric(
                "✅ Well Stocked", 
                f"{metrics['estimated_well_stocked']:,}",
                help="Estimated items with healthy stock levels"
            )
        
        # Quick actions
        st.markdown("---")
        st.subheader("🚀 Quick Actions")
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            if st.button("🔍 Browse Products", type="primary", help="Browse and manage your products"):
                # Switch to browse tab
                pass
        
        with col2:
            if st.button("📤 Export Sample", help="Export sample data for analysis"):
                # Create a sample export
                st.info("Use 'Browse Products' tab to export full inventory data")
        
        with col3:
            if st.button("🔄 Sync Now", help="Run inventory synchronization"):
                st.info("Navigate to Scheduled Sync to run inventory synchronization")
        
        with col4:
            if st.button("⚙️ Bulk Operations", help="Perform bulk operations on products"):
                st.info("Use the Bulk Operations tab for mass updates")
        
        # Quick insights
        st.markdown("---")
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("📊 Quick Insights")
            if metrics['estimated_variants'] > 0:
                out_of_stock_pct = (metrics['estimated_out_of_stock'] / metrics['estimated_variants']) * 100
                low_stock_pct = (metrics['estimated_low_stock'] / metrics['estimated_variants']) * 100
                
                st.write(f"**Out of Stock:** {out_of_stock_pct:.1f}% of variants")
                st.write(f"**Low Stock:** {low_stock_pct:.1f}% of variants")
                
                if out_of_stock_pct > 20:
                    st.warning("⚠️ High percentage of out-of-stock items!")
                elif out_of_stock_pct > 10:
                    st.info("💡 Consider restocking out-of-stock items")
                else:
                    st.success("✅ Good stock availability")
        
        with col2:
            st.subheader("⚡ Performance Tips")
            st.markdown("""
            - **Browse Products**: View paginated product list
            - **Use Filters**: Filter by stock status to focus on issues  
            - **Bulk Operations**: Update multiple products at once
            - **Scheduled Sync**: Automate inventory updates
            - **Cache Refresh**: Click refresh to update metrics
            """)
        
        # Load and display critical alerts asynchronously
        if st.button("🔍 Show Critical Alerts", help="Load detailed low stock and out of stock items"):
            show_critical_alerts()
    
    except Exception as e:
        st.error(f"❌ Error loading dashboard: {str(e)}")
        st.info("💡 Try refreshing the page or check your Shopify connection.")

def show_critical_alerts():
    """Show critical stock alerts with caching."""
    try:
        with st.spinner("Loading critical alerts..."):
            # Use cache for critical alerts
            low_stock_items = cache_manager.cached_call(
                st.session_state.shopify_client.get_low_stock_products,
                "low_stock_products",
                ttl=180,  # 3 minutes cache
                threshold=5
            )
            
            out_of_stock_items = cache_manager.cached_call(
                st.session_state.shopify_client.get_out_of_stock_products,
                "out_of_stock_products",
                ttl=180  # 3 minutes cache
            )
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("⚠️ Low Stock Items")
            if low_stock_items:
                df_low = pd.DataFrame(low_stock_items)
                st.dataframe(
                    df_low[['title', 'sku', 'inventory_quantity', 'price']], 
                    use_container_width=True,
                    hide_index=True
                )
            else:
                st.success("✅ No low stock items!")
        
        with col2:
            st.subheader("🚫 Out of Stock Items")
            if out_of_stock_items:
                df_out = pd.DataFrame(out_of_stock_items)
                st.dataframe(
                    df_out[['title', 'sku', 'price']], 
                    use_container_width=True,
                    hide_index=True
                )
            else:
                st.success("✅ No out of stock items!")
    
    except Exception as e:
        st.error(f"❌ Error loading alerts: {str(e)}")

def browse_products_tab():
    """Browse and search products with pagination for better performance."""
    st.header("🔍 Browse Products")
    
    # Search and filter controls
    col1, col2, col3, col4 = st.columns([2, 1, 1, 1])
    
    with col1:
        search_term = st.text_input(
            "🔍 Search Products", 
            placeholder="Search by title, SKU, or product type...",
            key="product_search"
        )
    
    with col2:
        stock_filter = st.selectbox(
            "📦 Stock Status",
            ["All", "In Stock", "Low Stock (≤5)", "Out of Stock"],
            key="stock_filter"
        )
    
    with col3:
        st.session_state.products_per_page = st.selectbox(
            "Items per page",
            [10, 25, 50, 100],
            index=1,  # Default to 25
            key="items_per_page"
        )
    
    with col4:
        view_mode = st.selectbox("View", ["Table", "Cards"], key="view_mode")
    
    # Reset pagination when filters change
    if st.button("🔍 Search", type="primary") or 'last_search' not in st.session_state or st.session_state.get('last_search') != (search_term, stock_filter):
        st.session_state.current_page = 1
        st.session_state.last_search = (search_term, stock_filter)
    
    try:
        # Load products with pagination and caching
        with st.spinner("Loading products..."):
            cache_key = f"products_page_{st.session_state.current_page}_{st.session_state.products_per_page}_{search_term}_{stock_filter}"
            
            products_data = cache_manager.cached_call(
                st.session_state.shopify_client.get_products_paginated,
                "products_paginated",
                ttl=120,  # 2 minutes cache
                page=st.session_state.current_page,
                limit=st.session_state.products_per_page,
                search=search_term if search_term else None,
                stock_filter=stock_filter if stock_filter != "All" else None
            )
        
        if 'error' in products_data:
            st.error(f"❌ Error loading products: {products_data['error']}")
            return
        
        products = products_data['products']
        
        if not products:
            st.warning("No products found matching your criteria.")
            return
        
        # Display results info and pagination controls
        col1, col2, col3 = st.columns([2, 1, 1])
        
        with col1:
            st.write(f"**Showing {len(products)} products** (Page {st.session_state.current_page})")
        
        with col2:
            if st.button("⬅️ Previous", disabled=st.session_state.current_page <= 1):
                st.session_state.current_page -= 1
                st.rerun()
        
        with col3:
            if st.button("Next ➡️", disabled=not products_data.get('has_more', False)):
                st.session_state.current_page += 1
                st.rerun()
        
        # Display products
        if view_mode == "Table":
            display_products_table(products)
        else:
            display_products_cards(products)
        
        # Pagination controls at bottom
        st.markdown("---")
        col1, col2, col3, col4 = st.columns([1, 1, 1, 1])
        
        with col1:
            if st.button("⬅️ Prev", disabled=st.session_state.current_page <= 1, key="prev_bottom"):
                st.session_state.current_page -= 1
                st.rerun()
        
        with col2:
            st.write(f"Page {st.session_state.current_page}")
        
        with col3:
            if st.button("Next ➡️", disabled=not products_data.get('has_more', False), key="next_bottom"):
                st.session_state.current_page += 1
                st.rerun()
        
        with col4:
            if st.button("🔄 Refresh"):
                cache_manager.invalidate("products_paginated")
                st.rerun()
    
    except Exception as e:
        st.error(f"❌ Error loading products: {str(e)}")
        st.info("💡 Try reducing the number of items per page or refreshing the page.")

def display_products_table(products):
    """Display products in table format with color coding."""
    df = pd.DataFrame(products)
    
    if df.empty:
        return
    
    # Prepare display columns
    display_cols = ['title', 'variant_title', 'sku', 'price', 'inventory_quantity', 'product_type', 'vendor']
    
    # Create a formatted version for display
    display_df = df[display_cols].copy()
    display_df['price'] = display_df['price'].apply(lambda x: f"${x:.2f}")
    
    # Color code based on stock status
    def color_stock_status(row):
        colors = []
        for col in display_cols:
            if col == 'inventory_quantity':
                qty = row[col]
                if qty == 0:
                    colors.append('background-color: #ffcdd2')  # Light red
                elif qty <= 5:
                    colors.append('background-color: #ffe0b2')  # Light orange  
                else:
                    colors.append('background-color: #dcedc8')  # Light green
            else:
                colors.append('')
        return colors
    
    # Apply styling
    styled_df = display_df.style.apply(color_stock_status, axis=1)
    
    st.dataframe(styled_df, use_container_width=True, hide_index=True)

def display_products_cards(products):
    """Display products in card format."""
    # Display 3 cards per row
    for i in range(0, len(products), 3):
        cols = st.columns(3)
        for j, col in enumerate(cols):
            if i + j < len(products):
                product = products[i + j]
                with col:
                    with st.container():
                        st.markdown(f"**{product['title']}**")
                        if product['variant_title'] != 'Default Title':
                            st.caption(f"*{product['variant_title']}*")
                        
                        # Product details
                        col_a, col_b = st.columns(2)
                        with col_a:
                            st.write(f"**Price:** ${product['price']:.2f}")
                            st.write(f"**SKU:** {product['sku']}")
                        with col_b:
                            # Stock status with color
                            stock = product['inventory_quantity']
                            status = product.get('stock_status', 'good')
                            
                            if status == 'out':
                                st.write(f"**Stock:** :red[{stock}] (Out)")
                            elif status == 'low':
                                st.write(f"**Stock:** :orange[{stock}] (Low)")
                            else:
                                st.write(f"**Stock:** :green[{stock}] (Good)")
                            
                            st.write(f"**Type:** {product.get('product_type', 'N/A')}")
                        
                        # Quick edit button
                        if st.button(f"✏️ Edit", key=f"edit_{product['variant_id']}", use_container_width=True):
                            edit_product_modal(product)

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