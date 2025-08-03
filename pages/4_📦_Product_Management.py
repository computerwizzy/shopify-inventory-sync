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

# Initialize cache store for the cache manager
if 'cache_store' not in st.session_state:
    st.session_state.cache_store = {}

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
    """Smart dashboard with load controls for large product catalogs."""
    st.header("📊 Inventory Dashboard")
    
    # Load control panel
    with st.expander("⚙️ **Load Controls** - Essential for Large Catalogs (10,000+ products)", expanded=False):
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            sample_size = st.selectbox(
                "📊 Sample Size",
                [50, 100, 250, 500, 1000],
                index=1,  # Default to 100
                help="Number of recent products to sample for estimates"
            )
        
        with col2:
            metrics_mode = st.selectbox(
                "📈 Metrics Mode", 
                ["Quick (Estimates)", "Detailed (Slower)", "Count Only"],
                help="Choose speed vs accuracy tradeoff"
            )
        
        with col3:
            auto_refresh = st.selectbox(
                "🔄 Auto Refresh",
                ["Manual", "5 min", "15 min", "1 hour"],
                help="Automatic cache refresh interval"
            )
        
        with col4:
            if st.button("🗑️ Clear All Cache", help="Clear all cached data"):
                cache_manager.invalidate()
                st.success("Cache cleared!")
                st.rerun()
    
    # Store info and basic stats (always fast)
    try:
        # Always show store info quickly
        shop_info = cache_manager.cached_call(
            st.session_state.shopify_client.get_shop_info,
            "shop_info",
            ttl=3600  # 1 hour cache for shop info
        )
        
        product_count = cache_manager.cached_call(
            st.session_state.shopify_client.get_products_count,
            "product_count",
            ttl=300  # 5 minutes cache
        )
        
        # Display store header
        col1, col2, col3 = st.columns([2, 1, 1])
        with col1:
            st.subheader(f"🏪 {shop_info.get('name', 'Your Store')}")
            st.caption(f"Total Products: **{product_count:,}**")
        
        with col2:
            cache_info = cache_manager.get_cache_info()
            st.metric("Cache Entries", cache_info['active_entries'])
        
        with col3:
            if st.button("🔄 Refresh", help="Refresh dashboard data"):
                cache_manager.invalidate("quick_metrics")
                cache_manager.invalidate("detailed_metrics")
                st.rerun()
        
        # Load appropriate metrics based on mode
        if metrics_mode == "Count Only":
            show_count_only_dashboard(product_count, shop_info)
        elif metrics_mode == "Quick (Estimates)":
            show_quick_dashboard(sample_size, product_count, shop_info)
        else:  # Detailed mode
            show_detailed_dashboard_with_warning(product_count, shop_info)
            
    except Exception as e:
        st.error(f"❌ Error loading dashboard: {str(e)}")
        st.info("💡 Try using 'Count Only' mode or check your Shopify connection.")

def show_count_only_dashboard(product_count, shop_info):
    """Ultra-fast dashboard showing only counts."""
    st.info("📊 **Count Only Mode** - Ultra-fast loading for large catalogs")
    
    # Basic metrics
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Total Products", f"{product_count:,}")
    with col2:
        st.metric("Store Status", "✅ Connected")
    with col3:
        st.metric("Mode", "Count Only")
    
    # Load control actions
    st.markdown("---")
    st.subheader("🎯 Targeted Actions")
    
    col1, col2 = st.columns(2)
    with col1:
        st.markdown("**🔍 Browse Specific Products**")
        if st.button("Browse with Filters", type="primary"):
            st.info("Switch to 'Browse Products' tab to search and filter specific products")
    
    with col2:
        st.markdown("**📊 Load Sample Analysis**")
        if st.button("Get Sample Metrics"):
            st.info("Switch to 'Quick (Estimates)' mode above to see sample-based metrics")

def show_quick_dashboard(sample_size, product_count, shop_info):
    """Quick dashboard with sample-based estimates."""
    with st.spinner(f"Loading sample of {sample_size} products..."):
        metrics = cache_manager.cached_call(
            lambda: st.session_state.shopify_client.get_quick_metrics_with_sample(sample_size),
            f"quick_metrics_{sample_size}",
            ttl=300  # 5 minutes cache
        )
    
    if 'error' in metrics:
        st.error(f"❌ Error loading metrics: {metrics['error']}")
        return
    
    # Performance indicator
    st.info(f"📊 **Quick Mode** - Estimates based on {metrics.get('sample_size', sample_size)} recent products")
    
    # Key metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Products", f"{product_count:,}")
        st.caption("Actual count")
    
    with col2:
        st.metric("Est. Variants", f"{metrics.get('estimated_variants', 0):,}")
        st.caption("Based on sample")
    
    with col3:
        st.metric("Est. Inventory", f"{metrics.get('estimated_inventory', 0):,}")
        st.caption("Total units")
    
    with col4:
        st.metric("Est. Value", f"${metrics.get('estimated_value', 0):,.2f}")
        st.caption("At retail price")
    
    # Stock alerts
    st.markdown("---")
    col1, col2, col3 = st.columns(3)
    
    with col1:
        low_stock = metrics.get('estimated_low_stock', 0)
        st.metric("⚠️ Est. Low Stock", f"{low_stock:,}")
    
    with col2:
        out_stock = metrics.get('estimated_out_of_stock', 0)
        st.metric("🚫 Est. Out of Stock", f"{out_stock:,}")
    
    with col3:
        well_stock = metrics.get('estimated_well_stocked', 0)
        st.metric("✅ Est. Well Stocked", f"{well_stock:,}")
    
    # Action buttons with load awareness
    st.markdown("---")
    st.subheader("🎯 Smart Actions")
    
    col1, col2 = st.columns(2)
    with col1:
        if st.button("🔍 Browse Products (Paginated)", type="primary"):
            st.info("Switch to 'Browse Products' tab - loads 25 products at a time")
    
    with col2:
        if st.button("⚠️ Load Critical Alerts"):
            show_smart_critical_alerts()

def show_detailed_dashboard_with_warning(product_count, shop_info):
    """Detailed dashboard with performance warnings."""
    # Performance warning for large catalogs
    if product_count > 1000:
        st.error(f"⚠️ **Performance Warning**: You have {product_count:,} products. Detailed mode may be very slow!")
        st.warning("**Recommendation**: Use 'Quick (Estimates)' mode for better performance.")
        
        if not st.checkbox("I understand this may take several minutes", key="detailed_warning"):
            st.stop()
    
    # Show estimated load time
    estimated_time = max(5, product_count // 200)  # Rough estimate
    st.info(f"⏱️ Estimated load time: ~{estimated_time} seconds for {product_count:,} products")
    
    # Load detailed metrics (this could be slow)
    with st.spinner(f"Loading detailed analysis of {product_count:,} products..."):
        try:
            detailed_metrics = cache_manager.cached_call(
                st.session_state.shopify_client.get_product_analytics,
                "detailed_metrics",
                ttl=600,  # 10 minutes cache for detailed analysis
            )
            
            if detailed_metrics:
                show_detailed_metrics(detailed_metrics)
            else:
                st.error("Failed to load detailed metrics")
                
        except Exception as e:
            st.error(f"❌ Detailed analysis failed: {str(e)}")
            st.info("💡 Switch to 'Quick (Estimates)' mode for better reliability")

def show_detailed_metrics(metrics):
    """Display comprehensive detailed metrics."""
    st.success("✅ Detailed analysis complete!")
    
    # Comprehensive metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Products", f"{metrics['total_products']:,}")
        st.metric("Variants", f"{metrics['total_variants']:,}")
    
    with col2:
        st.metric("Total Inventory", f"{metrics['total_inventory']:,}")
        st.metric("Total Value", f"${metrics['total_value']:,.2f}")
    
    with col3:
        st.metric("Low Stock", f"{metrics['low_stock_count']:,}")
        st.metric("Out of Stock", f"{metrics['out_of_stock_count']:,}")
    
    with col4:
        st.metric("Well Stocked", f"{metrics['well_stocked_count']:,}")
        avg_value = metrics['total_value'] / metrics['total_variants'] if metrics['total_variants'] > 0 else 0
        st.metric("Avg Value/Item", f"${avg_value:.2f}")
    
    # Product type breakdown
    if metrics.get('product_types'):
        st.markdown("---")
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("📊 Product Types")
            types_df = pd.DataFrame(list(metrics['product_types'].items()), columns=['Type', 'Count'])
            st.dataframe(types_df.head(10), use_container_width=True, hide_index=True)
        
        with col2:
            st.subheader("🏢 Top Vendors")
            vendors_df = pd.DataFrame(list(metrics['vendors'].items()), columns=['Vendor', 'Count'])
            st.dataframe(vendors_df.head(10), use_container_width=True, hide_index=True)

def show_smart_critical_alerts():
    """Show critical alerts with smart loading."""
    st.subheader("⚠️ Critical Stock Alerts")
    
    # Load alerts with limits
    with st.spinner("Loading critical alerts (limited to 100 items each)..."):
        try:
            # Limit critical alerts to prevent overload
            low_stock_items = st.session_state.shopify_client.get_low_stock_products(threshold=5)[:100]
            out_of_stock_items = st.session_state.shopify_client.get_out_of_stock_products()[:100]
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.write(f"**⚠️ Low Stock Items** (showing first 100)")
                if low_stock_items:
                    df_low = pd.DataFrame(low_stock_items)
                    st.dataframe(
                        df_low[['title', 'sku', 'inventory_quantity', 'price']].head(50), 
                        use_container_width=True, hide_index=True
                    )
                    if len(low_stock_items) == 100:
                        st.info("💡 Showing first 100 items. Use 'Browse Products' with filters to see more.")
                else:
                    st.success("✅ No low stock items!")
            
            with col2:
                st.write(f"**🚫 Out of Stock Items** (showing first 100)")
                if out_of_stock_items:
                    df_out = pd.DataFrame(out_of_stock_items)
                    st.dataframe(
                        df_out[['title', 'sku', 'price']].head(50), 
                        use_container_width=True, hide_index=True
                    )
                    if len(out_of_stock_items) == 100:
                        st.info("💡 Showing first 100 items. Use 'Browse Products' with filters to see more.")
                else:
                    st.success("✅ No out of stock items!")
        
        except Exception as e:
            st.error(f"❌ Error loading alerts: {str(e)}")
            st.info("💡 Try using the 'Browse Products' tab with stock filters instead.")

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
    """Browse and search products with smart loading for large catalogs."""
    st.header("🔍 Browse Products")
    
    # Performance warning for large catalogs
    try:
        product_count = cache_manager.cached_call(
            st.session_state.shopify_client.get_products_count,
            "product_count",
            ttl=300
        )
        
        if product_count > 10000:
            st.warning(f"⚠️ **Large Catalog Detected**: {product_count:,} products. Use filters to narrow results for better performance.")
        elif product_count > 1000:
            st.info(f"📊 **Medium Catalog**: {product_count:,} products. Pagination and filters recommended.")
        
    except:
        product_count = 0
    
    # Advanced search and filter controls
    with st.expander("🔍 **Search & Filter Controls**", expanded=True):
        col1, col2, col3 = st.columns(3)
        
        with col1:
            search_term = st.text_input(
                "🔍 Search Products", 
                placeholder="Search by title, SKU, or product type...",
                key="product_search",
                help="Use specific terms for better performance"
            )
        
        with col2:
            stock_filter = st.selectbox(
                "📦 Stock Status",
                ["All", "In Stock", "Low Stock (≤5)", "Out of Stock"],
                key="stock_filter",
                help="Filter by stock level to reduce load"
            )
        
        with col3:
            product_type_filter = st.text_input(
                "🏷️ Product Type",
                placeholder="e.g., Electronics, Clothing...",
                key="product_type_filter",
                help="Filter by product type"
            )
    
    # Load controls
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.session_state.products_per_page = st.selectbox(
            "📄 Items per page",
            [10, 25, 50, 100, 250],
            index=1,  # Default to 25
            key="items_per_page",
            help="Larger pages = slower loading"
        )
    
    with col2:
        view_mode = st.selectbox(
            "👀 View Mode", 
            ["Table", "Cards"], 
            key="view_mode"
        )
    
    with col3:
        load_mode = st.selectbox(
            "⚡ Load Mode",
            ["Smart (Filtered)", "Force Load All"],
            help="Smart mode applies filters server-side"
        )
    
    with col4:
        if st.button("🔄 Reset Filters"):
            st.session_state.current_page = 1
            for key in ['product_search', 'stock_filter', 'product_type_filter']:
                if key in st.session_state:
                    del st.session_state[key]
            st.rerun()
    
    # Search button and pagination reset
    col1, col2 = st.columns([1, 3])
    with col1:
        search_clicked = st.button("🔍 Search", type="primary")
    
    with col2:
        # Show current filters
        active_filters = []
        if search_term:
            active_filters.append(f"Search: '{search_term}'")
        if stock_filter != "All":
            active_filters.append(f"Stock: {stock_filter}")
        if product_type_filter:
            active_filters.append(f"Type: '{product_type_filter}'")
        
        if active_filters:
            st.caption(f"**Active filters**: {' | '.join(active_filters)}")
        else:
            st.caption("**No filters applied** - showing all products")
    
    # Reset pagination when filters change
    current_filters = (search_term, stock_filter, product_type_filter)
    if search_clicked or 'last_search' not in st.session_state or st.session_state.get('last_search') != current_filters:
        st.session_state.current_page = 1
        st.session_state.last_search = current_filters
    
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