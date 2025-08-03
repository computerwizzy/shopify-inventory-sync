import streamlit as st
import pandas as pd
import numpy as np
from io import StringIO
import os
from dotenv import load_dotenv
import sys
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src.file_processor import FileProcessor
from src.column_mapper import ColumnMapper
from src.sku_matcher import SKUMatcher
from src.shopify_client import ShopifyClient
from utils.config import Config

# Load environment variables
load_dotenv()

# Configure page
st.set_page_config(
    page_title="Shopify Inventory Manager",
    page_icon="🏪",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Initialize session state
if 'step' not in st.session_state:
    st.session_state.step = 1
if 'uploaded_data' not in st.session_state:
    st.session_state.uploaded_data = None
if 'column_mapping' not in st.session_state:
    st.session_state.column_mapping = {}
if 'matched_data' not in st.session_state:
    st.session_state.matched_data = None
if 'shopify_client' not in st.session_state:
    try:
        # Initialize config first to check what's available
        config = Config()
        
        # Debug information
        if config.debug_mode:
            st.write("Debug: Config validation")
            st.write(f"Store URL: {config.shopify_store_url}")
            st.write(f"Has Access Token: {'Yes' if config.shopify_access_token else 'No'}")
            st.write(f"API Version: {config.shopify_api_version}")
        
        # Check if configuration is missing
        if not config.validate_shopify_config():
            missing = config.get_missing_config()
            st.error(f"⚠️ **Shopify Configuration Missing**")
            st.error(f"Missing required settings: {', '.join(missing)}")
            
            st.markdown("### 🔧 How to Fix:")
            st.markdown("**For Streamlit Cloud:**")
            st.markdown("1. Go to your app settings")
            st.markdown("2. Add these secrets:")
            st.code("""[shopify]
SHOP_NAME = "your-shop-name"
ACCESS_TOKEN = "shpat_your_access_token"
API_VERSION = "2025-01" """)
            
            st.markdown("**For Local Development:**")
            st.markdown("Create a `.env` file with:")
            st.code(config.create_env_template())
            st.stop()
        
        st.session_state.shopify_client = ShopifyClient()
        
    except Exception as e:
        st.error(f"Failed to initialize Shopify Client: {e}")
        
        # Additional debug info
        try:
            config = Config()
            st.write("**Debug Information:**")
            st.write(config)
        except Exception as debug_e:
            st.write(f"Config debug failed: {debug_e}")
        
        st.stop()

def main():
    # Header with store info
    try:
        shop_info = st.session_state.shopify_client.get_shop_info()
        store_name = shop_info.get('name', 'Your Store')
    except:
        store_name = 'Your Store'
    
    st.title(f"🏪 {store_name} - Inventory Manager")
    st.markdown("**Complete inventory management solution for Shopify**")
    st.markdown("---")
    
    # Welcome section
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.subheader("👋 Welcome to your Inventory Management Hub")
        st.markdown("""
        This powerful tool helps you manage your Shopify store's inventory with:
        - **Real-time product management** 📦
        - **Automated inventory synchronization** 🔄
        - **Bulk operations and updates** ⚡
        - **Analytics and reporting** 📊
        - **Low stock alerts** ⚠️
        """)
    
    with col2:
        # Quick stats if available
        try:
            products = st.session_state.shopify_client.get_all_products()
            if products:
                total_products = len(products)
                total_variants = sum(len(p.get('variants', [])) for p in products)
                
                st.metric("Products", total_products)
                st.metric("Variants", total_variants)
        except:
            st.info("Loading store data...")
    
    st.markdown("---")
    
    # Quick action cards
    st.subheader("🚀 Quick Actions")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        with st.container():
            st.markdown("### 📦 Product Management")
            st.markdown("Browse, search, and edit your products and inventory")
            if st.button("🔍 Browse Products", type="primary", use_container_width=True):
                st.switch_page("pages/4_📦_Product_Management.py")
    
    with col2:
        with st.container():
            st.markdown("### 🔗 Feed Sources")
            st.markdown("Configure automated data feeds from suppliers")
            if st.button("⚙️ Configure Feeds", type="secondary", use_container_width=True):
                st.switch_page("pages/1_🔗_Feed_Sources.py")
    
    with col3:
        with st.container():
            st.markdown("### ⏰ Scheduled Sync")
            st.markdown("Set up automated inventory synchronization")
            if st.button("📅 Schedule Sync", type="secondary", use_container_width=True):
                st.switch_page("pages/2_⏰_Scheduled_Sync.py")
    
    with col4:
        with st.container():
            st.markdown("### 📊 API Monitor")
            st.markdown("Monitor API usage and performance")
            if st.button("📈 View Monitor", type="secondary", use_container_width=True):
                st.switch_page("pages/3_📊_API_Monitor.py")
    
    st.markdown("---")
    
    # Manual upload section for quick sync
    st.subheader("📤 Quick File Upload & Sync")
    st.markdown("Upload an inventory file for immediate synchronization")
    
    mode = st.radio(
        "Choose operation mode:",
        ["🔄 Quick Sync (Guided)", "⚡ Expert Mode (Direct Upload)"],
        horizontal=True
    )
    
    if mode == "🔄 Quick Sync (Guided)":
        show_guided_sync()
    else:
        show_expert_sync()

def show_guided_sync():
    """Show guided sync interface."""
    # Sidebar navigation
    st.sidebar.title("🔄 Quick Sync Steps")
    steps = [
        "1. Upload File",
        "2. Map Columns", 
        "3. Match SKUs",
        "4. Sync Inventory"
    ]
    
    current_step = st.sidebar.radio("Current Step", steps, index=st.session_state.step-1)
    st.session_state.step = steps.index(current_step) + 1
    
    # Progress bar
    progress = st.session_state.step / len(steps)
    st.progress(progress, text=f"Step {st.session_state.step} of {len(steps)}")
    
    # Main content based on step
    if st.session_state.step == 1:
        upload_file_step()
    elif st.session_state.step == 2:
        map_columns_step()
    elif st.session_state.step == 3:
        match_skus_step()
    elif st.session_state.step == 4:
        sync_inventory_step()

def show_expert_sync():
    """Show expert mode for quick uploads."""
    st.info("💡 **Expert Mode**: For experienced users who want to quickly upload and sync without guided steps.")
    
    uploaded_file = st.file_uploader(
        "Upload Inventory File", 
        type=['csv', 'xlsx', 'xls'],
        help="File should contain SKU and Quantity columns"
    )
    
    if uploaded_file is not None:
        try:
            processor = FileProcessor()
            df = processor.process_file(uploaded_file)
            
            st.success(f"✅ File loaded: {len(df)} rows, {len(df.columns)} columns")
            
            # Quick column mapping
            col1, col2 = st.columns(2)
            with col1:
                sku_col = st.selectbox("SKU Column", df.columns)
            with col2:
                qty_col = st.selectbox("Quantity Column", df.columns)
            
            if st.button("⚡ Quick Sync", type="primary"):
                with st.spinner("Performing quick sync..."):
                    # Quick mapping and sync
                    mapping = {"SKU": sku_col, "Quantity": qty_col}
                    mapper = ColumnMapper(df.columns.tolist())
                    mapped_df = mapper.get_mapped_data(df, mapping)
                    
                    if not mapped_df.empty:
                        # Quick SKU matching and sync
                        matcher = SKUMatcher(st.session_state.shopify_client)
                        matches = matcher.find_sku_matches(mapped_df['SKU'].tolist())
                        
                        sync_data = []
                        for _, row in mapped_df.iterrows():
                            if row['SKU'] in matches:
                                sync_data.append({
                                    'variant_id': matches[row['SKU']]['variant_id'],
                                    'new_quantity': int(row['Quantity'])
                                })
                        
                        if sync_data:
                            results = st.session_state.shopify_client.bulk_update_inventory(sync_data)
                            success_count = sum(1 for r in results if r.get('success', False))
                            st.success(f"✅ Quick sync complete! Updated {success_count} of {len(sync_data)} items.")
                        else:
                            st.warning("⚠️ No matching SKUs found in your store.")
                    else:
                        st.error("❌ Failed to map columns.")
        except Exception as e:
            st.error(f"❌ Error processing file: {str(e)}")

def upload_file_step():
    st.header("📁 Step 1: Upload Your Inventory File")
    
    st.info("Upload a CSV or Excel file containing your inventory data with SKUs and quantities.")
    
    uploaded_file = st.file_uploader(
        "Choose a file", 
        type=['csv', 'xlsx', 'xls'],
        help="Supported formats: CSV, Excel (.xlsx, .xls)"
    )
    
    if uploaded_file is not None:
        try:
            # Process the uploaded file
            processor = FileProcessor()
            df = processor.process_file(uploaded_file)
            
            st.success(f"File uploaded successfully! Found {len(df)} rows.")
            
            # Display preview
            st.subheader("📊 Data Preview")
            st.dataframe(df.head(10), use_container_width=True)
            
            # Display file info
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Total Rows", len(df))
            with col2:
                st.metric("Total Columns", len(df.columns))
            with col3:
                st.metric("File Size", f"{uploaded_file.size / 1024:.1f} KB")
            
            # Store data in session state
            st.session_state.uploaded_data = df
            
            # Next step button
            if st.button("➡️ Proceed to Column Mapping", type="primary"):
                st.session_state.step = 2
                st.rerun()
                
        except Exception as e:
            st.error(f"Error processing file: {str(e)}")
            st.info("Please ensure your file is a valid CSV or Excel file.")

def map_columns_step():
    st.header("🔗 Step 2: Map Your Columns")
    
    if st.session_state.uploaded_data is None:
        st.warning("Please upload a file first.")
        return
    
    df = st.session_state.uploaded_data
    st.info("Map your file columns to the required fields for inventory sync.")
    
    # Column mapping interface
    mapper = ColumnMapper(df.columns.tolist())
    mapping = mapper.create_mapping_interface()
    
    if mapping:
        st.session_state.column_mapping = mapping
        
        # Show mapping preview
        st.subheader("📋 Mapping Preview")
        mapping_df = pd.DataFrame([
            {"Required Field": k, "Your Column": v} 
            for k, v in mapping.items() if v != "-- Select Column --"
        ])
        
        if not mapping_df.empty:
            st.dataframe(mapping_df, use_container_width=True)
            
            # Validate mapping
            required_fields = ["SKU", "Quantity"]
            mapped_required = [field for field in required_fields if mapping.get(field) != "-- Select Column --"]
            
            if len(mapped_required) == len(required_fields):
                st.success("✅ All required fields are mapped!")
                
                # Next step button
                if st.button("➡️ Proceed to SKU Matching", type="primary"):
                    st.session_state.step = 3
                    st.rerun()
            else:
                missing = [field for field in required_fields if field not in mapped_required]
                st.warning(f"❌ Please map these required fields: {', '.join(missing)}")
        else:
            st.warning("Please map at least the required fields (SKU and Quantity).")

def match_skus_step():
    st.header("🔍 Step 3: Match SKUs with Shopify")
    
    if st.session_state.uploaded_data is None or not st.session_state.column_mapping:
        st.warning("Please complete the previous steps first.")
        return
    
    # Check Shopify configuration
    config = Config()
    if not config.validate_shopify_config():
        st.error("❌ Shopify configuration is missing. Please check your .env file.")
        st.info("Required: SHOPIFY_STORE_URL, SHOPIFY_ACCESS_TOKEN")
        return
    
    st.info("Matching your SKUs with products in your Shopify store...")
    
    df = st.session_state.uploaded_data
    mapping = st.session_state.column_mapping
    
    # Initialize Shopify client and SKU matcher
    try:
        shopify_client = ShopifyClient()
        sku_matcher = SKUMatcher(shopify_client)
        
        with st.spinner("Fetching products from Shopify..."):
            shopify_products = shopify_client.get_all_products()
        
        st.success(f"✅ Found {len(shopify_products)} products in Shopify")
        
        # Perform SKU matching
        with st.spinner("Matching SKUs..."):
            matched_data = sku_matcher.match_skus(df, mapping, shopify_products)
        
        st.session_state.matched_data = matched_data
        
        # Display matching results
        st.subheader("📊 Matching Results")
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total SKUs", len(matched_data))
        with col2:
            exact_matches = len([m for m in matched_data if m['match_type'] == 'exact'])
            st.metric("Exact Matches", exact_matches)
        with col3:
            fuzzy_matches = len([m for m in matched_data if m['match_type'] == 'fuzzy'])
            st.metric("Fuzzy Matches", fuzzy_matches)
        
        # Show detailed results
        results_df = pd.DataFrame([{
            "Your SKU": m['file_sku'],
            "Shopify SKU": m['shopify_sku'],
            "Product Title": m['product_title'],
            "Match Type": m['match_type'],
            "Confidence": f"{m['confidence']:.1%}" if m['confidence'] else "N/A",
            "Current Qty": m['current_quantity'],
            "New Qty": m['new_quantity']
        } for m in matched_data])
        
        st.dataframe(results_df, use_container_width=True)
        
        # Next step button
        if st.button("➡️ Proceed to Inventory Sync", type="primary"):
            st.session_state.step = 4
            st.rerun()
            
    except Exception as e:
        st.error(f"Error connecting to Shopify: {str(e)}")
        st.info("Please check your Shopify credentials and store URL.")

def sync_inventory_step():
    st.header("🔄 Step 4: Sync Inventory")
    
    if not st.session_state.matched_data:
        st.warning("Please complete SKU matching first.")
        return
    
    matched_data = st.session_state.matched_data
    
    st.info("Review the changes and sync your inventory with Shopify.")
    
    # Filter options
    st.subheader("🔧 Sync Options")
    
    col1, col2 = st.columns(2)
    with col1:
        sync_exact_only = st.checkbox("Sync exact matches only", value=True)
        sync_zero_qty = st.checkbox("Include zero quantity updates", value=False)
    
    with col2:
        dry_run = st.checkbox("Dry run (preview only)", value=True)
        batch_size = st.slider("Batch size", min_value=1, max_value=50, value=10)
    
    # Filter data based on options
    filtered_data = matched_data.copy()
    if sync_exact_only:
        filtered_data = [m for m in filtered_data if m['match_type'] == 'exact']
    if not sync_zero_qty:
        filtered_data = [m for m in filtered_data if m['new_quantity'] > 0]
    
    st.info(f"Ready to sync {len(filtered_data)} inventory items.")
    
    # Show what will be synced
    if filtered_data:
        sync_df = pd.DataFrame([{
            "SKU": m['shopify_sku'],
            "Product": m['product_title'][:50] + "..." if len(m['product_title']) > 50 else m['product_title'],
            "Current": m['current_quantity'],
            "New": m['new_quantity'],
            "Change": f"{m['new_quantity'] - m['current_quantity']:+d}"
        } for m in filtered_data])
        
        st.dataframe(sync_df, use_container_width=True)
        
        # Sync button
        if st.button("🚀 Start Inventory Sync", type="primary"):
            try:
                shopify_client = ShopifyClient()
                
                progress_bar = st.progress(0)
                status_text = st.empty()
                results_container = st.container()
                
                success_count = 0
                error_count = 0
                
                for i, item in enumerate(filtered_data):
                    status_text.text(f"Syncing {i+1}/{len(filtered_data)}: {item['shopify_sku']}")
                    
                    if not dry_run:
                        try:
                            shopify_client.update_inventory(
                                item['variant_id'], 
                                item['new_quantity']
                            )
                            success_count += 1
                        except Exception as e:
                            error_count += 1
                            results_container.error(f"Failed to sync {item['shopify_sku']}: {str(e)}")
                    else:
                        success_count += 1  # Simulate success for dry run
                    
                    progress_bar.progress((i + 1) / len(filtered_data))
                
                # Show final results
                status_text.text("Sync completed!")
                
                if dry_run:
                    st.success("✅ Dry run completed successfully!")
                    st.info(f"Would have synced {success_count} items.")
                else:
                    st.success(f"✅ Inventory sync completed!")
                    st.info(f"Successfully synced: {success_count}, Errors: {error_count}")
                
                # Reset button
                if st.button("🔄 Start New Sync"):
                    for key in st.session_state.keys():
                        del st.session_state[key]
                    st.rerun()
                    
            except Exception as e:
                st.error(f"Error during sync: {str(e)}")
    else:
        st.warning("No items match your sync criteria.")

if __name__ == "__main__":
    main()