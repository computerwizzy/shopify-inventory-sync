import streamlit as st
import pandas as pd
import json
import os
import sys
from typing import Dict
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.feed_sources import FeedSourceManager, FeedConfigManager
from src.file_processor import FileProcessor

st.set_page_config(
    page_title="Feed Sources",
    page_icon="🔗",
    layout="wide"
)

# Initialize managers
if 'feed_manager' not in st.session_state:
    st.session_state.feed_manager = FeedSourceManager()

if 'config_manager' not in st.session_state:
    st.session_state.config_manager = FeedConfigManager()

def main():
    st.title("🔗 Feed Sources Configuration")
    st.markdown("Configure external data sources for automated inventory synchronization.")
    
    # Tabs for different functions
    tab1, tab2, tab3, tab4 = st.tabs(["📝 Configure Feeds", "📋 Manage Feeds", "🧪 Test Connections", "🔍 Diagnose Issues"])
    
    with tab1:
        configure_feeds_tab()
    
    with tab2:
        manage_feeds_tab()
    
    with tab3:
        test_connections_tab()
    
    with tab4:
        diagnose_issues_tab()

def configure_feeds_tab():
    st.header("📝 Configure New Feed Source")
    
    # Feed source type selection
    feed_type = st.selectbox(
        "Select Feed Source Type",
        ["FTP", "SFTP", "URL/API", "Google Sheets"],
        help="Choose the type of data source you want to configure"
    )
    
    # Configuration name
    config_name = st.text_input(
        "Configuration Name",
        placeholder="e.g., 'supplier-inventory-feed'",
        help="Unique name for this feed configuration"
    )
    
    if feed_type == "FTP":
        configure_ftp_feed(config_name)
    elif feed_type == "SFTP":
        configure_sftp_feed(config_name)
    elif feed_type == "URL/API":
        configure_url_feed(config_name)
    elif feed_type == "Google Sheets":
        configure_google_sheets_feed(config_name)

def configure_ftp_feed(config_name: str):
    st.subheader("📁 FTP Configuration")
    
    col1, col2 = st.columns(2)
    
    with col1:
        host = st.text_input("FTP Host", placeholder="ftp.example.com")
        username = st.text_input("Username")
        port = st.number_input("Port", value=21, min_value=1, max_value=65535)
    
    with col2:
        password = st.text_input("Password", type="password")
        file_path = st.text_input("File Path", placeholder="/path/to/inventory.csv")
        
    st.info("💡 **Tip**: Make sure your FTP server allows passive mode connections.")
    
    if st.button("💾 Save FTP Configuration", type="primary"):
        if all([config_name, host, username, password, file_path]):
            config = {
                'type': 'ftp',
                'host': host,
                'username': username,
                'password': password,
                'file_path': file_path,
                'port': port
            }
            st.session_state.config_manager.add_config(config_name, config)
            st.success(f"✅ FTP configuration '{config_name}' saved successfully!")
            st.rerun()
        else:
            st.error("❌ Please fill in all required fields.")

def configure_sftp_feed(config_name: str):
    st.subheader("🔐 SFTP Configuration")
    
    col1, col2 = st.columns(2)
    
    with col1:
        host = st.text_input("SFTP Host", placeholder="sftp.example.com")
        username = st.text_input("Username")
        port = st.number_input("Port", value=22, min_value=1, max_value=65535)
        
    with col2:
        auth_method = st.radio("Authentication Method", ["Password", "Private Key"])
        
        if auth_method == "Password":
            password = st.text_input("Password", type="password")
            private_key = None
        else:
            password = None
            private_key = st.text_input("Private Key Path", placeholder="/path/to/private_key")
    
    file_path = st.text_input("File Path", placeholder="/path/to/inventory.csv")
    
    st.info("🔒 **Security**: Private key authentication is recommended for production use.")
    
    if st.button("💾 Save SFTP Configuration", type="primary"):
        if all([config_name, host, username, file_path]) and (password or private_key):
            config = {
                'type': 'sftp',
                'host': host,
                'username': username,
                'password': password,
                'file_path': file_path,
                'port': port,
                'private_key': private_key
            }
            st.session_state.config_manager.add_config(config_name, config)
            st.success(f"✅ SFTP configuration '{config_name}' saved successfully!")
            st.rerun()
        else:
            st.error("❌ Please fill in all required fields.")

def configure_url_feed(config_name: str):
    st.subheader("🌐 URL/API Configuration")
    
    url = st.text_input("File URL", placeholder="https://example.com/inventory.csv")
    
    # Authentication options
    auth_type = st.selectbox("Authentication", ["None", "Basic Auth", "API Key Header"])
    
    auth = None
    headers = {}
    
    if auth_type == "Basic Auth":
        col1, col2 = st.columns(2)
        with col1:
            auth_username = st.text_input("Auth Username")
        with col2:
            auth_password = st.text_input("Auth Password", type="password")
        
        if auth_username and auth_password:
            auth = (auth_username, auth_password)
    
    elif auth_type == "API Key Header":
        col1, col2 = st.columns(2)
        with col1:
            header_name = st.text_input("Header Name", placeholder="X-API-Key")
        with col2:
            header_value = st.text_input("Header Value", type="password")
        
        if header_name and header_value:
            headers[header_name] = header_value
    
    # Additional headers
    with st.expander("➕ Additional Headers"):
        st.write("Add custom HTTP headers if needed:")
        custom_headers = st.text_area(
            "Headers (JSON format)",
            placeholder='{"Content-Type": "application/json", "User-Agent": "MyApp/1.0"}',
            help="Enter headers as a JSON object"
        )
        
        if custom_headers:
            try:
                additional_headers = json.loads(custom_headers)
                headers.update(additional_headers)
            except json.JSONDecodeError:
                st.error("❌ Invalid JSON format for headers")
    
    timeout = st.slider("Request Timeout (seconds)", 5, 120, 30)
    
    # Column mapping section
    st.subheader("🗂️ Column Mapping (Optional)")
    
    column_mapping = {}
    if st.button("📖 Read Feed Headers", help="Test connection and read available columns"):
        if url:
            try:
                with st.spinner("Reading feed headers..."):
                    temp_config = {
                        'url': url,
                        'headers': headers if headers else None,
                        'auth': auth,
                        'timeout': timeout
                    }
                    available_columns = st.session_state.feed_manager.get_feed_headers('url', temp_config)
                
                st.success(f"✅ Found {len(available_columns)} columns in the feed")
                
                # Store in session state for mapping
                st.session_state[f'feed_columns_{config_name}'] = available_columns
                
            except Exception as e:
                st.error(f"❌ Failed to read headers: {str(e)}")
    
    # Show column mapping interface if headers are available
    if f'feed_columns_{config_name}' in st.session_state:
        available_columns = st.session_state[f'feed_columns_{config_name}']
        
        st.write("**Available columns in your feed:**")
        cols = st.columns(min(len(available_columns), 4))
        for i, col in enumerate(available_columns):
            with cols[i % 4]:
                st.write(f"• {col}")
        
        st.write("**Map to inventory fields:**")
        
        # Required fields
        col1, col2 = st.columns(2)
        with col1:
            sku_column = st.selectbox(
                "SKU Column", 
                ["-- Select Column --"] + available_columns,
                help="Column containing product SKUs"
            )
        with col2:
            quantity_column = st.selectbox(
                "Quantity Column", 
                ["-- Select Column --"] + available_columns,
                help="Column containing inventory quantities"
            )
        
        # Optional fields
        col3, col4 = st.columns(2)
        with col3:
            product_title_column = st.selectbox(
                "Product Title Column (Optional)", 
                ["-- Select Column --"] + available_columns,
                help="Column containing product names/titles"
            )
        with col4:
            price_column = st.selectbox(
                "Price Column (Optional)", 
                ["-- Select Column --"] + available_columns,
                help="Column containing product prices"
            )
        
        # Build column mapping
        if sku_column != "-- Select Column --":
            column_mapping["SKU"] = sku_column
        if quantity_column != "-- Select Column --":
            column_mapping["Quantity"] = quantity_column
        if product_title_column != "-- Select Column --":
            column_mapping["Product Title"] = product_title_column
        if price_column != "-- Select Column --":
            column_mapping["Price"] = price_column
        
        if column_mapping:
            st.write("**Current mapping:**")
            mapping_df = pd.DataFrame([
                {"Inventory Field": k, "Feed Column": v} 
                for k, v in column_mapping.items()
            ])
            st.dataframe(mapping_df, use_container_width=True)
        
        # Column selection for sync
        st.subheader("🎯 Column Selection for Sync")
        st.write("Select which columns to include in synchronization:")
        
        # Initialize session state for column selection if not exists
        session_key = f'selected_cols_{config_name}'
        if session_key not in st.session_state:
            st.session_state[session_key] = []
        
        selected_columns = []
        if available_columns:
            st.write("**Select columns to include in sync:**")
            
            # Create a multiselect for column selection
            default_selected = []
            for col_name in available_columns:
                if (col_name in column_mapping.values() or
                    any(keyword in col_name.lower() for keyword in ['sku', 'quantity', 'price', 'title', 'name', 'description'])):
                    default_selected.append(col_name)
            
            selected_columns = st.multiselect(
                "Choose columns:",
                options=available_columns,
                default=default_selected,
                key=f"multiselect_cols_{config_name}",
                help="Select which columns to include in the synchronization process"
            )
            
            if selected_columns:
                st.success(f"✅ Selected {len(selected_columns)} columns for sync: {', '.join(selected_columns[:5])}{'...' if len(selected_columns) > 5 else ''}")
            else:
                st.warning("⚠️ No columns selected. All columns will be included by default.")

    if st.button("💾 Save URL Configuration", type="primary"):
        if config_name and url:
            config = {
                'type': 'url',
                'url': url,
                'headers': headers if headers else None,
                'auth': auth,
                'timeout': timeout,
                'column_mapping': column_mapping if column_mapping else None,
                'selected_columns': selected_columns if selected_columns else None
            }
            st.session_state.config_manager.add_config(config_name, config)
            st.success(f"✅ URL configuration '{config_name}' saved successfully!")
            
            # Clear the stored columns and selections
            if f'feed_columns_{config_name}' in st.session_state:
                del st.session_state[f'feed_columns_{config_name}']
            if f'selected_cols_{config_name}' in st.session_state:
                del st.session_state[f'selected_cols_{config_name}']
            
            st.rerun()
        else:
            st.error("❌ Please provide configuration name and URL.")

def configure_google_sheets_feed(config_name: str):
    st.subheader("📊 Google Sheets Configuration")
    
    sheet_id = st.text_input(
        "Google Sheets ID",
        placeholder="1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms",
        help="Extract this from your Google Sheets URL"
    )
    
    worksheet_name = st.text_input(
        "Worksheet Name (optional)",
        placeholder="Sheet1",
        help="Leave empty to use the first sheet"
    )
    
    st.info("🔐 **Authentication**: You need to provide Google Service Account credentials.")
    
    auth_method = st.radio("Authentication Method", ["Upload JSON File", "Paste JSON Content"])
    
    credentials_path = None
    credentials_json = None
    
    if auth_method == "Upload JSON File":
        uploaded_file = st.file_uploader(
            "Upload Service Account JSON",
            type=['json'],
            help="Upload your Google Service Account credentials file"
        )
        
        if uploaded_file:
            # Save uploaded file temporarily
            credentials_path = f"temp_credentials_{config_name}.json"
            with open(credentials_path, 'wb') as f:
                f.write(uploaded_file.read())
    
    else:
        credentials_text = st.text_area(
            "Service Account JSON",
            placeholder='{"type": "service_account", "project_id": "...", ...}',
            height=200,
            help="Paste your Google Service Account JSON content"
        )
        
        if credentials_text:
            try:
                credentials_json = json.loads(credentials_text)
            except json.JSONDecodeError:
                st.error("❌ Invalid JSON format for credentials")
    
    # Instructions for setting up Google Sheets API
    with st.expander("📖 Google Sheets Setup Instructions"):
        st.markdown("""
        **To use Google Sheets integration:**
        
        1. **Create a Google Cloud Project**: Go to [Google Cloud Console](https://console.cloud.google.com/)
        2. **Enable Google Sheets API**: In your project, enable the Google Sheets API
        3. **Create Service Account**: 
           - Go to IAM & Admin > Service Accounts
           - Create a new service account
           - Download the JSON key file
        4. **Share your Sheet**: Share your Google Sheet with the service account email
        5. **Extract Sheet ID**: From your sheet URL: `docs.google.com/spreadsheets/d/[SHEET_ID]/edit`
        """)
    
    if st.button("💾 Save Google Sheets Configuration", type="primary"):
        if all([config_name, sheet_id]) and (credentials_path or credentials_json):
            config = {
                'type': 'google_sheets',
                'sheet_id': sheet_id,
                'worksheet_name': worksheet_name if worksheet_name else None,
                'credentials_path': credentials_path,
                'credentials_json': credentials_json
            }
            st.session_state.config_manager.add_config(config_name, config)
            st.success(f"✅ Google Sheets configuration '{config_name}' saved successfully!")
            st.rerun()
        else:
            st.error("❌ Please provide all required information.")

def manage_feeds_tab():
    st.header("📋 Manage Feed Configurations")
    
    # Check if we're editing a mapping
    if hasattr(st.session_state, 'editing_mapping') and st.session_state.editing_mapping:
        config_name = st.session_state.editing_mapping
        configs = st.session_state.config_manager.configs
        if config_name in configs:
            configure_column_mapping(config_name, configs[config_name])
            return
        else:
            # Config doesn't exist anymore, clear the editing state
            del st.session_state.editing_mapping
    
    configs = st.session_state.config_manager.configs
    
    if not configs:
        st.info("📭 No feed configurations found. Create one in the 'Configure Feeds' tab.")
        return
    
    # Display existing configurations
    for name, config in configs.items():
        with st.expander(f"📁 {name} ({config.get('type', 'Unknown').upper()})"):
            col1, col2, col3 = st.columns([2, 1, 1])
            
            with col1:
                st.write(f"**Type:** {config.get('type', 'Unknown')}")
                if config.get('type') == 'ftp':
                    st.write(f"**Host:** {config.get('host')}")
                    st.write(f"**File Path:** {config.get('file_path')}")
                elif config.get('type') == 'sftp':
                    st.write(f"**Host:** {config.get('host')}")
                    st.write(f"**File Path:** {config.get('file_path')}")
                elif config.get('type') == 'url':
                    st.write(f"**URL:** {config.get('url')}")
                elif config.get('type') == 'google_sheets':
                    st.write(f"**Sheet ID:** {config.get('sheet_id')}")
                    st.write(f"**Worksheet:** {config.get('worksheet_name', 'Default')}")
                
                st.write(f"**Created:** {config.get('created_at', 'Unknown')}")
                
                # Show column mapping if available
                if config.get('column_mapping'):
                    st.write("**Column Mapping:**")
                    mapping = config['column_mapping']
                    for field, column in mapping.items():
                        st.write(f"  • {field} → {column}")
                else:
                    st.write("**Column Mapping:** Not configured")
                
                # Show selected columns if available
                if config.get('selected_columns'):
                    st.write(f"**Selected Columns ({len(config['selected_columns'])}):** {', '.join(config['selected_columns'][:3])}{'...' if len(config['selected_columns']) > 3 else ''}")
                else:
                    st.write("**Column Selection:** All columns (not configured)")
            
            with col2:
                if st.button(f"🧪 Test", key=f"test_{name}"):
                    test_feed_connection(name, config)
                
                # Add button to edit column mapping
                if st.button(f"🗂️ Mapping", key=f"mapping_{name}", help="Configure column mapping"):
                    st.session_state.editing_mapping = name
                    st.rerun()
            
            with col3:
                if st.button(f"🗑️ Delete", key=f"delete_{name}", type="secondary"):
                    st.session_state.config_manager.delete_config(name)
                    st.success(f"Deleted configuration '{name}'")
                    st.rerun()

def configure_column_mapping(name: str, config: Dict):
    """Configure column mapping for an existing feed."""
    st.subheader(f"🗂️ Configure Column Mapping for '{name}'")
    
    try:
        # Read headers from the feed
        with st.spinner("Reading feed headers..."):
            available_columns = st.session_state.feed_manager.get_feed_headers(config['type'], config)
        
        st.success(f"✅ Found {len(available_columns)} columns in the feed")
        
        # Show available columns
        st.write("**Available columns in your feed:**")
        cols = st.columns(min(len(available_columns), 4))
        for i, col in enumerate(available_columns):
            with cols[i % 4]:
                st.write(f"• {col}")
        
        st.write("**Map to inventory fields:**")
        
        # Get existing mappings
        existing_mapping = config.get('column_mapping', {})
        
        # Required fields
        col1, col2 = st.columns(2)
        with col1:
            sku_column = st.selectbox(
                "SKU Column", 
                ["-- Select Column --"] + available_columns,
                index=available_columns.index(existing_mapping.get("SKU")) + 1 if existing_mapping.get("SKU") in available_columns else 0,
                help="Column containing product SKUs"
            )
        with col2:
            quantity_column = st.selectbox(
                "Quantity Column", 
                ["-- Select Column --"] + available_columns,
                index=available_columns.index(existing_mapping.get("Quantity")) + 1 if existing_mapping.get("Quantity") in available_columns else 0,
                help="Column containing inventory quantities"
            )
        
        # Optional fields
        col3, col4 = st.columns(2)
        with col3:
            product_title_column = st.selectbox(
                "Product Title Column (Optional)", 
                ["-- Select Column --"] + available_columns,
                index=available_columns.index(existing_mapping.get("Product Title")) + 1 if existing_mapping.get("Product Title") in available_columns else 0,
                help="Column containing product names/titles"
            )
        with col4:
            price_column = st.selectbox(
                "Price Column (Optional)", 
                ["-- Select Column --"] + available_columns,
                index=available_columns.index(existing_mapping.get("Price")) + 1 if existing_mapping.get("Price") in available_columns else 0,
                help="Column containing product prices"
            )
        
        # Build new mapping
        new_mapping = {}
        if sku_column != "-- Select Column --":
            new_mapping["SKU"] = sku_column
        if quantity_column != "-- Select Column --":
            new_mapping["Quantity"] = quantity_column
        if product_title_column != "-- Select Column --":
            new_mapping["Product Title"] = product_title_column
        if price_column != "-- Select Column --":
            new_mapping["Price"] = price_column
        
        if new_mapping:
            st.write("**Current mapping:**")
            mapping_df = pd.DataFrame([
                {"Inventory Field": k, "Feed Column": v} 
                for k, v in new_mapping.items()
            ])
            st.dataframe(mapping_df, use_container_width=True)
        
        # Column selection for sync
        st.subheader("🎯 Column Selection for Sync")
        st.write("Select which columns to include in synchronization:")
        
        existing_selection = config.get('selected_columns', [])
        
        # Determine default selected columns
        default_selected = []
        for col_name in available_columns:
            if (col_name in existing_selection or
                col_name in new_mapping.values() or
                any(keyword in col_name.lower() for keyword in ['sku', 'quantity', 'price', 'title', 'name', 'description'])):
                default_selected.append(col_name)
        
        # Use multiselect for better UX
        selected_columns = st.multiselect(
            "Choose columns to include in sync:",
            options=available_columns,
            default=default_selected,
            key=f"edit_multiselect_cols_{name}",
            help="Select which columns to include in the synchronization process"
        )
        
        if selected_columns:
            st.success(f"✅ Selected {len(selected_columns)} columns for sync")
        else:
            st.warning("⚠️ No columns selected. All columns will be included by default.")
        
        # Save mapping
        col1, col2 = st.columns(2)
        with col1:
            if st.button("💾 Save Column Mapping", type="primary"):
                # Update the configuration
                config['column_mapping'] = new_mapping
                config['selected_columns'] = selected_columns if selected_columns else None
                st.session_state.config_manager.add_config(name, config)
                st.success(f"✅ Column mapping and selection for '{name}' saved successfully!")
                if 'editing_mapping' in st.session_state:
                    del st.session_state.editing_mapping
                st.rerun()
        
        with col2:
            if st.button("❌ Cancel"):
                if 'editing_mapping' in st.session_state:
                    del st.session_state.editing_mapping
                st.rerun()
                
    except Exception as e:
        st.error(f"❌ Failed to read feed headers: {str(e)}")
        st.info("Make sure the feed connection is working before configuring column mapping.")

def test_connections_tab():
    st.header("🧪 Test Feed Connections")
    
    configs = st.session_state.config_manager.configs
    
    if not configs:
        st.info("📭 No feed configurations to test. Create one first.")
        return
    
    selected_config = st.selectbox(
        "Select Configuration to Test",
        list(configs.keys())
    )
    
    if selected_config:
        config = configs[selected_config]
        
        col1, col2 = st.columns([1, 1])
        
        with col1:
            if st.button("🔗 Test Connection", type="primary"):
                test_feed_connection(selected_config, config)
        
        with col2:
            if st.button("📊 Download Sample Data", type="secondary"):
                download_sample_data(selected_config, config)

def test_feed_connection(name: str, config: Dict):
    """Test connection to a feed source."""
    feed_type = config.get('type')
    
    with st.spinner(f"Testing connection to {name}..."):
        try:
            if feed_type == 'ftp':
                success = st.session_state.feed_manager.test_ftp_connection(
                    host=config['host'],
                    username=config['username'],
                    password=config['password'],
                    port=config.get('port', 21)
                )
            elif feed_type == 'sftp':
                success = st.session_state.feed_manager.test_sftp_connection(
                    host=config['host'],
                    username=config['username'],
                    password=config.get('password'),
                    port=config.get('port', 22),
                    private_key=config.get('private_key')
                )
            elif feed_type == 'url':
                success = st.session_state.feed_manager.test_url_connection(
                    url=config['url'],
                    headers=config.get('headers'),
                    auth=tuple(config['auth']) if config.get('auth') else None
                )
            elif feed_type == 'google_sheets':
                success = st.session_state.feed_manager.test_google_sheets_connection(
                    sheet_id=config['sheet_id'],
                    credentials_path=config.get('credentials_path'),
                    credentials_json=config.get('credentials_json')
                )
            else:
                success = False
            
            if success:
                st.success(f"✅ Connection to '{name}' successful!")
            else:
                st.error(f"❌ Connection to '{name}' failed!")
                
        except Exception as e:
            st.error(f"❌ Connection test failed: {str(e)}")

def download_sample_data(name: str, config: Dict):
    """Download and preview sample data from feed source."""
    feed_type = config.get('type')
    
    with st.spinner(f"Downloading sample data from {name}..."):
        try:
            if feed_type == 'ftp':
                file_path = st.session_state.feed_manager.download_from_ftp(
                    host=config['host'],
                    username=config['username'],
                    password=config['password'],
                    file_path=config['file_path'],
                    port=config.get('port', 21)
                )
                processor = FileProcessor()
                df = processor.process_file_by_path(file_path)
                
            elif feed_type == 'sftp':
                file_path = st.session_state.feed_manager.download_from_sftp(
                    host=config['host'],
                    username=config['username'],
                    password=config.get('password'),
                    file_path=config['file_path'],
                    port=config.get('port', 22),
                    private_key=config.get('private_key')
                )
                processor = FileProcessor()
                df = processor.process_file_by_path(file_path)
                
            elif feed_type == 'url':
                file_path = st.session_state.feed_manager.download_from_url(
                    url=config['url'],
                    headers=config.get('headers'),
                    auth=tuple(config['auth']) if config.get('auth') else None
                )
                processor = FileProcessor()
                df = processor.process_file_by_path(file_path)
                
            elif feed_type == 'google_sheets':
                df = st.session_state.feed_manager.download_from_google_sheets(
                    sheet_id=config['sheet_id'],
                    worksheet_name=config.get('worksheet_name'),
                    credentials_path=config.get('credentials_path'),
                    credentials_json=config.get('credentials_json')
                )
            else:
                raise ValueError(f"Unsupported feed type: {feed_type}")
            
            if not df.empty:
                st.success(f"✅ Successfully downloaded {len(df)} records from '{name}'")
                
                # Show data preview
                st.subheader("📊 Data Preview")
                st.dataframe(df.head(10), use_container_width=True)
                
                # Show column info
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Total Rows", len(df))
                with col2:
                    st.metric("Total Columns", len(df.columns))
                with col3:
                    st.metric("Memory Usage", f"{df.memory_usage(deep=True).sum() / 1024 / 1024:.1f} MB")
                
                # Column details
                with st.expander("📋 Column Details"):
                    col_info = pd.DataFrame({
                        'Column': df.columns,
                        'Type': df.dtypes,
                        'Non-Null Count': df.count(),
                        'Null Count': df.isnull().sum()
                    })
                    st.dataframe(col_info, use_container_width=True)
            else:
                st.warning(f"⚠️ No data found in '{name}'")
                
        except Exception as e:
            st.error(f"❌ Failed to download sample data: {str(e)}")

def diagnose_issues_tab():
    """Diagnose column mapping and feed issues."""
    st.header("🔍 Diagnose Feed Issues")
    st.markdown("Troubleshoot column mapping and data issues with your feeds.")
    
    configs = st.session_state.config_manager.configs
    
    if not configs:
        st.info("📭 No feed configurations to diagnose. Create one first.")
        return
    
    selected_config = st.selectbox(
        "Select Configuration to Diagnose",
        list(configs.keys()),
        help="Choose the feed configuration you're having issues with"
    )
    
    if selected_config:
        config = configs[selected_config]
        
        st.subheader(f"🔍 Diagnosing: {selected_config}")
        
        # Show current configuration
        with st.expander("📋 Current Configuration"):
            st.json(config)
        
        if st.button("🔍 Run Full Diagnosis", type="primary"):
            run_full_diagnosis(selected_config, config)

def run_full_diagnosis(name: str, config: Dict):
    """Run comprehensive diagnosis of a feed configuration."""
    with st.spinner(f"Running diagnosis for {name}..."):
        try:
            # Step 1: Test connection
            st.write("**Step 1: Testing Connection**")
            feed_type = config.get('type')
            
            if feed_type == 'url':
                success = st.session_state.feed_manager.test_url_connection(
                    url=config['url'],
                    headers=config.get('headers'),
                    timeout=config.get('timeout', 30)
                )
                if success:
                    st.success("✅ Connection test passed")
                else:
                    st.error("❌ Connection test failed")
                    return
            
            # Step 2: Download and analyze data
            st.write("**Step 2: Downloading and Analyzing Data**")
            
            if feed_type == 'url':
                file_path = st.session_state.feed_manager.download_from_url(
                    url=config['url'],
                    headers=config.get('headers'),
                    auth=tuple(config['auth']) if config.get('auth') else None,
                    timeout=config.get('timeout', 30)
                )
                processor = FileProcessor()
                df = processor.process_file_by_path(file_path)
            else:
                st.warning("Full diagnosis currently only supports URL feeds")
                return
            
            if df.empty:
                st.error("❌ No data found in feed")
                return
            
            st.success(f"✅ Downloaded {len(df)} rows with {len(df.columns)} columns")
            
            # Step 3: Analyze columns
            st.write("**Step 3: Column Analysis**")
            available_columns = list(df.columns)
            st.info(f"**Available columns in feed**: {', '.join(available_columns)}")
            
            # Step 4: Check column mapping
            st.write("**Step 4: Column Mapping Validation**")
            column_mapping = config.get('column_mapping', {})
            selected_columns = config.get('selected_columns', [])
            
            if column_mapping:
                st.info(f"**Current mapping**: {column_mapping}")
                
                # Check for missing columns
                missing_columns = []
                existing_columns = []
                
                for field, source_column in column_mapping.items():
                    if source_column in available_columns:
                        existing_columns.append(source_column)
                        st.success(f"✅ {field} → {source_column} (exists)")
                    else:
                        missing_columns.append(source_column)
                        st.error(f"❌ {field} → {source_column} (MISSING)")
                
                if missing_columns:
                    st.error(f"**Found {len(missing_columns)} missing columns**: {', '.join(missing_columns)}")
                    
                    # Suggest fixes
                    st.write("**💡 Suggested Fixes:**")
                    for missing_col in missing_columns:
                        st.write(f"- **{missing_col}**: Look for similar columns like:")
                        similar_cols = [col for col in available_columns if missing_col.lower() in col.lower() or col.lower() in missing_col.lower()]
                        if similar_cols:
                            for similar in similar_cols[:3]:  # Show top 3 matches
                                st.write(f"  - `{similar}`")
                        else:
                            st.write("  - No similar columns found")
                else:
                    st.success("✅ All mapped columns exist in the feed")
            else:
                st.warning("⚠️ No column mapping configured")
            
            # Step 5: Check selected columns
            if selected_columns:
                st.write("**Step 5: Selected Columns Validation**")
                st.info(f"**Selected columns**: {', '.join(selected_columns)}")
                
                missing_selected = [col for col in selected_columns if col not in available_columns]
                if missing_selected:
                    st.error(f"❌ Selected columns not found in feed: {', '.join(missing_selected)}")
                else:
                    st.success("✅ All selected columns exist in the feed")
            
            # Step 6: Data quality check
            st.write("**Step 6: Data Quality Check**")
            if column_mapping and 'SKU' in column_mapping:
                sku_column = column_mapping['SKU']
                if sku_column in df.columns:
                    sku_data = df[sku_column]
                    null_count = sku_data.isnull().sum()
                    empty_count = (sku_data == '').sum()
                    duplicate_count = sku_data.duplicated().sum()
                    
                    st.write(f"**SKU Column ({sku_column}) Quality:**")
                    if null_count > 0:
                        st.warning(f"⚠️ {null_count} null SKUs found")
                    if empty_count > 0:
                        st.warning(f"⚠️ {empty_count} empty SKUs found")
                    if duplicate_count > 0:
                        st.warning(f"⚠️ {duplicate_count} duplicate SKUs found")
                    
                    if null_count == 0 and empty_count == 0 and duplicate_count == 0:
                        st.success("✅ SKU data quality looks good")
            
            # Show sample of processed data
            st.write("**Step 7: Processed Data Preview**")
            
            if column_mapping:
                # Apply column mapping to show final result
                mapper = ColumnMapper(df.columns.tolist())
                mapped_df = mapper.get_mapped_data(df, column_mapping)
                
                if not mapped_df.empty:
                    st.success(f"✅ Successfully mapped data: {len(mapped_df)} rows, {len(mapped_df.columns)} columns")
                    st.dataframe(mapped_df.head(5), use_container_width=True)
                else:
                    st.error("❌ Column mapping resulted in empty dataset")
            else:
                st.dataframe(df.head(5), use_container_width=True)
                
        except Exception as e:
            st.error(f"❌ Diagnosis failed: {str(e)}")
            st.write("**Error Details:**", str(e))

if __name__ == "__main__":
    main()