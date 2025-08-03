# 📦 Inventory Synchronization App

A complete Shopify inventory synchronization application with multiple feed sources, scheduled automation, column mapping, SKU matching, and real-time inventory sync capabilities.

## 🌟 Features

### 📁 Data Sources
- **File Upload Support**: CSV and Excel file processing with automatic encoding detection
- **FTP/SFTP Integration**: Connect to FTP and SFTP servers for automated file retrieval
- **URL/API Feeds**: Download inventory files from web URLs with authentication support
- **Google Sheets Integration**: Direct integration with Google Sheets using service accounts

### 🤖 Automation
- **Scheduled Sync**: Automated inventory synchronization with flexible scheduling
- **Cron & Interval Scheduling**: Support for both cron expressions and interval-based scheduling
- **Job Management**: Create, pause, resume, and monitor scheduled sync jobs
- **Error Handling & Recovery**: Comprehensive error handling with retry mechanisms

### 🔧 Processing & Matching
- **Smart Column Mapping**: Auto-detection and manual mapping of data columns
- **Advanced SKU Matching**: Exact and fuzzy matching with confidence scoring
- **Real-time Sync**: Batch inventory updates with progress tracking
- **Validation & Quality Control**: Data validation and quality checks before sync

### 📊 Monitoring & Management
- **Job History & Analytics**: Detailed execution history and performance metrics
- **System Health Monitoring**: Real-time monitoring of scheduled jobs and system status
- **Logging & Alerts**: Comprehensive logging with error tracking
- **User-friendly Interface**: Clean Streamlit web interface with intuitive navigation

## 🚀 Quick Start

### Prerequisites

- Python 3.7+
- Shopify store with private app access
- Shopify Admin API access token

### Installation

1. **Clone/Download the project**
   ```bash
   cd inventory-sync-app
   ```

2. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

3. **Set up environment variables**
   ```bash
   cp .env.example .env
   ```
   
   Edit `.env` file with your Shopify credentials:
   ```env
   SHOPIFY_STORE_URL=your-store.myshopify.com
   SHOPIFY_ACCESS_TOKEN=your_access_token_here
   SHOPIFY_API_VERSION=2023-10
   ```

4. **Run the application**
   ```bash
   streamlit run app.py
   ```

5. **Open your browser**
   Navigate to `http://localhost:8501`

## 🌐 Streamlit Cloud Deployment

Deploy your app to Streamlit Cloud for 24/7 operation:

1. **Push to GitHub** (see [DEPLOYMENT.md](DEPLOYMENT.md) for detailed guide)
2. **Connect to Streamlit Cloud** at [share.streamlit.io](https://share.streamlit.io)
3. **Configure Secrets** in app settings:
   ```toml
   [shopify]
   SHOP_NAME = "your-shop-name"
   ACCESS_TOKEN = "shpat_..."
   API_VERSION = "2025-01"
   ```
4. **Deploy** and access your app anywhere!

📖 **Full deployment guide**: See [DEPLOYMENT.md](DEPLOYMENT.md)

## 🔧 Configuration

### Shopify Setup

1. **Create a Private App** in your Shopify admin
2. **Enable Admin API access** with these permissions:
   - Products: Read access
   - Inventory: Read and write access
3. **Copy your Access Token** to the `.env` file

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `SHOPIFY_STORE_URL` | Your Shopify store URL | Required |
| `SHOPIFY_ACCESS_TOKEN` | Private app access token | Required |
| `SHOPIFY_API_VERSION` | Shopify API version | 2023-10 |
| `FUZZY_MATCH_THRESHOLD` | Fuzzy matching confidence threshold (0-100) | 85 |
| `MAX_FILE_SIZE_MB` | Maximum upload file size in MB | 100 |
| `BATCH_SIZE` | Number of items to sync per batch | 10 |

## 📊 Usage Workflow

### Step 1: Upload File
- Support for CSV, Excel (.xlsx, .xls) files
- Automatic encoding detection for CSV files
- File validation and preview

### Step 2: Map Columns
- Auto-detection of common column names
- Manual mapping interface for custom columns
- Required fields: SKU, Quantity
- Optional fields: Product Title, Price, etc.

### Step 3: Match SKUs
- Exact matching (case-sensitive and case-insensitive)
- Fuzzy matching with confidence scoring
- Real-time matching statistics
- Preview of matched products

### Step 4: Sync Inventory
- Configurable sync options
- Dry-run mode for testing
- Progress tracking with detailed results
- Error handling and reporting

## 📁 Project Structure

```
inventory-sync-app/
├── app.py                 # Main Streamlit application
├── requirements.txt       # Python dependencies
├── .env.example          # Environment template
├── README.md             # This file
├── src/
│   ├── __init__.py
│   ├── file_processor.py  # File upload and processing
│   ├── column_mapper.py   # Column mapping interface
│   ├── sku_matcher.py     # SKU matching logic
│   └── shopify_client.py  # Shopify API integration
└── utils/
    ├── __init__.py
    └── config.py          # Configuration management
```

## 🔍 File Format Requirements

### CSV Files
- UTF-8, Latin1, or CP1252 encoding supported
- Comma-separated values
- First row should contain column headers

### Excel Files
- .xlsx and .xls formats supported
- First row should contain column headers
- Data should start from row 2

### Required Columns
- **SKU**: Unique product identifier matching Shopify products
- **Quantity**: Numeric inventory quantity

### Sample File Format
```csv
SKU,Quantity,Product Name,Price
ABC123,50,Sample Product 1,29.99
XYZ789,25,Sample Product 2,39.99
DEF456,0,Sample Product 3,19.99
```

## ⚙️ Advanced Features

### Fuzzy Matching
- Uses Levenshtein distance algorithm
- Configurable confidence threshold
- Handles minor SKU variations and typos

### Batch Processing
- Configurable batch sizes for large uploads
- Progress tracking and error reporting
- Rate limiting to comply with Shopify API limits

### Validation
- File format validation
- Data type validation
- SKU uniqueness checks
- Quantity validation

## 🚨 Troubleshooting

### Common Issues

1. **"Shopify configuration is missing"**
   - Check your `.env` file exists and contains valid credentials
   - Verify your Shopify store URL format (should end with .myshopify.com)

2. **"Error connecting to Shopify"**
   - Verify your access token has correct permissions
   - Check your store URL is correct
   - Ensure your internet connection is stable

3. **"No matches found"**
   - Verify SKUs in your file match exactly with Shopify products
   - Try lowering the fuzzy match threshold
   - Check for extra spaces or special characters in SKUs

4. **"File processing failed"**
   - Ensure file format is CSV, XLSX, or XLS
   - Check file encoding (try saving as UTF-8)
   - Verify file is not corrupted

### Debug Mode

Enable debug mode in your `.env` file:
```env
DEBUG_MODE=True
LOG_LEVEL=DEBUG
```

## 🔐 Security Notes

- Never commit your `.env` file to version control
- Keep your Shopify access token secure
- Use private apps instead of public apps when possible
- Regularly rotate access tokens

## 📝 API Rate Limits

The app automatically handles Shopify's API rate limits:
- Built-in 500ms delay between requests
- Automatic retry with exponential backoff
- Batch processing to minimize API calls

## 🆘 Support

If you encounter issues:

1. Check the troubleshooting section above
2. Verify your Shopify permissions and credentials
3. Ensure your file format matches requirements
4. Check the console for detailed error messages

## 📄 License

This project is provided as-is for educational and commercial use.

---

**Made with ❤️ for Shopify merchants**