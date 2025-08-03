# 🚀 Deployment Guide: Streamlit Community Cloud

This guide will help you deploy the Shopify Inventory Sync App to Streamlit Community Cloud.

## 📋 Prerequisites

1. **GitHub Account** - Required for Streamlit Cloud
2. **Shopify Store** with API access
3. **Shopify Private App** or Custom App with inventory permissions

## 🔧 Step 1: Prepare Your Repository

### 1.1 Push to GitHub
```bash
# Initialize git repository (if not already done)
git init
git add .
git commit -m "Initial commit: Shopify Inventory Sync App"

# Add your GitHub remote
git remote add origin https://github.com/YOUR_USERNAME/YOUR_REPO_NAME.git
git push -u origin main
```

### 1.2 Required Files ✅
Your repository should have these files:
- `app.py` - Main Streamlit application
- `requirements.txt` - Python dependencies
- `.streamlit/config.toml` - Streamlit configuration
- `.streamlit/secrets.toml.example` - Secrets template
- `.env.example` - Environment variables template

## 🌐 Step 2: Deploy to Streamlit Cloud

### 2.1 Access Streamlit Cloud
1. Go to [share.streamlit.io](https://share.streamlit.io)
2. Sign in with your GitHub account
3. Click **"New app"**

### 2.2 Configure Deployment
- **Repository**: Select your GitHub repository
- **Branch**: `main` (or your default branch)
- **Main file path**: `app.py`
- **App URL**: Choose a custom URL (optional)

## 🔐 Step 3: Configure Secrets

### 3.1 Shopify Configuration (Required)
In your app's **Settings** → **Secrets**, add:

```toml
[shopify]
SHOP_NAME = "your-shop-name"  # Just the name, not full URL
ACCESS_TOKEN = "shpat_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
API_VERSION = "2025-01"
```

### 3.2 Google Sheets (Optional)
If using Google Sheets integration:

```toml
[google]
SERVICE_ACCOUNT_JSON = '''
{
  "type": "service_account",
  "project_id": "your-project-id",
  "private_key_id": "key-id",
  "private_key": "-----BEGIN PRIVATE KEY-----\\n...\\n-----END PRIVATE KEY-----\\n",
  "client_email": "service-account@project.iam.gserviceaccount.com",
  "client_id": "123456789",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token"
}
'''
```

## 🎯 Step 4: Get Shopify API Credentials

### 4.1 Create a Private App
1. Go to your Shopify Admin → **Settings** → **Apps and sales channels**
2. Click **"Develop apps"** → **"Create an app"**
3. Name your app (e.g., "Inventory Sync")
4. Click **"Configure Admin API scopes"**

### 4.2 Required Permissions
Enable these scopes:
- `read_products` - Read product information
- `read_product_listings` - Read product listings  
- `write_inventory` - Update inventory levels
- `read_inventory` - Read inventory levels

### 4.3 Install and Get Token
1. Click **"Install app"**
2. Copy the **Admin API access token**
3. Use this token in your Streamlit secrets

## 🔄 Step 5: Deploy and Test

### 5.1 Deploy
1. Click **"Deploy!"** in Streamlit Cloud
2. Wait for deployment to complete
3. Your app will be available at your chosen URL

### 5.2 Initial Testing
1. **Test API Connection**: Go to app → Settings → Test Shopify connection
2. **Configure Feed Source**: Add your first data source
3. **Test Feed Connection**: Verify data can be retrieved
4. **Create Test Sync**: Set up a manual sync to verify functionality

## 🔧 Step 6: Production Configuration

### 6.1 Feed Sources
Configure your inventory data sources:
- **FTP/SFTP**: Supplier inventory files
- **URL/CSV**: Direct file downloads
- **Google Sheets**: Real-time spreadsheet data

### 6.2 Scheduled Syncs
Set up automated synchronization:
- Choose sync frequency (hourly, daily, etc.)
- Configure column mapping
- Set up error notifications

## 🔍 Troubleshooting

### Common Issues

**❌ "No module named 'xyz'"**
- Add missing packages to `requirements.txt`
- Redeploy the app

**❌ "Shopify API connection failed"**
- Check your `SHOP_NAME` (without .myshopify.com)
- Verify `ACCESS_TOKEN` is correct
- Ensure API permissions are granted

**❌ "Feed connection failed"**
- Check feed URL/credentials
- Verify network access from Streamlit Cloud
- Test with smaller files first

**❌ "Session state errors"**
- Clear browser cache
- Restart the Streamlit app
- Check for any persistent state conflicts

### Support Resources
- [Streamlit Docs](https://docs.streamlit.io/)
- [Shopify API Docs](https://shopify.dev/docs/admin-api)
- [GitHub Issues](https://github.com/YOUR_USERNAME/YOUR_REPO_NAME/issues)

## 🎉 Success!

Your Shopify Inventory Sync App is now deployed and ready to automate your inventory management!

### Next Steps
1. Monitor sync logs regularly
2. Set up additional feed sources as needed
3. Fine-tune sync schedules based on usage
4. Consider backup strategies for critical data

---

**📧 Need Help?** Open an issue in your GitHub repository or check the Streamlit Community forum.