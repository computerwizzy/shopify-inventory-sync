#!/bin/bash

echo "Starting Inventory Sync App..."
echo ""
echo "Make sure you have:"
echo "1. Installed Python 3.7+"
echo "2. Installed requirements: pip install -r requirements.txt"
echo "3. Configured your .env file with Shopify credentials"
echo ""
echo "Press Enter to continue..."
read

streamlit run app.py