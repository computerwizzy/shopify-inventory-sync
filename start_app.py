#!/usr/bin/env python3
"""
Startup script for Shopify Inventory Sync App
"""

import subprocess
import sys
import os
import webbrowser
import time

def start_app():
    """Start the Streamlit app"""
    print("🚀 Starting Shopify Inventory Sync App...")
    print("=" * 50)
    
    # Check if we're in the right directory
    if not os.path.exists('app.py'):
        print("❌ Error: app.py not found in current directory")
        print("Please run this script from the inventory-sync-app folder")
        return
    
    # Check if .env file exists
    if not os.path.exists('.env'):
        print("⚠️  Warning: .env file not found")
        print("Please create .env file with your Shopify credentials")
        return
    
    print("✅ Environment check passed")
    print("📡 Starting Streamlit server...")
    print()
    
    # Start Streamlit
    port = 8504
    url = f"http://localhost:{port}"
    
    print(f"🌐 App will be available at: {url}")
    print("🔄 Starting server (this may take a moment)...")
    print()
    print("📝 Instructions:")
    print("   1. Wait for your browser to open automatically")
    print("   2. If browser doesn't open, manually go to:", url)
    print("   3. Press Ctrl+C in this window to stop the app")
    print("=" * 50)
    
    try:
        # Start Streamlit
        cmd = [
            sys.executable, "-m", "streamlit", "run", "app.py",
            "--server.port", str(port),
            "--server.headless", "false"
        ]
        
        subprocess.run(cmd, check=True)
        
    except KeyboardInterrupt:
        print("\n👋 App stopped by user")
    except subprocess.CalledProcessError as e:
        print(f"\n❌ Error starting app: {e}")
        print("Try running: python -m streamlit run app.py")
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")

if __name__ == "__main__":
    start_app()