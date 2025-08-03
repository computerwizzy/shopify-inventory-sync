@echo off
echo Starting Shopify Inventory Sync App...
echo.
echo Open your browser and go to: http://localhost:8503
echo.
echo Press Ctrl+C to stop the app
echo.
python -m streamlit run app.py --server.port 8503 --server.headless false
pause