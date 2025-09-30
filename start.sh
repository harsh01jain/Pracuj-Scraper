# Install Playwright browsers
playwright install

# Start FastAPI with Uvicorn
uvicorn scraper:app --host 0.0.0.0 --port $PORT
