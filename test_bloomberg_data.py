#!/usr/bin/env python3
"""
Test Bloomberg data retrieval
"""

from bloomberg_client import create_client
from datetime import datetime, timedelta
import pandas as pd

# Connect to Windows Bloomberg server
client = create_client('100.111.86.75')

print("Bloomberg Data Test")
print("=" * 50)

# 1. Check server status
health = client.check_connection()
print(f"\nServer Status: {health['status']}")
print(f"Mode: {health['mode']}")
print(f"Bloomberg Terminal: {'Connected' if health['bbcomm_running'] else 'Not detected'}")

# 2. Get current prices
print("\n--- Current Prices ---")
securities = ["AAPL US Equity", "MSFT US Equity", "LMCADY Index", "HG1 Comdty"]
prices = client.get_last_price(securities)
for sec, price in prices.items():
    print(f"{sec}: ${price}")

# 3. Get historical data for copper
print("\n--- LME Copper Historical Data ---")
copper_data = client.get_historical_data(
    securities="LMCADY Index",
    fields=["PX_LAST", "VOLUME"],
    start_date="2025-01-01",
    end_date="2025-01-10"
)

if "LMCADY Index" in copper_data:
    df = copper_data["LMCADY Index"]
    print(f"Data points: {len(df)}")
    print("\nFirst 5 days:")
    print(df.head())
    
    # Save to CSV
    df.to_csv("lme_copper_test.csv")
    print("\nData saved to: lme_copper_test.csv")

# 4. Get FX rates
print("\n--- FX Rates ---")
fx_pairs = ["USDJPY Curncy", "EURUSD Curncy"]
fx_data = client.get_reference_data(
    securities=fx_pairs,
    fields=["PX_LAST", "NAME"]
)
for pair, data in fx_data.items():
    print(f"{pair}: {data.get('PX_LAST')} - {data.get('NAME')}")

print("\n" + "=" * 50)
if health['mode'] == 'mock':
    print("⚠️  Note: Using MOCK data (Bloomberg Terminal not detected)")
    print("To get real data:")
    print("1. Ensure Bloomberg Terminal is running on Windows")
    print("2. Log in to Bloomberg")
    print("3. Install blpapi: pip install blpapi")
    print("4. Restart the server")
else:
    print("✅ Connected to Bloomberg Terminal - Real data")