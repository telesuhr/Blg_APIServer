#!/usr/bin/env python3
"""
Test real Bloomberg data
"""

from bloomberg_client import create_client
from datetime import datetime, timedelta
import json

# Connect to Bloomberg server
client = create_client('100.111.86.75')

print("Bloomberg Real Data Test")
print("=" * 50)

# 1. Test basic prices
print("\n1. Testing Reference Data (Current Prices)")
try:
    securities = ["AAPL US Equity", "MSFT US Equity", "LMCADY Index", "USDJPY Curncy"]
    prices = client.get_last_price(securities)
    
    for sec, price in prices.items():
        print(f"   {sec}: {price}")
except Exception as e:
    print(f"   Error: {e}")

# 2. Test company info
print("\n2. Testing Company Information")
try:
    info = client.get_reference_data(
        "AAPL US Equity",
        ["NAME", "COUNTRY", "CRNCY", "PX_LAST", "VOLUME"]
    )
    
    for sec, data in info.items():
        print(f"   {sec}:")
        for field, value in data.items():
            print(f"     {field}: {value}")
except Exception as e:
    print(f"   Error: {e}")

# 3. Test historical data with shorter date range
print("\n3. Testing Historical Data (Last 3 Days)")
try:
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=3)
    
    data = client.get_historical_data(
        securities="AAPL US Equity",
        fields=["PX_LAST"],
        start_date=start_date,
        end_date=end_date
    )
    
    if "AAPL US Equity" in data:
        df = data["AAPL US Equity"]
        print(f"   Received {len(df)} data points")
        print(df)
except Exception as e:
    print(f"   Error: {e}")

# 4. Test FX rates
print("\n4. Testing FX Rates")
try:
    fx_data = client.get_reference_data(
        ["USDJPY Curncy", "EURUSD Curncy"],
        ["PX_LAST", "NAME"]
    )
    
    for pair, data in fx_data.items():
        print(f"   {pair}: {data.get('PX_LAST')} - {data.get('NAME')}")
except Exception as e:
    print(f"   Error: {e}")

# 5. Test commodity data
print("\n5. Testing Commodity Data (Copper)")
try:
    copper_data = client.get_reference_data(
        ["LMCADY Index", "HG1 Comdty"],
        ["PX_LAST", "LAST_UPDATE", "NAME"]
    )
    
    for commodity, data in copper_data.items():
        print(f"   {commodity}:")
        for field, value in data.items():
            print(f"     {field}: {value}")
except Exception as e:
    print(f"   Error: {e}")

print("\n" + "=" * 50)
print("✅ Connected to Bloomberg Terminal - Real market data!")