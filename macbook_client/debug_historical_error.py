#!/usr/bin/env python3
"""
Debug historical data error
"""

import requests
import json
from datetime import datetime, timedelta

# Server details
SERVER_URL = "http://100.111.86.75:8080"
API_KEY = "your-secure-api-key-here"

print("Bloomberg Historical Data Debug")
print("=" * 50)

# Test different date formats and ranges
test_cases = [
    {
        "name": "Test 1: Date objects",
        "securities": ["AAPL US Equity"],
        "fields": ["PX_LAST"],
        "start_date": (datetime.now() - timedelta(days=5)).date().isoformat(),
        "end_date": datetime.now().date().isoformat()
    },
    {
        "name": "Test 2: Single day",
        "securities": ["AAPL US Equity"],
        "fields": ["PX_LAST"],
        "start_date": "2025-01-21",
        "end_date": "2025-01-21"
    },
    {
        "name": "Test 3: Different security type",
        "securities": ["USDJPY Curncy"],
        "fields": ["PX_LAST"],
        "start_date": "2025-01-20",
        "end_date": "2025-01-21"
    },
    {
        "name": "Test 4: Multiple fields",
        "securities": ["AAPL US Equity"],
        "fields": ["PX_OPEN", "PX_HIGH", "PX_LOW", "PX_LAST"],
        "start_date": "2025-01-21",
        "end_date": "2025-01-21"
    }
]

headers = {
    "api-key": API_KEY,
    "Content-Type": "application/json"
}

for test in test_cases:
    print(f"\n{test['name']}")
    print("-" * 40)
    print(f"Request: {json.dumps(test, indent=2)}")
    
    try:
        response = requests.post(
            f"{SERVER_URL}/historical_data",
            json=test,
            headers=headers,
            timeout=30
        )
        
        print(f"Status Code: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            print("✓ Success!")
            print(f"Response: {json.dumps(data, indent=2)[:500]}...")
        else:
            print("✗ Error!")
            print(f"Response: {response.text[:500]}")
            
    except Exception as e:
        print(f"✗ Exception: {e}")

# Check server logs hint
print("\n" + "=" * 50)
print("💡 To see detailed error on Windows server:")
print("1. Check the console output where start_server.bat is running")
print("2. Check bloomberg_api_server.log file")
print("\nCommon issues:")
print("- Date format: Bloomberg expects YYYYMMDD format")
print("- Security not found: Check if ticker is correct")
print("- Field not available: Some fields may not be available for all securities")
print("- Weekend/holiday: No data for non-trading days")