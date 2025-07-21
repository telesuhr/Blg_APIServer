#!/usr/bin/env python3
"""
Quick start script to test Bloomberg connection
"""

import sys
from bloomberg_client import BloombergClient, create_client

def main():
    # Get server host from command line or use default
    host = sys.argv[1] if len(sys.argv) > 1 else "localhost"
    
    print(f"Bloomberg API Bridge - Quick Start")
    print("=" * 50)
    print(f"Connecting to server: {host}:8080")
    print()
    
    try:
        # Create client
        client = create_client(host)
        
        # Check connection
        print("1. Checking connection...")
        health = client.check_connection()
        print(f"   ✓ Server status: {health['status']}")
        print(f"   ✓ Mode: {health['mode']}")
        print(f"   ✓ Bloomberg available: {health['bloomberg_available']}")
        print()
        
        # Test reference data
        print("2. Testing reference data...")
        securities = ["AAPL US Equity", "MSFT US Equity"]
        prices = client.get_last_price(securities)
        for sec, price in prices.items():
            print(f"   {sec}: ${price}")
        print()
        
        # Test historical data
        print("3. Testing historical data...")
        data = client.get_price_history("AAPL US Equity", days=5)
        if not data.empty:
            print("   Last 5 days of AAPL:")
            print(data.tail())
        else:
            print("   No data returned")
        print()
        
        print("✅ All tests passed! Bloomberg API Bridge is working.")
        
        if health.get('is_mock') or health['mode'] == 'mock':
            print("\n⚠️  Note: Server is running in MOCK MODE")
            print("   To use real Bloomberg data, ensure Bloomberg Terminal is running on the Windows PC")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        print("\nTroubleshooting:")
        print("1. Make sure the Windows server is running (start_server.bat)")
        print("2. Check the IP address is correct")
        print("3. Ensure port 8080 is not blocked by firewall")
        print("4. Verify the API key matches between server and client")
        sys.exit(1)

if __name__ == "__main__":
    main()