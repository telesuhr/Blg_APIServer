#!/usr/bin/env python3
"""
Bloomberg API Bridge - Usage Examples
=====================================
10 practical examples of using the Bloomberg client
"""

from bloomberg_client import BloombergClient, create_client
from datetime import datetime, timedelta
import pandas as pd

# Initialize client (replace with your Windows PC IP)
# client = create_client("192.168.1.100")  # Production
client = create_client("localhost")  # Testing


def example_1_basic_price_data():
    """Example 1: Get basic price data for stocks"""
    print("\n=== Example 1: Basic Price Data ===")
    
    # Get last prices for multiple securities
    securities = ["AAPL US Equity", "MSFT US Equity", "GOOGL US Equity"]
    prices = client.get_last_price(securities)
    
    for security, price in prices.items():
        print(f"{security}: ${price}")


def example_2_historical_data():
    """Example 2: Get historical price data"""
    print("\n=== Example 2: Historical Price Data ===")
    
    # Get 30 days of price and volume data
    data = client.get_historical_data(
        securities="AAPL US Equity",
        fields=["PX_LAST", "VOLUME"],
        start_date="2024-01-01",
        end_date="2024-01-31"
    )
    
    # Access the DataFrame for AAPL
    aapl_data = data["AAPL US Equity"]
    print(f"Data shape: {aapl_data.shape}")
    print(f"Average price: ${aapl_data['PX_LAST'].mean():.2f}")
    print(f"Total volume: {aapl_data['VOLUME'].sum():,.0f}")


def example_3_company_information():
    """Example 3: Get company reference data"""
    print("\n=== Example 3: Company Information ===")
    
    # Get comprehensive company info
    info = client.get_company_info(["AAPL US Equity", "MSFT US Equity"])
    
    for security, details in info.items():
        print(f"\n{security}:")
        for field, value in details.items():
            print(f"  {field}: {value}")


def example_4_commodity_data():
    """Example 4: Get commodity futures data"""
    print("\n=== Example 4: Commodity Data ===")
    
    # LME Copper and COMEX Gold futures
    commodities = ["LMCADY Index", "HG1 Comdty", "GC1 Comdty"]
    
    # Get reference data
    ref_data = client.get_reference_data(
        securities=commodities,
        fields=["PX_LAST", "VOLUME", "OPEN_INT", "FUT_CONT_SIZE"]
    )
    
    for commodity, data in ref_data.items():
        print(f"\n{commodity}:")
        print(f"  Last Price: {data.get('PX_LAST')}")
        print(f"  Volume: {data.get('VOLUME')}")
        print(f"  Open Interest: {data.get('OPEN_INT')}")


def example_5_forex_rates():
    """Example 5: Get foreign exchange rates"""
    print("\n=== Example 5: FX Rates ===")
    
    # Major currency pairs
    fx_pairs = ["EURUSD Curncy", "GBPUSD Curncy", "USDJPY Curncy", "USDCNY Curncy"]
    
    # Get current rates
    rates = client.get_last_price(fx_pairs)
    
    for pair, rate in rates.items():
        print(f"{pair}: {rate}")
    
    # Get historical data for analysis
    eur_data = client.get_price_history("EURUSD Curncy", days=30)
    if not eur_data.empty:
        print(f"\nEUR/USD 30-day range: {eur_data['PX_LAST'].min():.4f} - {eur_data['PX_LAST'].max():.4f}")


def example_6_export_data():
    """Example 6: Export data to different formats"""
    print("\n=== Example 6: Export Data ===")
    
    # Get data for multiple securities
    data = client.get_historical_data(
        securities=["AAPL US Equity", "MSFT US Equity"],
        fields=["PX_OPEN", "PX_HIGH", "PX_LOW", "PX_LAST", "VOLUME"],
        start_date="2024-01-01",
        end_date="2024-01-10"
    )
    
    # Export to CSV
    client.export_to_csv(data, "tech_stocks.csv")
    print("✓ Exported to CSV: tech_stocks_AAPL_US_Equity.csv, tech_stocks_MSFT_US_Equity.csv")
    
    # Export to Excel (all securities in one file)
    client.export_to_excel(data, "tech_stocks.xlsx")
    print("✓ Exported to Excel: tech_stocks.xlsx")
    
    # Export to JSON
    client.export_to_json(data, "tech_stocks.json")
    print("✓ Exported to JSON: tech_stocks.json")


def example_7_batch_processing():
    """Example 7: Batch processing multiple securities"""
    print("\n=== Example 7: Batch Processing ===")
    
    # S&P 500 sector ETFs
    sectors = [
        "XLK US Equity",  # Technology
        "XLF US Equity",  # Financials
        "XLV US Equity",  # Healthcare
        "XLE US Equity",  # Energy
        "XLI US Equity",  # Industrials
    ]
    
    # Get year-to-date performance
    start_date = "2024-01-01"
    end_date = "2024-01-31"
    
    data = client.get_historical_data(
        securities=sectors,
        fields=["PX_LAST"],
        start_date=start_date,
        end_date=end_date
    )
    
    # Calculate returns
    print("YTD Performance:")
    for sector, df in data.items():
        if not df.empty:
            start_price = df['PX_LAST'].iloc[0]
            end_price = df['PX_LAST'].iloc[-1]
            return_pct = ((end_price - start_price) / start_price) * 100
            print(f"  {sector}: {return_pct:+.2f}%")


def example_8_intraday_data():
    """Example 8: Get intraday data (mock mode only)"""
    print("\n=== Example 8: Intraday Data ===")
    
    # Get 1-minute bars for the last hour
    end_time = datetime.now()
    start_time = end_time - timedelta(hours=1)
    
    intraday = client.get_intraday_data(
        security="AAPL US Equity",
        start_datetime=start_time,
        end_datetime=end_time,
        interval=5  # 5-minute bars
    )
    
    if isinstance(intraday, pd.DataFrame) and not intraday.empty:
        print(f"Received {len(intraday)} bars")
        print("\nLast 5 bars:")
        print(intraday.tail())


def example_9_cache_management():
    """Example 9: Cache management"""
    print("\n=== Example 9: Cache Management ===")
    
    # First request - will hit the server
    print("First request (uncached)...")
    start = datetime.now()
    data1 = client.get_last_price("AAPL US Equity")
    time1 = (datetime.now() - start).total_seconds()
    print(f"Time taken: {time1:.3f} seconds")
    
    # Second request - should be cached
    print("\nSecond request (cached)...")
    start = datetime.now()
    data2 = client.get_last_price("AAPL US Equity")
    time2 = (datetime.now() - start).total_seconds()
    print(f"Time taken: {time2:.3f} seconds")
    
    if time2 < time1:
        print("✓ Cache is working!")
    
    # Clear cache
    print("\nClearing cache...")
    client.clear_cache()
    print("✓ Cache cleared")


def example_10_error_handling():
    """Example 10: Error handling"""
    print("\n=== Example 10: Error Handling ===")
    
    # Test with invalid security
    print("Testing invalid security...")
    try:
        data = client.get_last_price("INVALID_TICKER")
        print(f"Data: {data}")
    except Exception as e:
        print(f"✓ Error caught: {e}")
    
    # Test with invalid date range
    print("\nTesting invalid date range...")
    try:
        data = client.get_historical_data(
            securities="AAPL US Equity",
            fields=["PX_LAST"],
            start_date="2024-01-01",
            end_date="2023-01-01"  # End before start
        )
    except Exception as e:
        print(f"✓ Error caught: {e}")


def main():
    """Run all examples"""
    print("Bloomberg API Bridge - Usage Examples")
    print("=====================================")
    
    # Check connection first
    try:
        health = client.check_connection()
        print(f"Connected to server in {health['mode']} mode")
    except Exception as e:
        print(f"Failed to connect: {e}")
        return
    
    # Run examples
    examples = [
        example_1_basic_price_data,
        example_2_historical_data,
        example_3_company_information,
        example_4_commodity_data,
        example_5_forex_rates,
        example_6_export_data,
        example_7_batch_processing,
        example_8_intraday_data,
        example_9_cache_management,
        example_10_error_handling
    ]
    
    for example in examples:
        try:
            example()
        except Exception as e:
            print(f"\nError in {example.__name__}: {e}")
        print()  # Blank line between examples
    
    print("\n✅ All examples completed!")


if __name__ == "__main__":
    main()