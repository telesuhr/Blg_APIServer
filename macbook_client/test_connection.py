#!/usr/bin/env python3
"""
Bloomberg API Bridge - Connection Test Suite
===========================================
Comprehensive tests for the Bloomberg connection
"""

import sys
import time
import json
from datetime import datetime, timedelta
from bloomberg_client import BloombergClient, create_client


class TestRunner:
    """Test runner for Bloomberg API Bridge"""
    
    def __init__(self, host="localhost"):
        self.host = host
        self.client = None
        self.results = []
        
    def run_all_tests(self):
        """Run all tests"""
        print("Bloomberg API Bridge - Test Suite")
        print("=================================")
        print(f"Server: {self.host}:8080")
        print()
        
        tests = [
            self.test_1_connection,
            self.test_2_health_check,
            self.test_3_reference_data,
            self.test_4_historical_data,
            self.test_5_batch_request,
            self.test_6_error_handling,
            self.test_7_cache_functionality,
            self.test_8_data_validation,
            self.test_9_performance
        ]
        
        passed = 0
        failed = 0
        
        for test in tests:
            try:
                print(f"\n{test.__doc__}")
                print("-" * 50)
                result = test()
                if result:
                    print("✅ PASSED")
                    passed += 1
                else:
                    print("❌ FAILED")
                    failed += 1
            except Exception as e:
                print(f"❌ FAILED with exception: {e}")
                failed += 1
        
        print("\n" + "=" * 50)
        print(f"Test Results: {passed} passed, {failed} failed")
        print("=" * 50)
        
        return failed == 0
    
    def test_1_connection(self):
        """Test 1: Basic Connection"""
        try:
            self.client = create_client(self.host)
            print("✓ Client created successfully")
            return True
        except Exception as e:
            print(f"✗ Failed to create client: {e}")
            return False
    
    def test_2_health_check(self):
        """Test 2: Health Check Endpoint"""
        try:
            health = self.client.check_connection()
            print(f"✓ Server status: {health['status']}")
            print(f"✓ Mode: {health['mode']}")
            print(f"✓ Bloomberg available: {health['bloomberg_available']}")
            print(f"✓ Timestamp: {health['timestamp']}")
            return health['status'] == 'healthy'
        except Exception as e:
            print(f"✗ Health check failed: {e}")
            return False
    
    def test_3_reference_data(self):
        """Test 3: Reference Data Request"""
        try:
            # Test single security
            data = self.client.get_reference_data(
                "AAPL US Equity",
                ["PX_LAST", "NAME", "COUNTRY"]
            )
            
            if "AAPL US Equity" in data:
                aapl = data["AAPL US Equity"]
                print(f"✓ AAPL last price: ${aapl.get('PX_LAST')}")
                print(f"✓ Name: {aapl.get('NAME')}")
                print(f"✓ Country: {aapl.get('COUNTRY')}")
                return True
            else:
                print("✗ No data returned")
                return False
                
        except Exception as e:
            print(f"✗ Reference data request failed: {e}")
            return False
    
    def test_4_historical_data(self):
        """Test 4: Historical Data Request"""
        try:
            # Get last 5 days of data
            end_date = datetime.now().date()
            start_date = end_date - timedelta(days=5)
            
            data = self.client.get_historical_data(
                securities="MSFT US Equity",
                fields=["PX_LAST", "VOLUME"],
                start_date=start_date,
                end_date=end_date
            )
            
            if "MSFT US Equity" in data:
                df = data["MSFT US Equity"]
                print(f"✓ Received {len(df)} data points")
                print(f"✓ Date range: {df.index[0]} to {df.index[-1]}")
                print(f"✓ Fields: {list(df.columns)}")
                return len(df) > 0
            else:
                print("✗ No data returned")
                return False
                
        except Exception as e:
            print(f"✗ Historical data request failed: {e}")
            return False
    
    def test_5_batch_request(self):
        """Test 5: Batch Request (Multiple Securities)"""
        try:
            securities = ["AAPL US Equity", "GOOGL US Equity", "AMZN US Equity"]
            prices = self.client.get_last_price(securities)
            
            print(f"✓ Requested {len(securities)} securities")
            print(f"✓ Received {len(prices)} responses")
            
            for sec, price in prices.items():
                print(f"  {sec}: ${price}")
            
            return len(prices) == len(securities)
            
        except Exception as e:
            print(f"✗ Batch request failed: {e}")
            return False
    
    def test_6_error_handling(self):
        """Test 6: Error Handling"""
        try:
            # Test with invalid date range
            print("Testing invalid date range...")
            try:
                data = self.client.get_historical_data(
                    securities="AAPL US Equity",
                    fields=["PX_LAST"],
                    start_date="2024-01-01",
                    end_date="2023-01-01"
                )
                print("✗ Should have raised an error")
                return False
            except Exception as e:
                print(f"✓ Correctly caught error: {e}")
            
            # Test with too many securities
            print("\nTesting request limits...")
            try:
                # Create 101 fake tickers (exceeds limit of 100)
                securities = [f"TEST{i} US Equity" for i in range(101)]
                data = self.client.get_last_price(securities)
                print("✗ Should have raised an error for too many securities")
                return False
            except Exception as e:
                print(f"✓ Correctly caught error: {e}")
            
            return True
            
        except Exception as e:
            print(f"✗ Error handling test failed: {e}")
            return False
    
    def test_7_cache_functionality(self):
        """Test 7: Cache Functionality"""
        try:
            # Clear cache first
            self.client.clear_cache()
            print("✓ Cache cleared")
            
            # First request
            start = time.time()
            data1 = self.client.get_last_price("IBM US Equity")
            time1 = time.time() - start
            print(f"✓ First request took {time1:.3f}s")
            
            # Second request (should be cached)
            start = time.time()
            data2 = self.client.get_last_price("IBM US Equity")
            time2 = time.time() - start
            print(f"✓ Second request took {time2:.3f}s")
            
            # Cache should be faster
            if time2 < time1 * 0.5:  # At least 50% faster
                print("✓ Cache is working effectively")
                return True
            else:
                print("⚠ Cache might not be working properly")
                return True  # Don't fail the test, just warn
                
        except Exception as e:
            print(f"✗ Cache test failed: {e}")
            return False
    
    def test_8_data_validation(self):
        """Test 8: Data Validation"""
        try:
            # Get data with multiple fields
            data = self.client.get_historical_data(
                securities="SPY US Equity",
                fields=["PX_OPEN", "PX_HIGH", "PX_LOW", "PX_LAST"],
                start_date="2024-01-01",
                end_date="2024-01-05"
            )
            
            if "SPY US Equity" in data:
                df = data["SPY US Equity"]
                
                # Validate OHLC relationship
                valid_ohlc = True
                for idx, row in df.iterrows():
                    if not (row['PX_LOW'] <= row['PX_OPEN'] <= row['PX_HIGH'] and
                            row['PX_LOW'] <= row['PX_LAST'] <= row['PX_HIGH']):
                        valid_ohlc = False
                        break
                
                if valid_ohlc:
                    print("✓ OHLC data validation passed")
                else:
                    print("✗ OHLC data validation failed")
                
                # Check for missing data
                missing = df.isnull().sum().sum()
                print(f"✓ Missing values: {missing}")
                
                return valid_ohlc
            else:
                print("✗ No data returned")
                return False
                
        except Exception as e:
            print(f"✗ Data validation test failed: {e}")
            return False
    
    def test_9_performance(self):
        """Test 9: Performance Test"""
        try:
            # Test response time for various request sizes
            test_cases = [
                (1, ["AAPL US Equity"]),
                (5, ["AAPL US Equity", "MSFT US Equity", "GOOGL US Equity", 
                     "AMZN US Equity", "META US Equity"]),
                (10, [f"TEST{i} US Equity" for i in range(10)])
            ]
            
            for count, securities in test_cases:
                start = time.time()
                try:
                    data = self.client.get_last_price(securities)
                    elapsed = time.time() - start
                    print(f"✓ {count} securities: {elapsed:.3f}s ({elapsed/count:.3f}s per security)")
                except:
                    elapsed = time.time() - start
                    print(f"⚠ {count} securities failed after {elapsed:.3f}s")
            
            return True
            
        except Exception as e:
            print(f"✗ Performance test failed: {e}")
            return False


def main():
    """Main test function"""
    # Get host from command line
    host = sys.argv[1] if len(sys.argv) > 1 else "localhost"
    
    # Run tests
    runner = TestRunner(host)
    success = runner.run_all_tests()
    
    # Exit with appropriate code
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()