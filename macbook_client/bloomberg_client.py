#!/usr/bin/env python3
"""
Bloomberg Client for Macbook
Provides Python interface to Bloomberg API Bridge
"""

import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from typing import List, Dict, Any, Optional, Union
from datetime import date, datetime, timedelta
import pandas as pd
import json
import os
import logging
import hashlib
import pickle
from pathlib import Path
import time

# Import configuration
import config

# Configure logging
logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(config.LOG_FILE),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class BloombergCache:
    """Simple file-based cache for Bloomberg data"""
    
    def __init__(self, cache_dir: str = config.CACHE_DIR, ttl_minutes: int = config.CACHE_TTL_MINUTES):
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(exist_ok=True)
        self.ttl_seconds = ttl_minutes * 60
    
    def _get_cache_key(self, key_data: str) -> str:
        """Generate cache key from request data"""
        return hashlib.md5(key_data.encode()).hexdigest()
    
    def get(self, key_data: str) -> Optional[Any]:
        """Get data from cache"""
        if not config.ENABLE_CLIENT_CACHE:
            return None
            
        cache_key = self._get_cache_key(key_data)
        cache_file = self.cache_dir / f"{cache_key}.pkl"
        
        if cache_file.exists():
            try:
                with open(cache_file, 'rb') as f:
                    timestamp, data = pickle.load(f)
                
                if time.time() - timestamp < self.ttl_seconds:
                    logger.debug(f"Cache hit for {cache_key}")
                    return data
                else:
                    # Cache expired
                    cache_file.unlink()
            except Exception as e:
                logger.warning(f"Cache read error: {e}")
        
        return None
    
    def set(self, key_data: str, value: Any):
        """Store data in cache"""
        if not config.ENABLE_CLIENT_CACHE:
            return
            
        cache_key = self._get_cache_key(key_data)
        cache_file = self.cache_dir / f"{cache_key}.pkl"
        
        try:
            with open(cache_file, 'wb') as f:
                pickle.dump((time.time(), value), f)
            logger.debug(f"Cached data for {cache_key}")
        except Exception as e:
            logger.warning(f"Cache write error: {e}")
    
    def clear(self):
        """Clear all cache files"""
        for cache_file in self.cache_dir.glob("*.pkl"):
            try:
                cache_file.unlink()
            except Exception as e:
                logger.warning(f"Failed to delete cache file {cache_file}: {e}")
        logger.info("Cache cleared")


class BloombergClient:
    """Client for Bloomberg API Bridge"""
    
    def __init__(
        self,
        host: str = config.DEFAULT_SERVER_HOST,
        port: int = config.DEFAULT_SERVER_PORT,
        api_key: str = config.DEFAULT_API_KEY,
        use_cache: bool = config.ENABLE_CLIENT_CACHE
    ):
        """
        Initialize Bloomberg client
        
        Args:
            host: Server hostname or IP address
            port: Server port
            api_key: API key for authentication
            use_cache: Enable client-side caching
        """
        self.base_url = f"http://{host}:{port}"
        self.api_key = api_key
        self.cache = BloombergCache() if use_cache else None
        
        # Configure session with retry strategy
        self.session = requests.Session()
        retry_strategy = Retry(
            total=config.MAX_RETRIES,
            backoff_factor=config.RETRY_DELAY,
            status_forcelist=[429, 500, 502, 503, 504]
        )
        adapter = HTTPAdapter(
            max_retries=retry_strategy,
            pool_connections=config.CONNECTION_POOL_SIZE,
            pool_maxsize=config.CONNECTION_POOL_SIZE
        )
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        
        # Set default headers
        self.session.headers.update({
            'api-key': self.api_key,
            'Content-Type': 'application/json'
        })
        
        logger.info(f"Bloomberg client initialized for {self.base_url}")
    
    def check_connection(self) -> Dict[str, Any]:
        """Check connection to Bloomberg API server"""
        try:
            response = self.session.get(
                f"{self.base_url}/health",
                timeout=config.REQUEST_TIMEOUT
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Connection check failed: {e}")
            raise
    
    def get_historical_data(
        self,
        securities: Union[str, List[str]],
        fields: Union[str, List[str]],
        start_date: Union[str, date],
        end_date: Union[str, date],
        as_dataframe: bool = True
    ) -> Union[Dict[str, pd.DataFrame], Dict[str, List[Dict]]]:
        """
        Get historical market data
        
        Args:
            securities: Single security or list of securities
            fields: Single field or list of fields
            start_date: Start date (YYYY-MM-DD or date object)
            end_date: End date (YYYY-MM-DD or date object)
            as_dataframe: Return as pandas DataFrame (default) or raw data
            
        Returns:
            Dictionary mapping securities to their data
        """
        # Normalize inputs
        if isinstance(securities, str):
            securities = [securities]
        if isinstance(fields, str):
            fields = [fields]
        if isinstance(start_date, date):
            start_date = start_date.strftime('%Y-%m-%d')
        if isinstance(end_date, date):
            end_date = end_date.strftime('%Y-%m-%d')
        
        # Check cache
        cache_key = f"hist_{json.dumps(securities)}_{json.dumps(fields)}_{start_date}_{end_date}"
        if self.cache:
            cached_data = self.cache.get(cache_key)
            if cached_data:
                logger.info("Returning cached historical data")
                return cached_data
        
        # Make request
        payload = {
            "securities": securities,
            "fields": fields,
            "start_date": start_date,
            "end_date": end_date
        }
        
        try:
            response = self.session.post(
                f"{self.base_url}/historical_data",
                json=payload,
                timeout=config.REQUEST_TIMEOUT
            )
            response.raise_for_status()
            result = response.json()
            
            if result.get("status") != "success":
                raise Exception(f"API error: {result}")
            
            data = result["data"]
            
            # Convert to DataFrames if requested
            if as_dataframe:
                df_data = {}
                for security, points in data.items():
                    if points:
                        df = pd.DataFrame(points)
                        df['date'] = pd.to_datetime(df['date'])
                        df.set_index('date', inplace=True)
                        df_data[security] = df
                    else:
                        df_data[security] = pd.DataFrame()
                data = df_data
            
            # Cache result
            if self.cache:
                self.cache.set(cache_key, data)
            
            return data
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed: {e}")
            raise
        except Exception as e:
            logger.error(f"Error getting historical data: {e}")
            raise
    
    def get_reference_data(
        self,
        securities: Union[str, List[str]],
        fields: Union[str, List[str]]
    ) -> Dict[str, Dict[str, Any]]:
        """
        Get reference/static data
        
        Args:
            securities: Single security or list of securities
            fields: Single field or list of fields
            
        Returns:
            Dictionary mapping securities to their field values
        """
        # Normalize inputs
        if isinstance(securities, str):
            securities = [securities]
        if isinstance(fields, str):
            fields = [fields]
        
        # Check cache
        cache_key = f"ref_{json.dumps(securities)}_{json.dumps(fields)}"
        if self.cache:
            cached_data = self.cache.get(cache_key)
            if cached_data:
                logger.info("Returning cached reference data")
                return cached_data
        
        # Make request
        payload = {
            "securities": securities,
            "fields": fields
        }
        
        try:
            response = self.session.post(
                f"{self.base_url}/reference_data",
                json=payload,
                timeout=config.REQUEST_TIMEOUT
            )
            response.raise_for_status()
            result = response.json()
            
            if result.get("status") != "success":
                raise Exception(f"API error: {result}")
            
            data = result["data"]
            
            # Cache result
            if self.cache:
                self.cache.set(cache_key, data)
            
            return data
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed: {e}")
            raise
        except Exception as e:
            logger.error(f"Error getting reference data: {e}")
            raise
    
    def get_intraday_data(
        self,
        security: str,
        start_datetime: Union[str, datetime],
        end_datetime: Union[str, datetime],
        interval: int = 1,
        as_dataframe: bool = True
    ) -> Union[pd.DataFrame, List[Dict]]:
        """
        Get intraday data
        
        Args:
            security: Security identifier
            start_datetime: Start datetime
            end_datetime: End datetime
            interval: Bar interval in minutes (default: 1)
            as_dataframe: Return as pandas DataFrame (default) or raw data
            
        Returns:
            Intraday data as DataFrame or list of dictionaries
        """
        # Normalize inputs
        if isinstance(start_datetime, str):
            start_datetime = datetime.fromisoformat(start_datetime)
        if isinstance(end_datetime, str):
            end_datetime = datetime.fromisoformat(end_datetime)
        
        # Make request
        payload = {
            "security": security,
            "start_datetime": start_datetime.isoformat(),
            "end_datetime": end_datetime.isoformat(),
            "interval": interval
        }
        
        try:
            response = self.session.post(
                f"{self.base_url}/intraday_data",
                json=payload,
                timeout=config.REQUEST_TIMEOUT
            )
            response.raise_for_status()
            result = response.json()
            
            if result.get("status") != "success":
                raise Exception(f"API error: {result}")
            
            data = result["data"][security]
            
            # Convert to DataFrame if requested
            if as_dataframe and data:
                df = pd.DataFrame(data)
                df['time'] = pd.to_datetime(df['time'])
                df.set_index('time', inplace=True)
                return df
            
            return data
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed: {e}")
            raise
        except Exception as e:
            logger.error(f"Error getting intraday data: {e}")
            raise
    
    # Convenience methods
    
    def get_last_price(self, securities: Union[str, List[str]]) -> Dict[str, float]:
        """Get last price for securities"""
        data = self.get_reference_data(securities, "PX_LAST")
        return {sec: info.get("PX_LAST") for sec, info in data.items()}
    
    def get_company_info(self, securities: Union[str, List[str]]) -> Dict[str, Dict]:
        """Get basic company information"""
        fields = ["NAME", "COUNTRY", "INDUSTRY_SECTOR", "CUR_MKT_CAP", "PE_RATIO"]
        return self.get_reference_data(securities, fields)
    
    def get_price_history(
        self,
        security: str,
        days: int = 30,
        fields: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """Get price history for the last N days"""
        if fields is None:
            fields = ["PX_LAST", "VOLUME"]
        
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=days)
        
        data = self.get_historical_data(
            security,
            fields,
            start_date,
            end_date
        )
        
        return data.get(security, pd.DataFrame())
    
    # Export methods
    
    def export_to_csv(
        self,
        data: Union[pd.DataFrame, Dict[str, pd.DataFrame]],
        filename: str,
        encoding: str = config.DEFAULT_CSV_ENCODING
    ):
        """Export data to CSV file"""
        if isinstance(data, pd.DataFrame):
            data.to_csv(filename, encoding=encoding)
            logger.info(f"Exported data to {filename}")
        elif isinstance(data, dict):
            # Multiple securities - create separate files
            for security, df in data.items():
                if isinstance(df, pd.DataFrame) and not df.empty:
                    safe_name = security.replace(" ", "_").replace("/", "_")
                    file_path = f"{filename.rsplit('.', 1)[0]}_{safe_name}.csv"
                    df.to_csv(file_path, encoding=encoding)
                    logger.info(f"Exported {security} data to {file_path}")
    
    def export_to_excel(
        self,
        data: Union[pd.DataFrame, Dict[str, pd.DataFrame]],
        filename: str
    ):
        """Export data to Excel file"""
        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            if isinstance(data, pd.DataFrame):
                data.to_excel(writer, sheet_name='Data')
            elif isinstance(data, dict):
                for security, df in data.items():
                    if isinstance(df, pd.DataFrame) and not df.empty:
                        # Excel sheet names have limitations
                        sheet_name = security[:31].replace(":", "").replace("/", "_")
                        df.to_excel(writer, sheet_name=sheet_name)
        
        logger.info(f"Exported data to {filename}")
    
    def export_to_json(
        self,
        data: Any,
        filename: str,
        orient: str = 'records'
    ):
        """Export data to JSON file"""
        def default_converter(obj):
            if isinstance(obj, (datetime, date)):
                return obj.isoformat()
            elif isinstance(obj, pd.DataFrame):
                return obj.to_dict(orient=orient)
            raise TypeError(f"Object of type {type(obj)} is not JSON serializable")
        
        with open(filename, 'w') as f:
            json.dump(data, f, indent=2, default=default_converter)
        
        logger.info(f"Exported data to {filename}")
    
    def clear_cache(self):
        """Clear client-side cache"""
        if self.cache:
            self.cache.clear()
        else:
            logger.info("Cache is not enabled")


# Convenience function for quick setup
def create_client(host: str = None, api_key: str = None) -> BloombergClient:
    """
    Create Bloomberg client with optional host override
    
    Args:
        host: Server hostname or IP (uses config default if None)
        api_key: API key (uses config default if None)
        
    Returns:
        BloombergClient instance
    """
    if host and ':' in host:
        # Handle host:port format
        host_parts = host.split(':')
        return BloombergClient(
            host=host_parts[0],
            port=int(host_parts[1]),
            api_key=api_key or config.DEFAULT_API_KEY
        )
    else:
        return BloombergClient(
            host=host or config.DEFAULT_SERVER_HOST,
            api_key=api_key or config.DEFAULT_API_KEY
        )