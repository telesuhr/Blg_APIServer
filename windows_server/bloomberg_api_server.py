#!/usr/bin/env python3
"""
Bloomberg API Server for Windows
Provides REST API access to Bloomberg Terminal data
"""

from fastapi import FastAPI, HTTPException, Depends, Header
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, validator
from typing import List, Optional, Dict, Any, Union
from datetime import date, datetime, timedelta
import logging
import json
import time
import random
import numpy as np
import pandas as pd
from collections import OrderedDict
import psutil
import traceback

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

# Try to import blpapi, fall back to mock mode if not available
try:
    import blpapi
    BLOOMBERG_AVAILABLE = True
    logger.info("Bloomberg API (blpapi) is available")
except ImportError:
    BLOOMBERG_AVAILABLE = False
    logger.warning("Bloomberg API (blpapi) not available - running in mock mode")

# Initialize FastAPI app
app = FastAPI(
    title="Bloomberg API Bridge",
    description="REST API for accessing Bloomberg Terminal data",
    version="1.0.0"
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=config.ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Request models
class HistoricalDataRequest(BaseModel):
    securities: List[str]
    fields: List[str]
    start_date: Union[str, date]
    end_date: Union[str, date]
    
    @validator('securities')
    def validate_securities(cls, v):
        if len(v) > config.MAX_SECURITIES_PER_REQUEST:
            raise ValueError(f"Maximum {config.MAX_SECURITIES_PER_REQUEST} securities per request")
        return v
    
    @validator('fields')
    def validate_fields(cls, v):
        if len(v) > config.MAX_FIELDS_PER_REQUEST:
            raise ValueError(f"Maximum {config.MAX_FIELDS_PER_REQUEST} fields per request")
        return v
    
    @validator('start_date', 'end_date', pre=True)
    def parse_date(cls, v):
        if isinstance(v, str):
            return datetime.strptime(v, '%Y-%m-%d').date()
        return v

class ReferenceDataRequest(BaseModel):
    securities: List[str]
    fields: List[str]
    
    @validator('securities')
    def validate_securities(cls, v):
        if len(v) > config.MAX_SECURITIES_PER_REQUEST:
            raise ValueError(f"Maximum {config.MAX_SECURITIES_PER_REQUEST} securities per request")
        return v
    
    @validator('fields')
    def validate_fields(cls, v):
        if len(v) > config.MAX_FIELDS_PER_REQUEST:
            raise ValueError(f"Maximum {config.MAX_FIELDS_PER_REQUEST} fields per request")
        return v

class IntradayDataRequest(BaseModel):
    security: str
    start_datetime: datetime
    end_datetime: datetime
    interval: Optional[int] = 1  # minutes

# Cache implementation
class SimpleCache:
    def __init__(self, max_size=1000, ttl_seconds=300):
        self.cache = OrderedDict()
        self.max_size = max_size
        self.ttl_seconds = ttl_seconds
    
    def get(self, key):
        if key in self.cache:
            value, timestamp = self.cache[key]
            if time.time() - timestamp < self.ttl_seconds:
                # Move to end (LRU)
                self.cache.move_to_end(key)
                return value
            else:
                del self.cache[key]
        return None
    
    def set(self, key, value):
        self.cache[key] = (value, time.time())
        if len(self.cache) > self.max_size:
            # Remove oldest
            self.cache.popitem(last=False)

# Initialize cache
cache = SimpleCache(
    max_size=config.MAX_CACHE_SIZE,
    ttl_seconds=config.CACHE_TTL_SECONDS
)

# Bloomberg connection manager
class BloombergConnection:
    def __init__(self):
        self.session = None
        self.service = None
        self.is_mock = not BLOOMBERG_AVAILABLE or not self._check_bbcomm_running()
        
        if self.is_mock:
            logger.info("Running in mock mode")
        else:
            self._connect()
    
    def _check_bbcomm_running(self):
        """Check if Bloomberg Terminal (bbcomm.exe) is running"""
        for proc in psutil.process_iter(['name']):
            if proc.info['name'] and 'bbcomm' in proc.info['name'].lower():
                return True
        return False
    
    def _connect(self):
        """Connect to Bloomberg API"""
        try:
            sessionOptions = blpapi.SessionOptions()
            sessionOptions.setServerHost(config.BLOOMBERG_HOST)
            sessionOptions.setServerPort(config.BLOOMBERG_PORT)
            
            self.session = blpapi.Session(sessionOptions)
            if not self.session.start():
                raise Exception("Failed to start Bloomberg session")
            
            if not self.session.openService("//blp/refdata"):
                raise Exception("Failed to open Bloomberg service")
            
            self.service = self.session.getService("//blp/refdata")
            logger.info("Successfully connected to Bloomberg API")
            
        except Exception as e:
            logger.error(f"Failed to connect to Bloomberg: {e}")
            self.is_mock = True
    
    def get_historical_data(self, securities, fields, start_date, end_date):
        """Get historical data from Bloomberg or mock"""
        cache_key = f"hist_{json.dumps(securities)}_{json.dumps(fields)}_{start_date}_{end_date}"
        cached_data = cache.get(cache_key)
        if cached_data:
            logger.info(f"Returning cached data for {cache_key}")
            return cached_data
        
        if self.is_mock:
            data = self._generate_mock_historical_data(securities, fields, start_date, end_date)
        else:
            data = self._fetch_historical_data(securities, fields, start_date, end_date)
        
        cache.set(cache_key, data)
        return data
    
    def _fetch_historical_data(self, securities, fields, start_date, end_date):
        """Fetch real data from Bloomberg"""
        try:
            request = self.service.createRequest("HistoricalDataRequest")
            
            for security in securities:
                request.append("securities", security)
            
            for field in fields:
                request.append("fields", field)
            
            request.set("startDate", start_date.strftime("%Y%m%d"))
            request.set("endDate", end_date.strftime("%Y%m%d"))
            request.set("periodicitySelection", "DAILY")
            
            self.session.sendRequest(request)
            
            results = {}
            while True:
                event = self.session.nextEvent(config.BLOOMBERG_TIMEOUT_MS)
                
                for msg in event:
                    if msg.hasElement("securityData"):
                        security_data = msg.getElement("securityData")
                        security = security_data.getElementAsString("security")
                        field_data_array = security_data.getElement("fieldData")
                        
                        data_points = []
                        for i in range(field_data_array.numValues()):
                            field_data = field_data_array.getValue(i)
                            point = {
                                "date": field_data.getElementAsDatetime("date").date().isoformat()
                            }
                            
                            for field in fields:
                                if field_data.hasElement(field):
                                    point[field] = field_data.getElementAsFloat(field)
                                else:
                                    point[field] = None
                            
                            data_points.append(point)
                        
                        results[security] = data_points
                
                if event.eventType() == blpapi.Event.RESPONSE:
                    break
            
            return {"status": "success", "data": results}
            
        except Exception as e:
            logger.error(f"Error fetching historical data: {e}")
            raise HTTPException(status_code=500, detail=str(e))
    
    def _generate_mock_historical_data(self, securities, fields, start_date, end_date):
        """Generate realistic mock data for testing"""
        results = {}
        
        # Base prices for different security types
        base_prices = {
            "AAPL US Equity": 150.0,
            "MSFT US Equity": 350.0,
            "LMCADY Index": 8500.0,
            "HG1 Comdty": 4.2,
            "JPY Curncy": 145.0,
            "EUR Curncy": 1.08
        }
        
        for security in securities:
            # Determine base price
            base_price = base_prices.get(security, 100.0)
            if "Equity" in security:
                base_price = base_prices.get(security, 100.0)
            elif "Index" in security:
                base_price = base_prices.get(security, 5000.0)
            elif "Comdty" in security:
                base_price = base_prices.get(security, 3.0)
            elif "Curncy" in security:
                base_price = base_prices.get(security, 1.0)
            
            # Generate date range
            date_range = pd.date_range(start=start_date, end=end_date, freq='B')  # Business days
            
            # Generate price path
            returns = np.random.normal(0.0002, config.MOCK_DATA_VARIANCE, len(date_range))
            price_path = base_price * np.exp(np.cumsum(returns))
            
            # Generate data points
            data_points = []
            for i, d in enumerate(date_range):
                point = {"date": d.date().isoformat()}
                
                for field in fields:
                    if field == "PX_LAST" or field == "LAST_PRICE":
                        point[field] = round(price_path[i], 2)
                    elif field == "PX_OPEN":
                        point[field] = round(price_path[i] * (1 + np.random.uniform(-0.005, 0.005)), 2)
                    elif field == "PX_HIGH":
                        point[field] = round(price_path[i] * (1 + np.random.uniform(0, 0.01)), 2)
                    elif field == "PX_LOW":
                        point[field] = round(price_path[i] * (1 - np.random.uniform(0, 0.01)), 2)
                    elif field == "VOLUME":
                        base_volume = 10000000 if "Equity" in security else 100000
                        point[field] = int(base_volume * np.random.uniform(0.5, 1.5))
                    elif field == "OPEN_INT":
                        point[field] = int(50000 * np.random.uniform(0.8, 1.2))
                    else:
                        point[field] = None
                
                data_points.append(point)
            
            results[security] = data_points
        
        return {"status": "success", "data": results, "is_mock": True}
    
    def get_reference_data(self, securities, fields):
        """Get reference data from Bloomberg or mock"""
        cache_key = f"ref_{json.dumps(securities)}_{json.dumps(fields)}"
        cached_data = cache.get(cache_key)
        if cached_data:
            return cached_data
        
        if self.is_mock:
            data = self._generate_mock_reference_data(securities, fields)
        else:
            data = self._fetch_reference_data(securities, fields)
        
        cache.set(cache_key, data)
        return data
    
    def _fetch_reference_data(self, securities, fields):
        """Fetch real reference data from Bloomberg"""
        try:
            request = self.service.createRequest("ReferenceDataRequest")
            
            for security in securities:
                request.append("securities", security)
            
            for field in fields:
                request.append("fields", field)
            
            self.session.sendRequest(request)
            
            results = {}
            while True:
                event = self.session.nextEvent(config.BLOOMBERG_TIMEOUT_MS)
                
                for msg in event:
                    if msg.hasElement("securityData"):
                        security_data_array = msg.getElement("securityData")
                        
                        for i in range(security_data_array.numValues()):
                            security_data = security_data_array.getValue(i)
                            security = security_data.getElementAsString("security")
                            field_data = security_data.getElement("fieldData")
                            
                            data = {}
                            for field in fields:
                                if field_data.hasElement(field):
                                    element = field_data.getElement(field)
                                    # Handle different data types
                                    if element.datatype() == blpapi.DataType.FLOAT64:
                                        data[field] = element.getValueAsFloat()
                                    elif element.datatype() == blpapi.DataType.INT32:
                                        data[field] = element.getValueAsInteger()
                                    else:
                                        data[field] = element.getValueAsString()
                                else:
                                    data[field] = None
                            
                            results[security] = data
                
                if event.eventType() == blpapi.Event.RESPONSE:
                    break
            
            return {"status": "success", "data": results}
            
        except Exception as e:
            logger.error(f"Error fetching reference data: {e}")
            raise HTTPException(status_code=500, detail=str(e))
    
    def _generate_mock_reference_data(self, securities, fields):
        """Generate mock reference data"""
        results = {}
        
        for security in securities:
            data = {}
            for field in fields:
                if field == "NAME":
                    data[field] = security.split()[0] + " Corporation"
                elif field == "COUNTRY":
                    data[field] = "US"
                elif field == "CURRENCY":
                    data[field] = "USD"
                elif field == "EXCHANGE":
                    data[field] = "NYSE"
                elif field == "INDUSTRY_SECTOR":
                    data[field] = "Technology"
                elif field == "PX_LAST":
                    data[field] = round(random.uniform(50, 500), 2)
                elif field == "CUR_MKT_CAP":
                    data[field] = round(random.uniform(100, 2000) * 1e9, 0)
                elif field == "PE_RATIO":
                    data[field] = round(random.uniform(15, 35), 2)
                else:
                    data[field] = f"Mock {field}"
            
            results[security] = data
        
        return {"status": "success", "data": results, "is_mock": True}

# Initialize Bloomberg connection
bloomberg = BloombergConnection()

# API authentication
async def verify_api_key(api_key: str = Header(...)):
    """Verify API key"""
    if api_key != config.API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API key")
    return api_key

# API endpoints
@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "service": "Bloomberg API Bridge",
        "version": "1.0.0",
        "status": "running",
        "mode": "mock" if bloomberg.is_mock else "live",
        "endpoints": [
            "/health",
            "/historical_data",
            "/reference_data",
            "/intraday_data"
        ]
    }

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    bbcomm_running = bloomberg._check_bbcomm_running() if BLOOMBERG_AVAILABLE else False
    
    return {
        "status": "healthy",
        "bloomberg_available": BLOOMBERG_AVAILABLE,
        "bbcomm_running": bbcomm_running,
        "mode": "mock" if bloomberg.is_mock else "live",
        "timestamp": datetime.now().isoformat(),
        "cache_size": len(cache.cache)
    }

@app.post("/historical_data")
async def get_historical_data(
    request: HistoricalDataRequest,
    api_key: str = Depends(verify_api_key)
):
    """Get historical market data"""
    logger.info(f"Historical data request: {request.securities} from {request.start_date} to {request.end_date}")
    
    try:
        # Validate date range
        if request.end_date < request.start_date:
            raise HTTPException(status_code=400, detail="End date must be after start date")
        
        date_diff = (request.end_date - request.start_date).days
        if date_diff > config.MAX_DATE_RANGE_DAYS:
            raise HTTPException(
                status_code=400,
                detail=f"Date range too large. Maximum {config.MAX_DATE_RANGE_DAYS} days"
            )
        
        # Get data
        result = bloomberg.get_historical_data(
            request.securities,
            request.fields,
            request.start_date,
            request.end_date
        )
        
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in historical_data endpoint: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/reference_data")
async def get_reference_data(
    request: ReferenceDataRequest,
    api_key: str = Depends(verify_api_key)
):
    """Get reference/static data"""
    logger.info(f"Reference data request: {request.securities}")
    
    try:
        result = bloomberg.get_reference_data(
            request.securities,
            request.fields
        )
        
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in reference_data endpoint: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/intraday_data")
async def get_intraday_data(
    request: IntradayDataRequest,
    api_key: str = Depends(verify_api_key)
):
    """Get intraday data (mock only for now)"""
    logger.info(f"Intraday data request: {request.security}")
    
    # For now, return mock data
    # Real implementation would use IntradayBarRequest
    
    time_range = pd.date_range(
        start=request.start_datetime,
        end=request.end_datetime,
        freq=f'{request.interval}T'
    )
    
    base_price = 100.0
    data_points = []
    
    for timestamp in time_range:
        price = base_price * (1 + random.uniform(-0.001, 0.001))
        data_points.append({
            "time": timestamp.isoformat(),
            "open": round(price, 2),
            "high": round(price * 1.001, 2),
            "low": round(price * 0.999, 2),
            "close": round(price * (1 + random.uniform(-0.0005, 0.0005)), 2),
            "volume": random.randint(1000, 10000)
        })
    
    return {
        "status": "success",
        "data": {request.security: data_points},
        "is_mock": True
    }

# Run the server
if __name__ == "__main__":
    import uvicorn
    
    logger.info(f"Starting Bloomberg API Bridge on {config.API_HOST}:{config.API_PORT}")
    logger.info(f"Mode: {'Mock' if bloomberg.is_mock else 'Live'}")
    
    uvicorn.run(
        app,
        host=config.API_HOST,
        port=config.API_PORT,
        log_level=config.LOG_LEVEL.lower()
    )