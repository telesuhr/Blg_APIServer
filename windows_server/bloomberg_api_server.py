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
from collections import OrderedDict
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

# Import blpapi - REQUIRED
try:
    import blpapi
    logger.info("Bloomberg API (blpapi) loaded successfully")
except ImportError:
    logger.error("CRITICAL: Bloomberg API (blpapi) not available!")
    logger.error("Please install: pip install blpapi")
    raise ImportError("Bloomberg API (blpapi) is required. Please install: pip install blpapi")

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
        self._connect()
    
    def _connect(self):
        """Connect to Bloomberg API"""
        try:
            sessionOptions = blpapi.SessionOptions()
            sessionOptions.setServerHost(config.BLOOMBERG_HOST)
            sessionOptions.setServerPort(config.BLOOMBERG_PORT)
            
            self.session = blpapi.Session(sessionOptions)
            if not self.session.start():
                raise Exception("Failed to start Bloomberg session - ensure Bloomberg Terminal is running")
            
            if not self.session.openService("//blp/refdata"):
                raise Exception("Failed to open Bloomberg service")
            
            self.service = self.session.getService("//blp/refdata")
            logger.info("Successfully connected to Bloomberg API")
            
        except Exception as e:
            logger.error(f"Failed to connect to Bloomberg: {e}")
            raise Exception(f"Bloomberg connection failed: {e}")
    
    def get_historical_data(self, securities, fields, start_date, end_date):
        """Get historical data from Bloomberg"""
        cache_key = f"hist_{json.dumps(securities)}_{json.dumps(fields)}_{start_date}_{end_date}"
        cached_data = cache.get(cache_key)
        if cached_data:
            logger.info(f"Returning cached data for {cache_key}")
            return cached_data
        
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
                        
                        if security_data.hasElement("fieldData"):
                            field_data_array = security_data.getElement("fieldData")
                            
                            data_points = []
                            for i in range(field_data_array.numValues()):
                                field_data = field_data_array.getValue(i)
                                point = {
                                    "date": field_data.getElementAsDatetime("date").date().isoformat()
                                }
                                
                                for field in fields:
                                    if field_data.hasElement(field):
                                        element = field_data.getElement(field)
                                        if element.datatype() == blpapi.DataType.FLOAT64:
                                            point[field] = element.getValueAsFloat()
                                        elif element.datatype() == blpapi.DataType.INT32:
                                            point[field] = element.getValueAsInteger()
                                        elif element.datatype() == blpapi.DataType.INT64:
                                            point[field] = element.getValueAsInteger()
                                        else:
                                            point[field] = element.getValueAsString()
                                    else:
                                        point[field] = None
                                
                                data_points.append(point)
                            
                            results[security] = data_points
                        else:
                            # No data available
                            results[security] = []
                
                if event.eventType() == blpapi.Event.RESPONSE:
                    break
            
            return {"status": "success", "data": results}
            
        except Exception as e:
            logger.error(f"Error fetching historical data: {e}")
            raise HTTPException(status_code=500, detail=str(e))
    
    def get_reference_data(self, securities, fields):
        """Get reference data from Bloomberg"""
        cache_key = f"ref_{json.dumps(securities)}_{json.dumps(fields)}"
        cached_data = cache.get(cache_key)
        if cached_data:
            return cached_data
        
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
                            
                            if security_data.hasElement("fieldData"):
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
                                        elif element.datatype() == blpapi.DataType.INT64:
                                            data[field] = element.getValueAsInteger()
                                        elif element.datatype() == blpapi.DataType.STRING:
                                            data[field] = element.getValueAsString()
                                        elif element.datatype() == blpapi.DataType.DATE:
                                            data[field] = element.getValueAsDatetime().date().isoformat()
                                        elif element.datatype() == blpapi.DataType.DATETIME:
                                            data[field] = element.getValueAsDatetime().isoformat()
                                        else:
                                            data[field] = str(element.getValue())
                                    else:
                                        data[field] = None
                                
                                results[security] = data
                            else:
                                results[security] = {field: None for field in fields}
                
                if event.eventType() == blpapi.Event.RESPONSE:
                    break
            
            return {"status": "success", "data": results}
            
        except Exception as e:
            logger.error(f"Error fetching reference data: {e}")
            raise HTTPException(status_code=500, detail=str(e))

    def get_intraday_data(self, security, start_datetime, end_datetime, interval):
        """Get intraday bar data from Bloomberg"""
        try:
            request = self.service.createRequest("IntradayBarRequest")
            
            request.set("security", security)
            request.set("eventType", "TRADE")
            request.set("interval", interval)
            request.set("startDateTime", start_datetime)
            request.set("endDateTime", end_datetime)
            
            self.session.sendRequest(request)
            
            data_points = []
            while True:
                event = self.session.nextEvent(config.BLOOMBERG_TIMEOUT_MS)
                
                for msg in event:
                    if msg.hasElement("barData"):
                        bar_data = msg.getElement("barData").getElement("barTickData")
                        
                        for i in range(bar_data.numValues()):
                            bar = bar_data.getValue(i)
                            data_points.append({
                                "time": bar.getElementAsDatetime("time").isoformat(),
                                "open": bar.getElementAsFloat("open"),
                                "high": bar.getElementAsFloat("high"),
                                "low": bar.getElementAsFloat("low"),
                                "close": bar.getElementAsFloat("close"),
                                "volume": bar.getElementAsInteger("volume"),
                                "numEvents": bar.getElementAsInteger("numEvents")
                            })
                
                if event.eventType() == blpapi.Event.RESPONSE:
                    break
            
            return {"status": "success", "data": {security: data_points}}
            
        except Exception as e:
            logger.error(f"Error fetching intraday data: {e}")
            raise HTTPException(status_code=500, detail=str(e))

# Initialize Bloomberg connection
try:
    bloomberg = BloombergConnection()
except Exception as e:
    logger.error(f"Failed to initialize Bloomberg connection: {e}")
    bloomberg = None

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
        "status": "running" if bloomberg else "error",
        "bloomberg_connected": bloomberg is not None,
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
    return {
        "status": "healthy" if bloomberg else "unhealthy",
        "bloomberg_connected": bloomberg is not None,
        "timestamp": datetime.now().isoformat(),
        "cache_size": len(cache.cache)
    }

@app.post("/historical_data")
async def get_historical_data(
    request: HistoricalDataRequest,
    api_key: str = Depends(verify_api_key)
):
    """Get historical market data"""
    if not bloomberg:
        raise HTTPException(status_code=503, detail="Bloomberg connection not available")
        
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
    if not bloomberg:
        raise HTTPException(status_code=503, detail="Bloomberg connection not available")
        
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
    """Get intraday bar data"""
    if not bloomberg:
        raise HTTPException(status_code=503, detail="Bloomberg connection not available")
        
    logger.info(f"Intraday data request: {request.security}")
    
    try:
        result = bloomberg.get_intraday_data(
            request.security,
            request.start_datetime,
            request.end_datetime,
            request.interval
        )
        
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in intraday_data endpoint: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=str(e))

# Run the server
if __name__ == "__main__":
    import uvicorn
    
    if not bloomberg:
        logger.error("Cannot start server - Bloomberg connection failed")
        logger.error("Please ensure:")
        logger.error("1. Bloomberg Terminal is running")
        logger.error("2. You are logged in to Bloomberg")
        logger.error("3. blpapi is installed: pip install blpapi")
        exit(1)
    
    logger.info(f"Starting Bloomberg API Bridge on {config.API_HOST}:{config.API_PORT}")
    
    uvicorn.run(
        app,
        host=config.API_HOST,
        port=config.API_PORT,
        log_level=config.LOG_LEVEL.lower()
    )