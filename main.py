from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import List, Optional
import json
from datetime import datetime, timedelta
import logging
import os
import glob

# Import our scraping functions
from scrap_tbm import extract_mygap_tbm_data, DATA_FIELDS as TBM_DATA_FIELDS
from scrap_pf import extract_mygap_pf_data, DATA_FIELDS as PF_DATA_FIELDS
from scrap_am import extract_mygap_am_data, DATA_FIELDS as AM_DATA_FIELDS
from scrap_my_organic import extract_mygap_organic_data, DATA_FIELDS as ORGANIC_DATA_FIELDS
from scrap_tanaman import extract_mygap_tanaman_data, DATA_FIELDS as TANAMAN_DATA_FIELDS

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize FastAPI app
app = FastAPI(
    title="MyGAP Data Scraper API",
    description="API to fetch Malaysian Good Agricultural Practice (MyGAP) certification data",
    version="1.0.0"
)

# Pydantic models for API responses
class MyGAPRecord(BaseModel):
    no_pensijilan: Optional[str] = None          # Certification Number
    # PF-specific fields
    projek: Optional[str] = None                 # Project 
    jenis_tanaman: Optional[str] = None          # Plant Type 
    # AM-specific fields  
    jenis_tanaman: Optional[str] = None            # Bee Type 
    bil_haif: Optional[str] = None               # Number of Hives
    # Common fields
    nama: Optional[str] = None                   # Name
    negeri: Optional[str] = None                 # State
    daerah: Optional[str] = None                 # District
    kategori_komoditi: Optional[str] = None      # Commodity Category
    kategori_tanaman: Optional[str] = None       # Plant Category
    luas_ladang: Optional[str] = None            # Farm Area 
    tahun_pensijilan: Optional[str] = None       # Certification Year
    tarikh_pensijilan: Optional[str] = None      # Certification Date
    tempoh_sah_laku: Optional[str] = None        # Validity Period/Expiry Date

class MyGAPResponse(BaseModel):
    success: bool
    message: str
    total_records: int
    timestamp: str
    data: List[MyGAPRecord]

class FieldStats(BaseModel):
    field_name: str
    completed_count: int
    total_count: int
    completion_percentage: float

class StatsResponse(BaseModel):
    success: bool
    message: str
    total_records: int
    timestamp: str
    field_statistics: List[FieldStats]

@app.get("/")
async def root():
    """Root endpoint with API information"""
    return {
        "message": "MyGAP Data Scraper API",
        "version": "1.0.0",
        "endpoints": {
            "/mygap/data/pf": "Fetch MyGAP Plant & Fresh certification data",
            "/mygap/data/am": "Fetch MyGAP Apiary Management certification data",
            "/mygap/data/organic": "Fetch MyGAP Organic certification data",
            "/mygap/data/tanaman": "Fetch MyGAP Tanaman certification data",
            "/mygap/stats": "Get statistics about the data",
            "/docs": "API documentation (Swagger UI)",
            "/redoc": "API documentation (ReDoc)"
        }
    }

@app.get("/mygap/data/tbm", response_model=MyGAPResponse)
async def get_mygap_data():
    """
    Fetch MyGAP certification data - reads from JSON file first, 
    only fetches new data if file is older than 1 day
    
    Returns:
        MyGAPResponse: Complete dataset with all certification records
    """
    try:
        # First try to read from existing JSON file
        raw_data = None
        data_source = "cache"
        
        # Find the most recent JSON file
        json_files = glob.glob("mygap_data_tbm*.json")
        if json_files:
            # Sort by modification time, get the newest
            latest_file = max(json_files, key=os.path.getmtime)
            file_mtime = datetime.fromtimestamp(os.path.getmtime(latest_file))
            file_age = datetime.now() - file_mtime
            
            logger.info(f"Found existing file: {latest_file}, age: {file_age}")
            
            # If file is less than 1 day old, read from it
            if file_age < timedelta(days=1):
                try:
                    with open(latest_file, 'r', encoding='utf-8') as f:
                        file_data = json.load(f)
                        if isinstance(file_data, list):
                            raw_data = file_data
                        elif isinstance(file_data, dict) and 'data' in file_data:
                            raw_data = file_data['data']
                        else:
                            raw_data = file_data
                    logger.info(f"Successfully loaded {len(raw_data) if raw_data else 0} records from cache")
                except Exception as e:
                    logger.warning(f"Failed to read from cache file: {str(e)}")
                    raw_data = None
            else:
                logger.info(f"File is older than 1 day ({file_age}), fetching fresh data")
        
        # If no valid cached data, extract from website
        if raw_data is None:
            logger.info("Fetching fresh data from MyGAP website...")
            raw_data = extract_mygap_tbm_data(save_to_file=True)  # Save fresh data to file
            data_source = "fresh"
            
            if raw_data is None:
                logger.error("Failed to extract data from MyGAP website")
                raise HTTPException(
                    status_code=500, 
                    detail="Failed to extract data from MyGAP website. The website might be unavailable."
                )
        
        # Convert raw data to Pydantic models
        records = []
        for item in raw_data:
            record = MyGAPRecord(**item)
            records.append(record)
        
        message = f"Successfully loaded {len(records)} MyGAP certification records from {data_source}"
        response = MyGAPResponse(
            success=True,
            message=message,
            total_records=len(records),
            timestamp=datetime.now().isoformat(),
            data=records
        )
        
        logger.info(message)
        return response
        
    except Exception as e:
        logger.error(f"Error loading MyGAP data: {str(e)}")
        raise HTTPException(
            status_code=500, 
            detail=f"Internal server error: {str(e)}"
        )
    
@app.get("/mygap/data/pf", response_model=MyGAPResponse)
async def get_mygap_pf_data():
    """
    Fetch MyGAP PF (Plantation Forestry) certification data - reads from JSON file first, 
    only fetches new data if file is older than 1 day
    
    Returns:
        MyGAPResponse: Complete dataset with all certification records
    """
    try:
        # First try to read from existing JSON file
        raw_data = None
        data_source = "cache"
        
        # Find the most recent JSON file
        json_files = glob.glob("mygap_data_pf*.json")
        if json_files:
            # Sort by modification time, get the newest
            latest_file = max(json_files, key=os.path.getmtime)
            file_mtime = datetime.fromtimestamp(os.path.getmtime(latest_file))
            file_age = datetime.now() - file_mtime
            
            logger.info(f"Found existing file: {latest_file}, age: {file_age}")
            
            # If file is less than 1 day old, read from it
            if file_age < timedelta(days=1):
                try:
                    with open(latest_file, 'r', encoding='utf-8') as f:
                        file_data = json.load(f)
                        if isinstance(file_data, list):
                            raw_data = file_data
                        elif isinstance(file_data, dict) and 'data' in file_data:
                            raw_data = file_data['data']
                        else:
                            raw_data = file_data
                    logger.info(f"Successfully loaded {len(raw_data) if raw_data else 0} records from cache")
                except Exception as e:
                    logger.warning(f"Failed to read from cache file: {str(e)}")
                    raw_data = None
            else:
                logger.info(f"File is older than 1 day ({file_age}), fetching fresh data")
        
        # If no valid cached data, extract from website
        if raw_data is None:
            logger.info("Fetching fresh data from MyGAP website...")
            raw_data = extract_mygap_pf_data(save_to_file=True)  # Save fresh data to file
            data_source = "fresh"
            
            if raw_data is None:
                logger.error("Failed to extract data from MyGAP website")
                raise HTTPException(
                    status_code=500, 
                    detail="Failed to extract data from MyGAP website. The website might be unavailable."
                )
        
        # Convert raw data to Pydantic models
        records = []
        for item in raw_data:
            record = MyGAPRecord(**item)
            records.append(record)
        
        message = f"Successfully loaded {len(records)} MyGAP certification records from {data_source}"
        response = MyGAPResponse(
            success=True,
            message=message,
            total_records=len(records),
            timestamp=datetime.now().isoformat(),
            data=records
        )
        
        logger.info(message)
        return response
        
    except Exception as e:
        logger.error(f"Error loading MyGAP data: {str(e)}")
        raise HTTPException(
            status_code=500, 
            detail=f"Internal server error: {str(e)}"
        )

@app.get("/mygap/data/am", response_model=MyGAPResponse)
async def get_mygap_am_data():
    """
    Fetch MyGAP AM (Apiary Management) certification data - reads from JSON file first, 
    only fetches new data if file is older than 1 day
    
    Returns:
        MyGAPResponse: Complete dataset with all AM certification records
    """
    try:
        # First try to read from existing JSON file
        raw_data = None
        data_source = "cache"
        
        # Find the most recent AM JSON file
        json_files = glob.glob("mygap_data_am_*.json")
        if json_files:
            # Sort by modification time, get the newest
            latest_file = max(json_files, key=os.path.getmtime)
            file_mtime = datetime.fromtimestamp(os.path.getmtime(latest_file))
            file_age = datetime.now() - file_mtime
            
            logger.info(f"Found existing AM file: {latest_file}, age: {file_age}")
            
            # If file is less than 1 day old, read from it
            if file_age < timedelta(days=1):
                try:
                    with open(latest_file, 'r', encoding='utf-8') as f:
                        file_data = json.load(f)
                        if isinstance(file_data, list):
                            raw_data = file_data
                        elif isinstance(file_data, dict) and 'data' in file_data:
                            raw_data = file_data['data']
                        else:
                            raw_data = file_data
                    logger.info(f"Successfully loaded {len(raw_data) if raw_data else 0} AM records from cache")
                except Exception as e:
                    logger.warning(f"Failed to read from AM cache file: {str(e)}")
                    raw_data = None
            else:
                logger.info(f"AM file is older than 1 day ({file_age}), fetching fresh data")
        
        # If no valid cached data, extract from website
        if raw_data is None:
            logger.info("Fetching fresh AM data from MyGAP website...")
            raw_data = extract_mygap_am_data(save_to_file=True)  # Save fresh data to file
            data_source = "fresh"
            
            if raw_data is None:
                logger.error("Failed to extract AM data from MyGAP website")
                raise HTTPException(
                    status_code=500, 
                    detail="Failed to extract AM data from MyGAP website. The website might be unavailable."
                )
        
        # Convert raw data to Pydantic models
        records = []
        for item in raw_data:
            record = MyGAPRecord(**item)
            records.append(record)
        
        message = f"Successfully loaded {len(records)} MyGAP AM certification records from {data_source}"
        response = MyGAPResponse(
            success=True,
            message=message,
            total_records=len(records),
            timestamp=datetime.now().isoformat(),
            data=records
        )
        
        logger.info(message)
        return response
        
    except Exception as e:
        logger.error(f"Error loading MyGAP AM data: {str(e)}")
        raise HTTPException(
            status_code=500, 
            detail=f"Internal server error: {str(e)}"
        )

@app.get("/mygap/data/organic", response_model=MyGAPResponse)
async def get_mygap_organic_data():
    """
    Fetch MyGAP Organic certification data - reads from JSON file first, 
    only fetches new data if file is older than 1 day
    
    Returns:
        MyGAPResponse: Complete dataset with all Organic certification records
    """
    try:
        # First try to read from existing JSON file
        raw_data = None
        data_source = "cache"
        
        # Find the most recent Organic JSON file
        json_files = glob.glob("mygap_data_organic_*.json")
        if json_files:
            # Sort by modification time, get the newest
            latest_file = max(json_files, key=os.path.getmtime)
            file_mtime = datetime.fromtimestamp(os.path.getmtime(latest_file))
            file_age = datetime.now() - file_mtime
            
            logger.info(f"Found existing Organic file: {latest_file}, age: {file_age}")
            
            # If file is less than 1 day old, read from it
            if file_age < timedelta(days=1):
                try:
                    with open(latest_file, 'r', encoding='utf-8') as f:
                        file_data = json.load(f)
                        if isinstance(file_data, list):
                            raw_data = file_data
                        elif isinstance(file_data, dict) and 'data' in file_data:
                            raw_data = file_data['data']
                        else:
                            raw_data = file_data
                    logger.info(f"Successfully loaded {len(raw_data) if raw_data else 0} Organic records from cache")
                except Exception as e:
                    logger.warning(f"Failed to read from Organic cache file: {str(e)}")
                    raw_data = None
            else:
                logger.info(f"Organic file is older than 1 day ({file_age}), fetching fresh data")
        
        # If no valid cached data, extract from website
        if raw_data is None:
            logger.info("Fetching fresh Organic data from MyGAP website...")
            raw_data = extract_mygap_organic_data(save_to_file=True)  # Save fresh data to file
            data_source = "fresh"
            
            if raw_data is None:
                logger.error("Failed to extract Organic data from MyGAP website")
                raise HTTPException(
                    status_code=500, 
                    detail="Failed to extract Organic data from MyGAP website. The website might be unavailable."
                )
        
        # Convert raw data to Pydantic models
        records = []
        for item in raw_data:
            record = MyGAPRecord(**item)
            records.append(record)
        
        message = f"Successfully loaded {len(records)} MyGAP Organic certification records from {data_source}"
        response = MyGAPResponse(
            success=True,
            message=message,
            total_records=len(records),
            timestamp=datetime.now().isoformat(),
            data=records
        )
        
        logger.info(message)
        return response
        
    except Exception as e:
        logger.error(f"Error loading MyGAP Organic data: {str(e)}")
        raise HTTPException(
            status_code=500, 
            detail=f"Internal server error: {str(e)}"
        )

@app.get("/mygap/data/tanaman", response_model=MyGAPResponse)
async def get_mygap_tanaman_data():
    """
    Fetch MyGAP Tanaman certification data - reads from JSON file first, 
    only fetches new data if file is older than 1 day
    
    Returns:
        MyGAPResponse: Complete dataset with all Tanaman certification records
    """
    try:
        # First try to read from existing JSON file
        raw_data = None
        data_source = "cache"
        
        # Find the most recent Tanaman JSON file
        json_files = glob.glob("mygap_data_tanaman_*.json")
        if json_files:
            # Sort by modification time, get the newest
            latest_file = max(json_files, key=os.path.getmtime)
            file_mtime = datetime.fromtimestamp(os.path.getmtime(latest_file))
            file_age = datetime.now() - file_mtime
            
            logger.info(f"Found existing Tanaman file: {latest_file}, age: {file_age}")
            
            # If file is less than 1 day old, read from it
            if file_age < timedelta(days=1):
                try:
                    with open(latest_file, 'r', encoding='utf-8') as f:
                        file_data = json.load(f)
                        if isinstance(file_data, list):
                            raw_data = file_data
                        elif isinstance(file_data, dict) and 'data' in file_data:
                            raw_data = file_data['data']
                        else:
                            raw_data = file_data
                    logger.info(f"Successfully loaded {len(raw_data) if raw_data else 0} Tanaman records from cache")
                except Exception as e:
                    logger.warning(f"Failed to read from Tanaman cache file: {str(e)}")
                    raw_data = None
            else:
                logger.info(f"Tanaman file is older than 1 day ({file_age}), fetching fresh data")
        
        # If no valid cached data, extract from website
        if raw_data is None:
            logger.info("Fetching fresh Tanaman data from MyGAP website...")
            raw_data = extract_mygap_tanaman_data(save_to_file=True)  # Save fresh data to file
            data_source = "fresh"
            
            if raw_data is None:
                logger.error("Failed to extract Tanaman data from MyGAP website")
                raise HTTPException(
                    status_code=500, 
                    detail="Failed to extract Tanaman data from MyGAP website. The website might be unavailable."
                )
        
        # Convert raw data to Pydantic models
        records = []
        for item in raw_data:
            record = MyGAPRecord(**item)
            records.append(record)
        
        message = f"Successfully loaded {len(records)} MyGAP Tanaman certification records from {data_source}"
        response = MyGAPResponse(
            success=True,
            message=message,
            total_records=len(records),
            timestamp=datetime.now().isoformat(),
            data=records
        )
        
        logger.info(message)
        return response
        
    except Exception as e:
        logger.error(f"Error loading MyGAP Tanaman data: {str(e)}")
        raise HTTPException(
            status_code=500, 
            detail=f"Internal server error: {str(e)}"
        )

@app.get("/mygap/stats", response_model=StatsResponse)
async def get_mygap_stats():
    """
    Get statistics about the MyGAP data including field completion rates
    
    Returns:
        StatsResponse: Statistics about data completeness and field completion rates
    """
    try:
        logger.info("Extracting MyGAP data for statistics...")
        
        # Extract data using our scraping function
        raw_data = extract_mygap_tbm_data(save_to_file=False)
        
        if raw_data is None:
            logger.error("Failed to extract data from MyGAP website")
            raise HTTPException(
                status_code=500, 
                detail="Failed to extract data from MyGAP website. The website might be unavailable."
            )
        
        # Calculate field statistics
        field_stats = []
        total_records = len(raw_data)
        
        for field in PF_DATA_FIELDS:
            completed_count = sum(1 for record in raw_data if record.get(field, '').strip())
            completion_percentage = (completed_count / total_records * 100) if total_records > 0 else 0
            
            stat = FieldStats(
                field_name=field,
                completed_count=completed_count,
                total_count=total_records,
                completion_percentage=round(completion_percentage, 1)
            )
            field_stats.append(stat)
        
        response = StatsResponse(
            success=True,
            message=f"Statistics for {total_records} MyGAP certification records",
            total_records=total_records,
            timestamp=datetime.now().isoformat(),
            field_statistics=field_stats
        )
        
        logger.info(f"Generated statistics for {total_records} records")
        return response
        
    except Exception as e:
        logger.error(f"Error generating MyGAP statistics: {str(e)}")
        raise HTTPException(
            status_code=500, 
            detail=f"Internal server error: {str(e)}"
        )

@app.get("/mygap/download/tbm")
async def download_json():
    """
    Download MyGAP data as JSON file
    
    Returns:
        JSONResponse: Raw JSON data for download
    """
    try:
        logger.info("Preparing JSON download...")
        
        # Extract data
        raw_data = extract_mygap_tbm_data(save_to_file=False)
        
        if raw_data is None:
            raise HTTPException(
                status_code=500, 
                detail="Failed to extract data from MyGAP website"
            )
        
        # Prepare download response
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"mygap_data_{timestamp}.json"
        
        return JSONResponse(
            content={
                "metadata": {
                    "extracted_at": datetime.now().isoformat(),
                    "total_records": len(raw_data),
                    "fields": PF_DATA_FIELDS
                },
                "data": raw_data
            },
            headers={
                "Content-Disposition": f"attachment; filename={filename}",
                "Content-Type": "application/json"
            }
        )
        
    except Exception as e:
        logger.error(f"Error preparing JSON download: {str(e)}")
        raise HTTPException(
            status_code=500, 
            detail=f"Internal server error: {str(e)}"
        )

@app.get("/mygap/download/am")
async def download_json():
    """
    Download MyGAP data as JSON file
    
    Returns:
        JSONResponse: Raw JSON data for download
    """
    try:
        logger.info("Preparing JSON download...")
        
        # Extract data
        raw_data = extract_mygap_am_data(save_to_file=False)
        
        if raw_data is None:
            raise HTTPException(
                status_code=500, 
                detail="Failed to extract data from MyGAP website"
            )
        
        # Prepare download response
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"mygap_data_{timestamp}.json"
        
        return JSONResponse(
            content={
                "metadata": {
                    "extracted_at": datetime.now().isoformat(),
                    "total_records": len(raw_data),
                    "fields": PF_DATA_FIELDS
                },
                "data": raw_data
            },
            headers={
                "Content-Disposition": f"attachment; filename={filename}",
                "Content-Type": "application/json"
            }
        )
        
    except Exception as e:
        logger.error(f"Error preparing JSON download: {str(e)}")
        raise HTTPException(
            status_code=500, 
            detail=f"Internal server error: {str(e)}"
        )

@app.get("/mygap/download/organic")
async def download_json():
    """
    Download MyGAP data as JSON file
    
    Returns:
        JSONResponse: Raw JSON data for download
    """
    try:
        logger.info("Preparing JSON download...")
        
        # Extract data
        raw_data = extract_mygap_organic_data(save_to_file=False)
        
        if raw_data is None:
            raise HTTPException(
                status_code=500, 
                detail="Failed to extract data from MyGAP website"
            )
        
        # Prepare download response
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"mygap_data_{timestamp}.json"
        
        return JSONResponse(
            content={
                "metadata": {
                    "extracted_at": datetime.now().isoformat(),
                    "total_records": len(raw_data),
                    "fields": PF_DATA_FIELDS
                },
                "data": raw_data
            },
            headers={
                "Content-Disposition": f"attachment; filename={filename}",
                "Content-Type": "application/json"
            }
        )
        
    except Exception as e:
        logger.error(f"Error preparing JSON download: {str(e)}")
        raise HTTPException(
            status_code=500, 
            detail=f"Internal server error: {str(e)}"
        )

@app.get("/mygap/download/tanaman")
async def download_json():
    """
    Download MyGAP data as JSON file
    
    Returns:
        JSONResponse: Raw JSON data for download
    """
    try:
        logger.info("Preparing JSON download...")
        
        # Extract data
        raw_data = extract_mygap_tanaman_data(save_to_file=False)
        
        if raw_data is None:
            raise HTTPException(
                status_code=500, 
                detail="Failed to extract data from MyGAP website"
            )
        
        # Prepare download response
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"mygap_data_{timestamp}.json"
        
        return JSONResponse(
            content={
                "metadata": {
                    "extracted_at": datetime.now().isoformat(),
                    "total_records": len(raw_data),
                    "fields": PF_DATA_FIELDS
                },
                "data": raw_data
            },
            headers={
                "Content-Disposition": f"attachment; filename={filename}",
                "Content-Type": "application/json"
            }
        )
        
    except Exception as e:
        logger.error(f"Error preparing JSON download: {str(e)}")
        raise HTTPException(
            status_code=500, 
            detail=f"Internal server error: {str(e)}"
        )

@app.get("/mygap/download/pf")
async def download_json():
    """
    Download MyGAP data as JSON file
    
    Returns:
        JSONResponse: Raw JSON data for download
    """
    try:
        logger.info("Preparing JSON download...")
        
        # Extract data
        raw_data = extract_mygap_pf_data(save_to_file=False)
        
        if raw_data is None:
            raise HTTPException(
                status_code=500, 
                detail="Failed to extract data from MyGAP website"
            )
        
        # Prepare download response
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"mygap_data_{timestamp}.json"
        
        return JSONResponse(
            content={
                "metadata": {
                    "extracted_at": datetime.now().isoformat(),
                    "total_records": len(raw_data),
                    "fields": PF_DATA_FIELDS
                },
                "data": raw_data
            },
            headers={
                "Content-Disposition": f"attachment; filename={filename}",
                "Content-Type": "application/json"
            }
        )
        
    except Exception as e:
        logger.error(f"Error preparing JSON download: {str(e)}")
        raise HTTPException(
            status_code=500, 
            detail=f"Internal server error: {str(e)}"
        )

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "service": "MyGAP Data Scraper API"
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
