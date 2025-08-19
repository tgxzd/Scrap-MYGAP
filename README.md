# MyGAP Data Scraper API

A FastAPI-based web scraper for extracting Malaysian Good Agricultural Practice (MyGAP) certification data from the official government website.

## Features

- 🌐 Web API for accessing MyGAP certification data
- 📊 Data caching (refreshes automatically after 24 hours)
- 📈 Statistics endpoint for data analysis
- 📥 JSON download functionality
- 🔍 Support for multiple data categories (PF, AM, Tanaman, TBM)
- 📝 Automatic data validation and formatting

## Installation

### Prerequisites
- Python 3.7 or higher
- Internet connection for web scraping

### Step 1: Clone the repository
```bash
git clone <repository-url>
cd Scrap-MYGAP
```

### Step 2: Set up virtual environment (recommended)
```bash
# On Windows
python -m venv myenv
myenv\Scripts\activate

# On macOS/Linux
python -m venv myenv
source myenv/bin/activate
```

### Step 3: Install dependencies
```bash
pip install -r requirement.txt
```

## Usage

### Running the API Server
```bash
python main.py
```

The server will start on `http://localhost:8000`

### API Endpoints

| Endpoint | Description |
|----------|-------------|
| `/` | API information and available endpoints |
| `/mygap/data/pf` | Get all MyGAP PF certification data |
| `/mygap/stats` | Get statistics about the data |
| `/mygap/download/json` | Download data as JSON file |
| `/health` | Health check endpoint |
| `/docs` | Interactive API documentation (Swagger UI) |
| `/redoc` | Alternative API documentation |

### Example Usage

**Get certification data:**
```bash
curl http://localhost:8000/mygap/data/pf
```

**Get data statistics:**
```bash
curl http://localhost:8000/mygap/stats
```

**Access interactive documentation:**
Open `http://localhost:8000/docs` in your browser

## Data Fields

The scraper extracts the following fields from MyGAP certification records:

- `no_pensijilan` - Certification Number
- `projek` - Project/Applicant Category
- `nama` - Name of certificate holder
- `negeri` - State
- `daerah` - District
- `jenis_tanaman` - Plant Type
- `kategori_komoditi` - Commodity Category
- `kategori_tanaman` - Plant Category
- `luas_ladang` - Farm Area (Hectares)
- `tahun_pensijilan` - Certification Year
- `tarikh_pensijilan` - Certification Date
- `tempoh_sah_laku` - Expiry Date

## Project Structure

```
Scrap-MYGAP/
├── main.py              # FastAPI application and API endpoints
├── scrap_pf.py          # PF (Poultry/Fish) data scraper
├── scrap_am.py          # AM data scraper (basic structure)
├── scrap_tanaman.py     # Plant-based data scraper (basic structure)
├── scrap_tbm.py         # TBM data scraper (empty)
├── requirement.txt      # Python dependencies
└── README.md           # Project documentation
```

## Data Caching

- The API automatically caches scraped data as JSON files
- Cache files are named with timestamps: `mygap_data_pf_YYYYMMDD_HHMMSS.json`
- Data is automatically refreshed if cache is older than 24 hours
- Fresh data is fetched from the source website when needed

## Dependencies

- `requests` - HTTP requests for web scraping
- `beautifulsoup4` - HTML parsing and extraction
- `selenium` - Browser automation (for complex scraping)
- `lxml` - Fast XML/HTML processing
- `fastapi` - Modern web framework for APIs
- `uvicorn` - ASGI server for running FastAPI
- `pydantic` - Data validation and settings management
- `schedule` - Task scheduling capabilities
- `pyautogui` - GUI automation support

## Development

### Running in development mode
```bash
python main.py
```
The server runs with auto-reload enabled for development.

### Running individual scrapers
```bash
# Run PF scraper directly
python scrap_pf.py

# This will save data to timestamped files
```

## Notes

- The scraper targets the official MyGAP website: `https://carianmygapmyorganic.doa.gov.my/`
- Data is extracted in real-time from the government database
- The API includes error handling for website unavailability
- All dates and times are in ISO format for consistency

## Author

Created by aiizad
