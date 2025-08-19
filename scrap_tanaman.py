import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
import csv
import json
from datetime import datetime
import time
import re
from urllib.parse import urljoin, parse_qs, urlparse
import html
from concurrent.futures import ThreadPoolExecutor
import threading

DATA_FIELDS = [
    'no_pensijilan',      # Certification Number
    'projek',   # Applicant Category  
    'nama',               # Name
    'negeri',             # State
    'daerah',             # District
    'jenis_tanaman',      # Plant Type
    'kategori_komoditi',  # Commodity Category
    'kategori_tanaman',   # Plant Category
    'luas_ladang',        # Farm Area (Ha)
    'tahun_pensijilan',   # Certification Year
    'tarikh_pensijilan',  # Certification Date
    'tempoh_sah_laku'     # Expiry Date
]

def create_optimized_session():
    """Create an optimized session with connection pooling and retry strategy"""
    session = requests.Session()
    
    # Configure retry strategy
    retry_strategy = Retry(
        total=3,
        backoff_factor=0.3,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["HEAD", "GET", "OPTIONS"]
    )
    
    # Configure HTTP adapter with connection pooling
    adapter = HTTPAdapter(
        max_retries=retry_strategy,
        pool_connections=10,  # Number of connection pools to cache
        pool_maxsize=20,      # Maximum number of connections to save in pool
        pool_block=True       # Block when no free connections available
    )
    
    # Mount adapter for both HTTP and HTTPS
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    
    # Set common headers to appear more like a regular browser
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Accept-Encoding': 'gzip, deflate',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
    })
    
    return session

def get_full_text_from_dialog(session, more_link_url, base_url):
    """Extract full text from the dialog modal when 'More ...' is clicked"""
    try:
        # Construct the full URL for the dialog content
        if more_link_url.startswith('fulltext.php'):
            full_url = urljoin(base_url, more_link_url)
        else:
            full_url = more_link_url
            
        response = session.get(full_url, timeout=10)  # Add timeout
        if response.status_code == 200:
            try:
                # The response is HTML-encoded JSON format: {"success":true,"textCont":"FULL_CONTENT"}
                # First decode HTML entities
                decoded_content = html.unescape(response.text)
                json_response = json.loads(decoded_content)
                if json_response.get('success') and 'textCont' in json_response:
                    content = json_response['textCont']
                    # Clean up HTML tags and entities
                    content = re.sub(r'<br\s*/?>', ', ', content)  # Replace <br> with commas
                    content = re.sub(r'<[^>]+>', '', content)      # Remove any other HTML tags
                    content = content.replace('\\n', ', ').replace('\n', ', ')  # Replace newlines
                    content = re.sub(r',\s*,', ',', content)       # Remove duplicate commas
                    content = re.sub(r',\s*$', '', content)        # Remove trailing comma
                    content = content.strip()
                    return content
                else:
                    print(f"  Unexpected JSON structure: {json_response}")
                    return None
            except json.JSONDecodeError:
                # Fallback to HTML parsing if not JSON
                dialog_soup = BeautifulSoup(response.content, 'html.parser')
                modal_body = dialog_soup.find('div', class_='modal-body')
                if modal_body:
                    return modal_body.get_text(strip=True)
                else:
                    body_text = dialog_soup.get_text(strip=True)
                    return body_text
        else:
            print(f"  Failed to fetch dialog content: {response.status_code}")
            return None
            
    except Exception as e:
        print(f"  Error fetching dialog content: {str(e)}")
        return None

def batch_fetch_full_content(session, dialog_requests, base_url, max_workers=5):
    """Fetch multiple dialog contents in parallel with controlled concurrency"""
    results = {}
    
    def fetch_single_dialog(request_info):
        """Helper function for threading"""
        field, url, row_index = request_info
        try:
            content = get_full_text_from_dialog(session, url, base_url)
            return (row_index, field, content)
        except Exception as e:
            print(f"  Error in batch fetch for {field}: {str(e)}")
            return (row_index, field, None)
    
    if not dialog_requests:
        return results
    
    print(f"  Batch fetching {len(dialog_requests)} dialog contents with {max_workers} workers...")
    
    # Use ThreadPoolExecutor for controlled parallel requests
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all requests
        future_to_request = {
            executor.submit(fetch_single_dialog, req): req 
            for req in dialog_requests
        }
        
        # Collect results as they complete
        for future in future_to_request:
            try:
                row_index, field, content = future.result(timeout=30)  # 30 second timeout per request
                if content:
                    if row_index not in results:
                        results[row_index] = {}
                    results[row_index][field] = content
            except Exception as e:
                request_info = future_to_request[future]
                print(f"  Batch request failed for {request_info}: {str(e)}")
    
    print(f"  Batch fetch completed. Retrieved {len(results)} full contents.")
    return results

def extract_mygap_tanaman_data(save_to_file=True):
    """Extract all available data from MyGAP certification table with optimized session handling"""
    print("Fetching data from MyGAP website with optimized session...")
    
    # Create optimized session with connection pooling and retry strategy
    session = create_optimized_session()
    base_url = 'https://carianmygapmyorganic.doa.gov.my/'
    
    # 1. Get the page
    response = session.get('https://carianmygapmyorganic.doa.gov.my/mygap_tanaman_list.php?pagesize=-1')
    
    if response.status_code != 200:
        print(f"Error fetching page: {response.status_code}")
        return None
    
    soup = BeautifulSoup(response.content, 'html.parser')
    
    # Find the table by looking for the first data field
    target_header = soup.find('th', {'data-field': DATA_FIELDS[0]})
    
    if not target_header:
        print(f"Header with data-field='{DATA_FIELDS[0]}' not found")
        return None
    
    # Get the parent table
    parent_table = target_header.find_parent('table')
    
    if not parent_table:
        print("Could not find parent table")
        return None
    
    print("Table found. Extracting data...")
    
    # Find all rows in the table
    rows = parent_table.find_all('tr')
    
    # Find the header row and map field names to column indices
    header_row_index = None
    field_to_col_map = {}
    
    for i, row in enumerate(rows):
        headers = row.find_all(['th', 'td'])
        temp_map = {}
        
        for j, header in enumerate(headers):
            data_field = header.get('data-field')
            if data_field in DATA_FIELDS:
                temp_map[data_field] = j
        
        # If we found at least one of our target fields, this is likely the header row
        if temp_map:
            header_row_index = i
            field_to_col_map = temp_map
            break
    
    if not field_to_col_map:
        print("Could not find any target data fields in table headers")
        return None
    
    print(f"Found {len(field_to_col_map)} data fields:")
    for field in field_to_col_map:
        print(f"  - {field}")
    
    # Phase 1: Extract basic data and collect "More..." requests
    extracted_data = []
    dialog_requests = []  # [(field, url, row_index), ...]
    
    print("Phase 1: Extracting basic data and identifying truncated fields...")
    
    for row_index, row in enumerate(rows[header_row_index + 1:]):
        cells = row.find_all(['th', 'td'])
        if len(cells) == 0:
            continue

        row_data = {}
        has_data = False

        for field in DATA_FIELDS:
            if field in field_to_col_map:
                col_index = field_to_col_map[field]
                if len(cells) > col_index:
                    cell = cells[col_index]
                    cell_data = cell.get_text(strip=True)
                    
                    # Check if this cell contains a "More ..." link for truncated content
                    if 'More' in cell_data and '...' in cell_data:
                        # Look for the "More ..." link in the cell with data-query attribute
                        more_link = cell.find('a', attrs={'data-query': re.compile(r'fulltext\.php')})
                        if more_link:
                            # Extract the URL from data-query attribute or href
                            query_url = more_link.get('data-query') or more_link.get('href')
                            if query_url and query_url != 'javascript:void(0);':
                                # Add to batch requests instead of fetching immediately
                                dialog_requests.append((field, query_url, row_index))
                        else:
                            # Try to clean up the "More ..." suffix for better data quality
                            cell_data = re.sub(r'More\s*\.+$', '', cell_data).strip()
                            if cell_data.endswith(','):
                                cell_data = cell_data[:-1].strip()
                    
                    row_data[field] = cell_data
                    if cell_data:
                        has_data = True
                else:
                    row_data[field] = ""
            else:
                row_data[field] = ""

        if has_data:
            extracted_data.append(row_data)
    
    print(f"Phase 1 complete: {len(extracted_data)} records, {len(dialog_requests)} truncated fields found")
    
    # Phase 2: Batch fetch all "More..." content
    if dialog_requests:
        print("Phase 2: Batch fetching truncated content...")
        batch_results = batch_fetch_full_content(session, dialog_requests, base_url, max_workers=3)
        
        # Phase 3: Update extracted data with full content
        print("Phase 3: Integrating full content into extracted data...")
        for row_index, field_contents in batch_results.items():
            if row_index < len(extracted_data):
                for field, full_content in field_contents.items():
                    if full_content:
                        extracted_data[row_index][field] = full_content
                        print(f"  Updated {field} for record {row_index + 1}")
    
    # Close the session when done
    session.close()

    print(f"\nExtracted {len(extracted_data)} records")
    
    # Automatically save to file if requested (default behavior)
    if save_to_file and extracted_data:
        save_data(extracted_data, format='json')
    
    return extracted_data

def save_data(data, format='both'):
    """Save extracted data to CSV and/or JSON files"""
    if not data:
        print("No data to save")
        return
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    if format in ['csv', 'both']:
        csv_filename = f"mygap_data_tanaman_{timestamp}.csv"
        with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=DATA_FIELDS)
            writer.writeheader()
            writer.writerows(data)
        print(f"Data saved to {csv_filename}")
    
    if format in ['json', 'both']:
        json_filename = f"mygap_data_tanaman_{timestamp}.json"
        
        # Create structured JSON with metadata
        json_data = {
            "metadata": {
                "extracted_at": datetime.now().isoformat(),
                "timestamp": timestamp,
                "total_records": len(data),
                "fields": DATA_FIELDS
            },
            "data": data
        }

        with open(json_filename, 'w', encoding='utf-8') as jsonfile:
            json.dump(json_data, jsonfile, indent=2, ensure_ascii=False)
        print(f"Data saved to {json_filename}")

def display_sample_data(data, num_samples=5):
    """Display a sample of the extracted data"""
    if not data:
        print("No data to display")
        return

        print(f"\nDisplaying first {min(num_samples, len(data))} records:")
        print("-" * 80)

        for i, record in enumerate(data[:num_samples], 1):
            print(f"\nRecord {i}:")
            for field, value in record.items():
                if value:
                    print(f"  {field}: {value}")

def run_enhanced_extraction():
    """Run the enhanced extraction and show progress"""
    
    print("=" * 60)
    print("ENHANCED MyGAP DATA EXTRACTION")
    print("Now with full content extraction from 'More ...' dialogs")
    print("=" * 60)
    
    # Run the enhanced extraction
    data = extract_mygap_tanaman_data(save_to_file=True)
    
    if data:
        print(f"\n=== EXTRACTION COMPLETE ===")
        print(f"Total records extracted: {len(data)}")
        
        # Count how many records had truncated jenis_tanaman fields
        more_count = 0
        full_content_examples = []
        
        for record in data:
            jenis_tanaman = record.get('jenis_tanaman', '')
            # Look for records that likely had "More ..." originally (now should have full content)
            if jenis_tanaman and (',' in jenis_tanaman and len(jenis_tanaman) > 100):
                # These are likely records that were expanded from "More ..." dialogs
                more_count += 1
                if len(full_content_examples) < 3:
                    full_content_examples.append({
                        'no_pensijilan': record.get('no_pensijilan', ''),
                        'nama': record.get('nama', ''),
                        'jenis_tanaman': jenis_tanaman
                    })
        
        print(f"\nRecords with extensive plant lists (likely expanded from 'More ...'): {more_count}")
        
        print("\nExamples of fully expanded plant lists:")
        for i, example in enumerate(full_content_examples, 1):
            print(f"\n{i}. {example['nama']} ({example['no_pensijilan']})")
            print(f"   Plants: {example['jenis_tanaman'][:100]}...")
        
        # Field completion analysis
        field_counts = {}
        for field in ['no_pensijilan', 'nama', 'jenis_tanaman', 'negeri', 'daerah']:
            field_counts[field] = sum(1 for record in data if record.get(field, '').strip())
        
        print(f"\n=== DATA QUALITY ===")
        for field, count in field_counts.items():
            percentage = (count / len(data)) * 100 if data else 0
            print(f"{field}: {count}/{len(data)} ({percentage:.1f}%)")
            
    else:
        print("❌ Extraction failed!")

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "--enhanced":
        # Run enhanced extraction with summary
        run_enhanced_extraction()
    else:
        # Run standard extraction
        mygap_data = extract_mygap_tanaman_data()

        if mygap_data:
            display_sample_data(mygap_data)
            save_data(mygap_data)

            print(f"\n=== SUMMARY ===")
            print(f"Total records extracted: {len(mygap_data)}")
            
            # Count non-empty values for each field
            field_counts = {}
            for field in DATA_FIELDS:
                field_counts[field] = sum(1 for record in mygap_data if record.get(field, '').strip())
            
            print("\nField completion rates:")
            for field, count in field_counts.items():
                percentage = (count / len(mygap_data)) * 100 if mygap_data else 0
                print(f"  {field}: {count}/{len(mygap_data)} ({percentage:.1f}%)")
        else:
            print("Failed to extract data")