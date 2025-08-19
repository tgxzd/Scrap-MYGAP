import requests
from bs4 import BeautifulSoup
import csv
import json
from datetime import datetime
import time
import re
from urllib.parse import urljoin, parse_qs, urlparse
import html

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

def get_full_text_from_dialog(session, more_link_url, base_url):
    """Extract full text from the dialog modal when 'More ...' is clicked"""
    try:
        # Construct the full URL for the dialog content
        if more_link_url.startswith('fulltext.php'):
            full_url = urljoin(base_url, more_link_url)
        else:
            full_url = more_link_url
            
        print(f"  Fetching full content from: {full_url}")
        
        # Add a small delay to be respectful
        time.sleep(0.5)
        
        response = session.get(full_url)
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

def extract_mygap_tanaman_data(save_to_file=True):
    """Extract all available data from MyGAP certification table"""
    print("Fetching data from MyGAP website...")
    
    # Create a session to maintain cookies and handle multiple requests
    session = requests.Session()
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
    
    # Extract data from all rows after the header
    extracted_data = []
    
    for row in rows[header_row_index + 1:]:
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
                        print(f"Found truncated {field} field, fetching full content...")
                        
                        # Look for the "More ..." link in the cell with data-query attribute
                        more_link = cell.find('a', attrs={'data-query': re.compile(r'fulltext\.php')})
                        if more_link:
                            # Extract the URL from data-query attribute or href
                            query_url = more_link.get('data-query') or more_link.get('href')
                            if query_url and query_url != 'javascript:void(0);':
                                full_content = get_full_text_from_dialog(session, query_url, base_url)
                                if full_content:
                                    cell_data = full_content
                                    print(f"  Successfully fetched full content: {cell_data[:100]}...")
                                else:
                                    print(f"  Failed to fetch full content, keeping truncated version")
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

if __name__ == "__main__":

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