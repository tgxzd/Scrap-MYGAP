import requests
from bs4 import BeautifulSoup
import csv
import json
from datetime import datetime

DATA_FIELDS = [
    'no_pensijilan',      # Certification Number
    'projek',   # Applicant Category  
    'nama',               # Name
    'negeri',             # State
    'daerah',             # District
    'jenis_tanaman',        # Bee Type
    'kategori_komoditi',  # Commodity Category
    'kategori_tanaman',   # Plant Category
    'bil_haif',           # Number of Hives
    'luas_ladang',        # Farm Area (Ha)
    'tahun_pensijilan',   # Certification Year
    'tarikh_pensijilan',  # Certification Date
    'tempoh_sah_laku',    # Validity Period/Expiry Date
]

def extract_mygap_am_data(save_to_file=True):
    """Extract all available data from MyGAP AM (Apiary Management) certification table"""
    print("Fetching data from MyGAP AM website...")
    
    # 1. Get the page
    response = requests.get('https://carianmygapmyorganic.doa.gov.my/mygap_am_list.php?pagesize=500')
    
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
        
        # Extract data for each field we're interested in
        for field in DATA_FIELDS:
            if field in field_to_col_map:
                col_index = field_to_col_map[field]
                if len(cells) > col_index:
                    cell_data = cells[col_index].get_text(strip=True)
                    row_data[field] = cell_data
                    if cell_data:  # Check if there's actual data
                        has_data = True
                else:
                    row_data[field] = ""
            else:
                row_data[field] = ""
        
        # Only add rows that have at least some data
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
        csv_filename = f"mygap_data_am_{timestamp}.csv"
        with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=DATA_FIELDS)
            writer.writeheader()
            writer.writerows(data)
        print(f"Data saved to {csv_filename}")
    
    if format in ['json', 'both']:
        json_filename = f"mygap_data_am_{timestamp}.json"
        
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
            if value:  # Only show fields with data
                print(f"  {field}: {value}")
    
    if len(data) > num_samples:
        print(f"\n... and {len(data) - num_samples} more records")

# Main execution
if __name__ == "__main__":
    # Extract the data
    mygap_data = extract_mygap_am_data()
    
    if mygap_data:
        # Display sample data
        display_sample_data(mygap_data)
        
        # Save data to files
        save_data(mygap_data)
        
        # Show summary statistics
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