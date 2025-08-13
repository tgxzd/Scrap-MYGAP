import requests
from bs4 import BeautifulSoup

# 1. Get the page
response = requests.get('https://carianmygapmyorganic.doa.gov.my/mygap_pf_list.php?pagesize=500')

soup = BeautifulSoup(response.content, 'html.parser')

# Filter by the specific table header with data-field="no_pensijilan"
target_header = soup.find('th', {'data-field': 'no_pensijilan'})

if target_header:
    print("Found the target header:")
    print(target_header)
    
    # Get the parent table to work with the data structure
    parent_table = target_header.find_parent('table')
    
    if parent_table:
        print("\nParent table found. Extracting data...")
        
        # Find all rows in the table
        rows = parent_table.find_all('tr')
        
        # Find the header row index
        header_row = None
        col_index = None
        
        for i, row in enumerate(rows):
            headers = row.find_all(['th', 'td'])
            for j, header in enumerate(headers):
                if header.get('data-field') == 'no_pensijilan':
                    header_row = i
                    col_index = j
                    break
            if header_row is not None:
                break
        
        if col_index is not None:
            print(f"\nCertification number column found at index {col_index}")
            
            # Extract data from that specific column
            certification_numbers = []
            
            # Skip header row and extract data
            for row in rows[header_row + 1:]:
                cells = row.find_all(['th', 'td'])
                if len(cells) > col_index:
                    cert_data = cells[col_index].get_text(strip=True)
                    if cert_data:  # Only add non-empty values
                        certification_numbers.append(cert_data)
            
            print(f"\nFound {len(certification_numbers)} certification numbers:")
            for i, cert_num in enumerate(certification_numbers[:10], 1):  # Show first 10
                print(f"{i}. {cert_num}")
            
            if len(certification_numbers) > 10:
                print(f"... and {len(certification_numbers) - 10} more")
                
        else:
            print("Could not find the certification number column")
    else:
        print("Could not find parent table")
else:
    print("Header with data-field='no_pensijilan' not found")
    print("Available headers:")
    all_headers = soup.find_all('th', {'data-field': True})
    for header in all_headers[:5]:  # Show first 5 available headers
        print(f"- {header.get('data-field')}: {header.get_text(strip=True)}")

# 