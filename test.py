import requests
from bs4 import BeautifulSoup

# 1. Get the page
response = requests.get('https://carianmygapmyorganic.doa.gov.my/mygap_pf_list.php?pagesize=500')

soup = BeautifulSoup(response.content, 'html.parser')

print(soup.prettify())

# 