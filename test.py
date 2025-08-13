import requests
from bs4 import BeautifulSoup

response = requests.get('https://carianmygapmyorganic.doa.gov.my/mygap_pf_list.php')

soup = BeautifulSoup(response.content, 'html.parser')

print(soup.prettify())