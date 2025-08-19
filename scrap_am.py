import requests
from bs4 import BeautifulSoup
import csv
import json
from datetime import datetime

DATA_FIELDS = [
    'no_pensijilan',      # Certification Number
    'kategori_pemohon',   # Applicant Category  
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