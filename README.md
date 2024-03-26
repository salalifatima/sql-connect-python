import json
import pyodbc
import requests
from bs4 import BeautifulSoup

server = 'localhost'
database = 'bina_az'
username = 'SA'
password = 'MyStrongPass123'
driver = '/opt/homebrew/Cellar/freetds/1.4.10/lib/libtdsodbc.so'

connection_string = f"DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password}"

connection = pyodbc.connect(connection_string)
cursor = connection.cursor()

headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2.1 Safari/605.1.15'}

urls = ["https://bina.az/alqi-satqi?page={}".format(i) for i in range(1, 2700)]

for url in urls:
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'html.parser')
        ev_links = soup.find_all('div', class_='items-i')

        if ev_links:
            for ev_link in ev_links:
                products = {
                    'Location': [],
                    'Category': [],
                    'Floor': [],
                    'Area': [],
                    'GroundArea': [],
                    'Rooms': [],
                    'TitleDeed': [],
                    'Mortgage': [],
                    'Repair': [],
                    'Phone': [],
                    'Price': [],
                    'Photo': []
                }
                link = "https://bina.az{}".format(ev_link.find('a')['href'])
                linked_page_response = requests.get(link, headers=headers)
                if linked_page_response.status_code == 200:
                    linked_soup = BeautifulSoup(linked_page_response.text, 'html.parser')
                    kateqoriyalar = linked_soup.find_all('div', class_='product-properties__i')
                    labels = {}
                    for kateqoriya in kateqoriyalar:
                        kateqoriya_in_name = kateqoriya.find('label', class_='product-properties__i-name').get_text(strip=True)
                        kateqoriya_in_val = kateqoriya.find('span', class_='product-properties__i-value').get_text(strip=True)
                        labels[kateqoriya_in_name] = kateqoriya_in_val

                    products['Category'].append(labels.get('Kateqoriya', 'null'))
                    products['Floor'].append(labels.get('Mərtəbə', 'null'))
                    products['Area'].append(labels.get('Sahə', 'null'))
                    products['Rooms'].append(labels.get('Otaq sayı', 'null'))
                    products['TitleDeed'].append(labels.get('Çıxarış', 'null'))
                    products['Mortgage'].append(labels.get('İpoteka', 'null'))
                    products['Repair'].append(labels.get('Təmir', 'null'))
                    products['GroundArea'].append(labels.get('Torpaq sahəsi', 'null'))

                    price_text = linked_soup.find('span', class_='price-val').get_text(strip=True)
                    products['Price'].append(price_text)

                    sekiller = linked_soup.find_all('div', class_='product-photos__slider-nav-i js-open-gallery')
                    photos = []
                    for sekil in sekiller:
                        img_url = sekil.find('div')['style']
                        photos.append(img_url)
                    products['Photo'].append(photos)

                    loc_text = linked_soup.find('div', class_='product-map__left__address').get_text(strip=True)
                    products['Location'].append(loc_text)

                    nomreler = ["{}/phones".format(link)]
                    for nomre in nomreler:
                        nomre_response = requests.get(nomre, headers=headers)
                        if nomre_response.status_code == 200:
                            nomre_soup = BeautifulSoup(nomre_response.text, 'html.parser')
                            phone_json = nomre_soup.find('p').get_text(strip=True)
                            phone_text = json.loads(phone_json).get('phones', [])
                            products['Phone'].append(', '.join(phone_text))

                    insert_query = f"INSERT INTO {EmlakElanlari} (Location, Category, Floor, Area, GroundArea, Rooms, TitleDeed, Mortgage, Repair, Phone, Price, Photo) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
                    photos_str = ', '.join(products['Photo'][0])
                    cursor.execute(insert_query, (products['Location'][0], products['Category'][0], products['Floor'][0], products['Area'][0], products['GroundArea'][0], products['Rooms'][0], products['TitleDeed'][0], products['Mortgage'][0], products['Repair'][0], products['Phone'][0], products['Price'][0], photos_str))
                    connection.commit()

connection.close()
