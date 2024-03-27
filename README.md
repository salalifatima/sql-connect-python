import json
import requests
import pandas as pd
from bs4 import BeautifulSoup
from mysql.connector import (connection)

cnx = connection.MySQLConnection(user='mysql1', password='mysql',host='192.168.0.108',database='bina_python')
cursor = cnx.cursor()


headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2.1 Safari/605.1.15'}
urls = ["https://bina.az/alqi-satqi?page={}".format(i) for i in range(1, 2)]

for url in urls:
    print("Crawling "+url)
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        print(200)
        soup = BeautifulSoup(response.text, 'html.parser')
        ev_links = soup.find_all('div', class_='items-i')
        print(str(len(ev_links))+" ev_links found")
        if ev_links:
            for ev_link in ev_links:
                link = "https://bina.az{}".format(ev_link.find('a')['href'])
                linked_page_response = requests.get(link, headers=headers)
                if linked_page_response.status_code == 200:
                    linked_soup = BeautifulSoup(linked_page_response.text, 'html.parser')
                    
                
products = {
    'location': [],
    'category': [],
    'floor': [],
    'area': [],
    'ground_area': [],
    'rooms': [],
    'title_deed': [],
    'mortgage': [],
    'repair': [],
    'phone': [],
    'price': [],
    'photo': []
}
try:
    kateqoriyalar = linked_soup.find_all('div', class_='product-properties__i')
    labels = {}
    for kateqoriya in kateqoriyalar:
        kateqoriya_in_name = kateqoriya.find('label', class_='product-properties__i-name').get_text(strip=True)
        kateqoriya_in_val = kateqoriya.find('span', class_='product-properties__i-value').get_text(strip=True)
        labels[kateqoriya_in_name] = kateqoriya_in_val

        products['category'].append(labels.get('Kateqoriya', 'null'))
        products['floor'].append(labels.get('Mərtəbə', 'null'))
        products['area'].append(labels.get('Sahə', 'null'))
        products['rooms'].append(labels.get('Otaq sayı', 'null'))
        products['title_deed'].append(labels.get('Çıxarış', 'null'))
        products['mortgage'].append(labels.get('İpoteka', 'null'))
        products['repair'].append(labels.get('Təmir', 'null'))
        products['ground_area'].append(labels.get('Torpaq sahəsi', 'null'))
                  
except requests.exceptions.RequestException as e:
    print("Error making the request:", e)                    
try:
    price_text = linked_soup.find('span', class_='price-val').get_text(strip=True)
    products['price'].append(price_text)

except requests.exceptions.RequestException as e:
    print("Error making the request:", e)                    
try:
    sekiller = linked_soup.find_all('div', class_='product-photos__slider-nav-i js-open-gallery')
    photos = []
    for sekil in sekiller:
        img_url = sekil.find('div')['style']
        photos.append(img_url)
        products['photo'].append(photos)
                    
except requests.exceptions.RequestException as e:
    print("Error making the request:", e)
try:
    loc_text = linked_soup.find('div', class_='product-map__left__address').get_text(strip=True)
    products['location'].append(loc_text)
    
except requests.exceptions.RequestException as e:
    print("Error making the request:", e)
try:
    req = requests.get(f'{link}/phones', headers=headers)
    req.raise_for_status() 

    sp = BeautifulSoup(req.content, 'lxml')

    phone_json = sp.find('p').get_text(strip=True)
    phone_data = json.loads(phone_json)
    phone_text = phone_data.get('phones', 'null')

    products['phone'].append(phone_text)

    print(phone_text)
except requests.exceptions.RequestException as e:
    print("Error making the request:", e)

print(products)                      

        
insert_query = f"INSERT INTO emlak_elanlari (category, floor, area, ground_area, rooms, price, title_deed, mortgage, repair, location, phone) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
if products['category'] and products['floor'] and products['area'] and products['ground_area'] and products['rooms'] and products['price'] and products['title_deed'] and products['mortgage'] and products['repair'] and products['location'] and products['phone']:
    cursor.execute(insert_query, (
        products['category'][0], 
        products['floor'][0], 
        products['area'][0], 
        products['ground_area'][0], 
        products['rooms'][0],
        products['price'][0], 
        products['title_deed'][0], 
        products['phone'][0]
    ))    
cnx.commit()

cursor.close()
cnx.close()
