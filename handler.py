
import mysql.connector
import pymysql
import requests
from bs4 import BeautifulSoup as soup
import pandas as pd


# RDS settingsß
rds_host = "database-1.cgjxeyoygkuk.us-east-1.rds.amazonaws.com"
user_name = "admin"
password = "Laloiowa2023!"
db_name = "reviews"

def get_db_connection():
    return mysql.connector.connect(
        host=rds_host,
        user=user_name,
        password=password,
        database=db_name
    )

# create the database connection outside of the handler to allow connections to be
#try:
#    conn = pymysql.connect(host=rds_host, user=user_name, passwd=password, db=db_name, connect_timeout=5)
#except pymysql.MySQLError as e:
#    logger.error("ERROR: Unexpected error: Could not connect to MySQL instance.")
#    logger.error(e)
#    sys.exit()

#logger.info("SUCCESS: Connection to RDS MySQL instance succeeded")

def insert_review(cursor, review):
    query = "INSERT INTO ClientReviews (ClientName, Origin, Contributions, Rating, ReviewTitle, ReviewText) " \
            "VALUES (%s, %s, %s, %s, %s, %s)"
    cursor.execute(query, review)


def lambda_handler(event, context):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.69 Safari/537.36'
    }

    conn = get_db_connection()
    cursor = conn.cursor()
    
    all_reviews = []
    for i in range(0, 30, 10):
        url = f'https://www.tripadvisor.com/Attraction_Review-g187514-d12871899-Reviews-or{i}-Teatro_Flamenco_Madrid-Madrid.html'
        print(f'Scraping page {i} at URL: {url}')
        req = requests.get(url, url,headers=headers,timeout=5,verify=False)
        bsobj = soup(req.text)
        #print(f'Scraping page {i} at URL: {url}')

        client_names = []
        for review in bsobj.findAll('div', {'class': 'zpDvc'}):
            client_name_elem = review.find('a', {'class': 'BMQDV _F G- wSSLS SwZTJ FGwzt ukgoS'})
            if client_name_elem:
                client_name = client_name_elem.text
                client_name = client_name.replace("TeatroFlamencoMadrid", "").strip()
                client_names.append(client_name)
        print(f'Scraped {len(client_names)} client_names: {client_names}')
                    
        client_origin = []
        for origin in bsobj.findAll('div', {'class': 'JINyA'}):
            origin_text = origin.text.strip()
            origin_parts = origin_text.split(',')
            if len(origin_parts) >= 2:
                city_country = origin_parts[-2] + ', ' + origin_parts[-1]
                client_origin.append(city_country)
        print(f'Scraped {len(client_origin)} client_origin: {client_origin}')
                 
        client_contributions = []
        for div in bsobj.find_all('div', {'class': 'biGQs _P pZUbB osNWb'}):
            contributions = div.text.strip().split()
            if len(contributions) > 0:
                try:
                    client_contributions.append(int(contributions[0]))
                except ValueError:
                    continue
        print(f'Scraped {len(client_contributions)} client_contributions: {client_contributions}') 
                    
        client_ratings = []
        for rating in bsobj.findAll('svg', {'class': 'UctUV d H0'}):
            client_ratings.append(rating['aria-label'])
        print(f'Scraped {len(client_ratings)} client_ratings: {client_ratings}')           
            
        client_review_title = []
        for review_title in bsobj.findAll('a',{'class':'BMQDV _F G- wSSLS SwZTJ FGwzt ukgoS'}):
            title_span = review_title.find('span', {'class': 'yCeTE'})
            if title_span:
                client_review_title.append(title_span.text.strip())
        print(f'Scraped {len(client_review_title)} client_review_title: {client_review_title}')
         
        client_review_text = []
        review_spans = bsobj.findAll('span', {'class': 'yCeTE'})
        for review_span in review_spans:
            review_text = review_span.text.strip()
            if "Tickets from" not in review_text:
                client_review_text.append(review_text)
        print(f'Scraped {len(client_review_text)} client_review_text: {client_review_text}')
        
        all_reviews.extend(zip(client_names, client_origin, client_contributions,
                               client_ratings, client_review_title, client_review_text))
    

    return all_reviews