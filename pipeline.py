import pandas as pd
from datetime import datetime
import time
from tqdm import tqdm
import psycopg2
import sys
from info import *

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup


def connect_to_web(url):
    # Set up Edge webdriver options.
    options = webdriver.EdgeOptions()
    # Run browser in headless mode
    options.add_argument("--headless=new")
    # Enable Chrome browser cloud management
    options.add_argument("--enable-chrome-browser-cloud-management")
    # Disable the sandbox security feature
    options.add_argument("--no-sandbox")
    # Disable sync
    options.add_argument("--disable-sync")
    # Exclude logging
    options.add_experimental_option("excludeSwitches", ["enable-logging"])

    # Set the path to your webdriver
    service = Service('msedgedriver.exe')

    # Initiate the browser
    driver = webdriver.Edge(service=service, options=options)
    # Open the URL
    driver.get(url)
    # Refresh the page
    driver.refresh()
    # Wait for 5 seconds to let the page load
    time.sleep(5)
    return driver

def crawl_data_one_page(soup, url):
    # Get the category from the URL
    category = url.split('/')[-1]
    # Find all movie articles based on the category
    if (category == 'phim-chieu-rap')|(category == 'phim-le'):
        movies = soup.find_all("article", {"class": "item movies"})
    if category == 'phim-bo':    
        movies = soup.find_all("article", {"class": "item tvshows"})
        
    movies_dict = []
    
    # Loop through each movie article and extract the data
    for movie in tqdm(movies):
        row = {
            "movie_id": int(movie['id'].split('-')[-1]),
            "movie_link": movie.find('a')['href'],
            "poster_image_link": movie.find('img')['src'],
            "movie_status": movie.find('div', {'class':'trangthai'}).text if movie.find('div', {'class':'trangthai'}) else None,
            "vn_name": movie.find('img')['alt'],
            "en_name": movie.find('span').text.strip(' '),
            "category": category
        }
        movies_dict.append(row)
    # Convert the list of dictionaries to a DataFrame
    movies_df = pd.DataFrame(movies_dict)
    return movies_df

def crawl_data(driver, url):
    print('='*30)
    print(f'Started crawling in web: {url}')
    # Get the page source
    page_source = driver.page_source
    # Parse the page source with BeautifulSoup
    soup = BeautifulSoup(page_source, 'lxml')
    # Wait for 5 seconds to let the page load
    time.sleep(5)
    # Crawl data from the first page
    df0 = crawl_data_one_page(soup, url)
    print('Finished crawling page: 1')
    # Get the total number of pages
    page_num = len(soup.find_all("div", {"class": "pagination"})[0].find_all('a', {'class': 'inactive'})) + 2
    # Loop through each page and crawl the data
    for page_id in range(2, page_num):
        driver = connect_to_web(url+'/page/'+str(page_id))
        page_source = driver.page_source
        soup = BeautifulSoup(page_source, 'lxml')
        time.sleep(5)
        df1 = crawl_data_one_page(soup, url)
        # Concatenate the data from the current page with the previous pages
        df0 = pd.concat([df0, df1], ignore_index=True)
        print(f'Finished crawling page: {page_id}')
    return df0

def create_connection(HOST, DATABASE, USER, PASSWORD):
    # Print a line for readability
    print('='*30)
    # Establish a connection to the database
    connection = psycopg2.connect(
        host=HOST,
        database=DATABASE,
        user=USER,
        password=PASSWORD
    )
    # Set autocommit mode for the connection
    connection.set_session(autocommit=True)
    # Print a success message
    print('Connected to database !')
    # Return the connection object
    return connection

def create_table(connection, table_name, event):
    # Print a line for readability
    print('='*30)
    # Create a list of field names and their types
    list_field_name_field_type = [f'{field["field"]} {field["type"]}' for field in event['data_field']] 
    # Join the list into a string
    result_field_name_field_type = ','.join(list_field_name_field_type)

    # Create a SQL query string to create a table
    create_table_str = f"""
        create table if not exists {table_name} (
            {result_field_name_field_type},
            PRIMARY KEY (movie_id)
        )
    """
    # Create a cursor object
    cursor = connection.cursor()
    # Execute the SQL query
    cursor.execute(create_table_str)
    # Print a success message
    print('Created a table !')

def ingest_to_db(connection, df, table_name):
    # Print a line for readability
    print('='*30)
    # Create a cursor object
    cur = connection.cursor()

    # Loop through each row in the dataframe
    for row in tqdm(df.itertuples()):
        try:
            # Execute an SQL query to insert data into the table
            cur.execute(
                f"INSERT INTO {table_name} (movie_id, movie_link, poster_image_link, movie_status, en_name, vn_name, category) VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT (movie_id) DO NOTHING",
                (row.movie_id, row.movie_link, row.poster_image_link, row.movie_status, row.en_name, row.vn_name, row.category)
            )
            # Commit the transaction if no error occurs
            connection.commit()
        except Exception as e:
            # Print the error message if an error occurs
            print("An error occurred:", e)
            # Rollback the transaction in case of error
            connection.rollback()

    # Close the cursor
    cur.close()
    # Print a success message
    print('Ingested data to database')

### ============ MAIN TASK ============
if __name__=='__main__':

    event = {
        "urls" : [
            "https://phimmoiiii.net/the-loai/phim-chieu-rap",
            "https://phimmoiiii.net/phim-bo",
            "https://phimmoiiii.net/phim-le"
        ],
        "table_name": "ai4e_movie_hauct",
        "data_field": [
            {
                "field": "movie_id",
                "type": "integer"
            },
            {
                "field": "movie_link",
                "type": "varchar"
            },
            {
                "field": "poster_image_link",
                "type": "varchar"
            },
            {
                "field": "movie_status",
                "type": "varchar"
            },
            {
                "field": "en_name",
                "type": "varchar"
            },
            {
                "field": "vn_name",
                "type": "varchar"
            },
            {
                "field": "category",
                "type": "varchar"
            }
        ]
    }

    # Create a connection to the database
    conn = create_connection(HOST, DATABASE, USER, PASSWORD)

    # Get the table name from the event dictionary
    table_name = event['table_name']
    # Create a table in the database
    create_table(conn, table_name, event)

    # Loop through each URL in the event dictionary
    for url in event['urls']:
        # Connect to the web page
        driver = connect_to_web(url)
        # Crawl data from the web page
        df = crawl_data(driver, url)
        # Ingest the crawled data into the database
        ingest_to_db(conn, df, table_name)        
    # Print a success message
    print('Completed the pipeline')
    # Close the connection to the database
    conn.close()
