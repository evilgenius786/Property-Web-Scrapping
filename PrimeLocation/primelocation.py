import csv
import datetime
import json
import os
import threading
import time
import traceback

import cfscrape
import pymysql
from bs4 import BeautifulSoup
from pymysql.converters import escape_string

DB_HOST = "localhost"
DB_USER = 'root'
DB_PW = ''
DB_NAME = 'PrimeLocation'
TABLE_NAME = 'PrimeLocation'

semaphore = threading.Semaphore(1)
write = threading.Semaphore(1)
outcsv = "Out-PrimeLocation.csv"
errorfile = "Error-PrimeLocation.txt"
headers = ['Name', 'PriceEUR', 'PricePerUnit', 'Area', 'Location', 'Contact', 'Description', 'URL', 'Features',
           'Images', 'PropertyInfo']
scraped = []

wait403 = 10
forbidden = False


def scrape(url):
    global forbidden, scraped
    with semaphore:
        if url not in scraped:
            try:
                print("Working on", url)
                # with open('index.html') as ufile:
                #     soup = BeautifulSoup(ufile.read(), 'lxml')
                soup = get(url)
                forbidden = retry = "403 Forbidden" in soup.text
                while forbidden:
                    time.sleep(wait403)
                    print(datetime.datetime.now(),"403")
                if retry:
                    soup = get(url)
                try:
                    price = int(getText(soup, 'span', 'class', 'price')[2:].replace(",", ""))
                except:
                    # traceback.print_exc()
                    price = 0
                try:
                    ppu = int(getText(soup, 'span', 'class', 'price-per-unit')[2:-2].replace(",", ""))
                except:
                    # traceback.print_exc()
                    ppu = 0

                try:
                    features = [li.text for li in
                                soup.find('h3', text="Property features").find_next_sibling('ul').find_all(
                                    'li') if soup.find('h3', text="Property features") is not None]
                except:
                    # traceback.print_exc()
                    features = []
                try:
                    info = []
                    for li in soup.find('ul', {'class': 'listing-content clearfix noprint'}).find_all('li'):
                        info.append({li.find('span').text.strip(): li.find("a")['href']})
                except:
                    # traceback.print_exc()
                    info = []
                try:
                    desc = [x.text.strip() for x in soup.find('div', {'class': "top"}).find_all() if
                            len(x.text.strip()) > 0]
                    if len(desc) == 0:
                        desc = [soup.find('div', {'class': "top"}).text.strip()]
                except:
                    # traceback.print_exc()
                    desc = []
                data = {
                    "Name": soup.find('h1').text.strip(),
                    "PriceEUR": price,
                    "PricePerUnit": ppu,
                    "Area": getText(soup, 'span', 'class', 'num-icon num-sqft'),
                    "Location": soup.find('h1').text.strip().split(" for sale in ")[1],
                    "Contact": getMeta(soup, 'meta', 'property', 'og:phone_number'),
                    "Description": " ".join(desc),
                    "URL": url,
                    "Features": " | ".join(features),
                    "Images": " | ".join([meta['content'] for meta in soup.find_all('meta', {'property': "og:image"})]),
                    "PropertyInfo": info
                }
                print(json.dumps(data, indent=4))
                with open(f"./JSON/{url.split('/')[-1]}.json", 'w', encoding='utf8', errors='ignore') as outfile:
                    json.dump(data, outfile, indent=4)
                append(data)
                scraped.append(url)
            except:
                print("Error on", url)
                with open(errorfile, 'a') as efile:
                    efile.write(url + "\n")
                traceback.print_exc()


def main():
    global semaphore, scraped, forbidden
    logo()
    threadcount = input("Please enter number of threads: ")
    if threadcount == "":
        threadcount = 1
    else:
        threadcount = int(threadcount)
    while True:
        semaphore = threading.Semaphore(threadcount)
        start_url = "https://www.primelocation.com/for-sale/commercial/property/uk/?page_size=100&pn=1"
        if not os.path.isdir("JSON"):
            os.mkdir("JSON")
        if not os.path.isfile(outcsv):
            with open(outcsv, 'w', newline='') as outfile:
                csv.DictWriter(outfile, fieldnames=headers).writeheader()
        with open(outcsv, encoding='utf8', errors='ignore') as ofile:
            for line in csv.DictReader(ofile):
                try:
                    scraped.append(line['URL'])
                except:
                    pass
        print("Already scraped listings", scraped)
        print("Loading data...")
        soup = get(start_url)
        while "403 Forbidden" in soup.text:
            print(datetime.datetime.now(), "======403 Forbidden======")
            forbidden = True
            soup = get(start_url)
            time.sleep(wait403)
        forbidden = False
        print("Result count:", soup.find('span', {"class": "listing-results-utils-count"}).text.strip())
        threads = []
        try:
            for i in range(2, 100):
                print("URL", start_url)
                for li in soup.find_all('li', {"data-listing-id": True}):
                    url = f"https://www.primelocation.com/for-sale/commercial/details/{li['data-listing-id']}"
                    if url not in scraped:
                        t = threading.Thread(target=scrape, args=(url,))
                        threads.append(t)
                        t.start()
                        # print(url)
                    else:
                        print("Already scraped", url)
                start_url = f"https://www.primelocation.com/for-sale/commercial/property/uk/?page_size=100&pn={i}"
                # time.sleep(5)
                while "403 Forbidden" in soup.text:
                    forbidden = True
                    print(datetime.datetime.now(), f"403 Forbidden! Retrying after {wait403} seconds...")
                    time.sleep(wait403)
                    soup = get(start_url)
                forbidden = False
                soup = get(start_url)
            for thread in threads:
                thread.join()

            print("Done with scraping, now adding stuff to DB.")
        except KeyboardInterrupt:
            print("Scraping interrupted! Now adding stuff to DB.")
        handler = DBHandler()
        with open(outcsv, encoding='utf8') as outfile:
            rows = [row for row in csv.DictReader(outfile)]
            handler.bulkInsert(rows)
        print("Done with DB insertion! Now waiting for 24 hrs")
        time.sleep(86400)


def append(data):
    with write:
        with open(outcsv, 'a', newline="", encoding='utf8', errors='ignore') as outfile:
            csv.DictWriter(outfile, fieldnames=headers).writerow(data)


def getText(soup, tag, attrib, Class):
    try:
        return soup.find(tag, {attrib: Class}).text.strip()
    except:
        return ""


def getMeta(soup, tag, attrib, Class):
    try:
        return soup.find(tag, {attrib: Class})['content']
    except:
        return ""


def get(url):
    return BeautifulSoup(cfscrape.create_scraper().get(url).text, 'lxml')


class DBHandler:
    DB_CONN = None
    scraped = []

    def __init__(self):
        self.DB_HOST = DB_HOST
        self.DB_USER = DB_USER
        self.DB_PW = DB_PW
        self.DB_NAME = DB_NAME
        self.TABLE_NAME = TABLE_NAME
        self.openConnection()
        self.setupDB()

    def setupDB(self):
        self.createDB()
        self.DB_CONN.select_db(self.DB_NAME)
        self.createTable()

    def openConnection(self):
        self.DB_CONN = pymysql.connect(host=self.DB_HOST, user=self.DB_USER, password=self.DB_PW, autocommit=True)

    def closeConnection(self):
        self.DB_CONN.close()

    def createDB(self):
        self.executeSQL(f"CREATE DATABASE IF NOT EXISTS {self.DB_NAME}")

    def executeSQL(self, sql, args=None):
        if self.DB_CONN is None:
            print("Please open connection first!")
            return
        with self.DB_CONN.cursor(pymysql.cursors.DictCursor) as cursor:
            cursor.execute(sql, args)
            queryResult = cursor
        return queryResult

    def exists(self, url):
        sql = f"SELECT COUNT(*) FROM {self.TABLE_NAME} WHERE URL='{url}'"
        res = self.executeSQL(sql).fetchone()['COUNT(*)']
        if res == 0:
            return False
        else:
            return True

    def getInt(self, value):
        try:
            return int(value)
        except Exception:
            return 0

    def createTable(self):
        self.executeSQL(f"""CREATE TABLE IF NOT EXISTS {self.TABLE_NAME} (
        Name TEXT, 
        PriceEUR INTEGER,
        PricePerUnit TEXT,
        Area TEXT,
        Location TEXT,
        Contact TEXT,
        Description Text,
        URL Text,
        Features Text,
        Images Text,
        PropertyInfo Text
        )""")

    def createQuery(self, data):
        return f"""INSERT INTO {self.TABLE_NAME} (Name, PriceEUR, PricePerUnit, Area, Location, Contact, Description, 
        URL, Features, Images, PropertyInfo) VALUES ('{escape_string(data["Name"])}','{(data["PriceEUR"])}',
        '{(data["PricePerUnit"])}','{escape_string(data["Area"])}','{data["Location"]}','
{escape_string(data["Contact"])}','{escape_string(data["Description"])}','{escape_string(data["URL"])}',
'{escape_string(data["Features"])}','{escape_string(data["Images"])}','{escape_string(data["PropertyInfo"])}'); """

    def insert(self, data):
        if not self.exists(data['URL']):
            self.executeSQL(self.createQuery(data))
            print(data['URL'], "inserted!")
        else:
            print(data['URL'], 'already in db!')

    def bulkInsert(self, rows):
        self.scraped = [row['URL'] for row in self.getAllData()]
        print("Already scraped", self.scraped)
        for row in rows:
            if row['URL'] not in self.scraped:
                self.executeSQL(self.createQuery(row))
                print(row['URL'], "inserted!")
            else:
                print(row['URL'], "already exists!")

    def getAllData(self):
        sql = f"SELECT * FROM {self.TABLE_NAME}"
        return self.executeSQL(sql).fetchall()


def logo():
    os.system('cls')
    os.system('color 0a')
    print(rf"""
__________         .__                   .____                             __   .__                 
\______   \_______ |__|  _____    ____   |    |     ____    ____  _____  _/  |_ |__|  ____    ____  
 |     ___/\_  __ \|  | /     \ _/ __ \  |    |    /  _ \ _/ ___\ \__  \ \   __\|  | /  _ \  /    \ 
 |    |     |  | \/|  ||  Y Y  \\  ___/  |    |___(  <_> )\  \___  / __ \_|  |  |  |(  <_> )|   |  \
 |____|     |__|   |__||__|_|  / \___  > |_______ \\____/  \___  >(____  /|__|  |__| \____/ |___|  /
                             \/      \/          \/            \/      \/                        \/                 
=====================================================================================================
                    PrimeLocation.com scraper by github.com/evilgenius786
=====================================================================================================
[+] Resumable
[+] Multithreaded
[+] Without browser
[+] CSV and JSON output
[+] Exception Handling
[+] Super fast and efficient
[+] Log/error/progress reporting
___________________________________________________________
Error file: {errorfile}
Output CSV file: {outcsv}
Output JSON dir: ./JSON
___________________________________________________________
""")


if __name__ == '__main__':
    main()
