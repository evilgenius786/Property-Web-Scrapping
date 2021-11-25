import csv
import datetime
import os
import threading
import time
import traceback

import cfscrape
import json
import pymysql
from bs4 import BeautifulSoup
from pymysql.converters import escape_string

DB_HOST = "localhost"
DB_USER = 'root'
DB_PW = ''
DB_NAME = 'ZooplaUK_Commercial'
TABLE_NAME = 'ZooplaUK_Commercial'
forbidden = False
semaphore = threading.Semaphore(1)
write = threading.Semaphore(1)
outcsv = "Out-Zoopla-UK-Commercial.csv"
errorfile = "Error-Zoopla-UK-Commercial.txt"
headers = ["Name", "PriceEUR", "PricePerArea", "FloorPlan", "Location", "Contact", "Description", "URL", "Features",
           "Images"]
scraped = []
wait403 = 10  # time to wait in case of 403 error in seconds


def main():
    global semaphore, scraped, forbidden
    logo()
    threadcount = input("Please enter number of threads: ")
    if threadcount == "":
        threadcount = 1
    else:
        threadcount = int(threadcount)
    while True:
        start_url = "https://www.zoopla.co.uk/for-sale/commercial/property/london/?page_size=100"
        semaphore = threading.Semaphore(threadcount)
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
        print(datetime.datetime.now(), "Already scraped listings", scraped)
        print(datetime.datetime.now(), "Loading data...")
        hope_soup = get(start_url)
        while "403 Forbidden" in hope_soup.text:
            print(datetime.datetime.now(), "======403 Forbidden======")
            forbidden = True
            try:
                hope_soup = get(start_url)
            except:
                pass
            time.sleep(wait403)
        forbidden = False
        threads = []
        while len(hope_soup.find_all('a', string="Next")) != 0:
            print(datetime.datetime.now(), "Home URL", getText(hope_soup, 'span', 'listing-results-utils-count'),
                  start_url)
            for li in hope_soup.find_all('li', {"data-listing-id": True}):
                lid = li["data-listing-id"]
                url = f'https://www.zoopla.co.uk/overseas/details/{lid}/'
                if url not in scraped:
                    t = threading.Thread(target=scrape, args=(url,))
                    threads.append(t)
                    t.start()
                else:
                    print(datetime.datetime.now(), "Already scraped", lid)
            while "403 Forbidden" in hope_soup.text:
                forbidden = True
                print(datetime.datetime.now(), f"403 Forbidden! Retrying after {wait403} seconds...")
                time.sleep(wait403)
                try:
                    hope_soup = get(start_url)
                except:
                    pass
            forbidden = False
            start_url = "https://www.zoopla.co.uk" + hope_soup.find('a', string="Next")['href']
            hope_soup = get(start_url)
        for thread in threads:
            thread.join()
        print(datetime.datetime.now(), "Done with scraping, now adding stuff to DB.")
        try:
            handler = DBHandler()
            with open(outcsv, encoding='utf8', errors='ignore') as outfile:
                rows = [row for row in csv.DictReader(outfile)]
                handler.bulkInsert(rows)
            print(datetime.datetime.now(), "Done with DB insertion! Now waiting for 24 hrs")
        except:
            traceback.print_exc()
            print(datetime.datetime.now(), "Error in DB insertion!")
        time.sleep(86400)


def scrape(url):
    global forbidden
    with semaphore:
        try:
            print(datetime.datetime.now(), "Working on", url)
            soup = get(url)
            forbidden = retry = "403 Forbidden" in soup.text
            while forbidden:
                print("403 Forbidden")
                time.sleep(wait403)
            if retry:
                soup = get(url)
            data = {
                "Name": soup.find('title').text,
                "PriceEUR": getPrice(soup, "EUR"),
                "Location": getText(soup, 'h2', 'ui-property-summary__address'),
                "Contact": getText(soup, 'p', 'ui-agent__tel ui-agent__text').split("  ")[-1],
                "Description": getText(soup, 'div', 'dp-description__text'),
                "PricePerArea": getText(soup, 'p', 'ui-pricing__area-price').replace("\u00a3", "EUR ").replace("(",
                                                                                                               "").replace(
                    ")", ""),
                "URL": url,
                "FloorPlan": " | ".join(
                    [a['href'] for a in soup.find_all('a', {'id': "ui-modal-gallery-trigger-floorplan"})]),
                "Features": " | ".join(
                    [li.text.strip().replace("\n", " ") for li in
                     soup.find_all('li', {"class": "dp-features-list__item"})]),
                "Images": " | ".join([img['src'] for img in soup.find_all('img', {'class': 'dp-gallery__image'})]),
            }
            print(datetime.datetime.now(), json.dumps(data, indent=4))
            with open(f"./JSON/{url.split('/')[-2]}.json", 'w', encoding='utf8', errors='ignore') as outfile:
                json.dump(data, outfile, indent=4)
            append(data)
            scraped.append(url.split("/")[-2])
        except:
            print(datetime.datetime.now(), "Error on", url)
            with open(errorfile, 'a') as efile:
                efile.write(url + "\n")
            traceback.print_exc()


def getPrice(soup, currency):
    if currency == "EUR":
        price = getText(soup, 'p', "ui-pricing__main-price ui-text-t4")[1:].replace(",", "")
    else:
        price = getText(soup, 'p', "ui-pricing__alt-price")[3:].replace(",", "")
    try:
        return int(price)
    except:
        print("Error in parsing price", price)
        return 0


def append(data):
    with write:
        with open(outcsv, 'a', newline="", encoding='utf8', errors='ignore') as outfile:
            csv.DictWriter(outfile, fieldnames=headers).writerow(data)


def getText(soup, tag, Class):
    try:
        return soup.find(tag, {'class': Class}).text.strip()
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
            print(datetime.datetime.now(), "Please open connection first!")
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

    @staticmethod
    def getInt(value):
        try:
            return int(value)
        except Exception:
            return 0

    def createTable(self):
        self.executeSQL(f"""CREATE TABLE IF NOT EXISTS {self.TABLE_NAME} (
        Name TEXT, 
        PriceEUR INTEGER, 
        FloorPlan TEXT,
        PricePerArea TEXT,
        Location TEXT,
        Contact TEXT,
        Description Text,
        URL Text,
        Features Text,
        Images Text
        )""")

    def createQuery(self, data):
        return f"""INSERT INTO {self.TABLE_NAME} (Name, PriceEUR, FloorPlan, PricePerArea, Location, Contact, Description,
         URL, Features, Images) VALUES 
        ('{escape_string(data["Name"])}','{data["PriceEUR"]}','{data["FloorPlan"]}','{data["PricePerArea"]}',
        '{escape_string(data["Location"])}','{data["Contact"]}','{escape_string(data["Description"])}',
        '{escape_string(data["URL"])}','{escape_string(data["Features"])}','{data["Images"]}');"""

    def insert(self, data):
        if not self.exists(data['URL']):
            self.executeSQL(self.createQuery(data))
            print(datetime.datetime.now(), data['URL'], "inserted!")
        else:
            print(datetime.datetime.now(), data['URL'], 'already in db!')

    def bulkInsert(self, rows):
        self.scraped = [row['URL'] for row in self.getAllData()]
        print(datetime.datetime.now(), "Already scraped", self.scraped)
        for row in rows:
            if row['URL'] not in self.scraped:
                self.executeSQL(self.createQuery(row))
                print(datetime.datetime.now(), row['URL'], "inserted!")
            else:
                print(datetime.datetime.now(), row['URL'], "already exists!")

    def getAllData(self):
        sql = f"SELECT * FROM {self.TABLE_NAME}"
        return self.executeSQL(sql).fetchall()


def logo():
    os.system('cls')
    os.system('color 0a')
    print(rf"""
        __________                   .__          
        \____    /____   ____ ______ |  | _____   
          /     //  _ \ /  _ \\____ \|  | \__  \  
         /     /(  <_> |  <_> )  |_> >  |__/ __ \_
        /_______ \____/ \____/|   __/|____(____  /
                \/            |__|             \/ 
===========================================================
      zoopla.co.uk (US) scraper by github.com/evilgenius786
===========================================================
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
