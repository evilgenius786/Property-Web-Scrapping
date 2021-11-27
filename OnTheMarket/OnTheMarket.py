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
DB_NAME = 'OnTheMarket'
TABLE_NAME = 'OnTheMarket'

semaphore = threading.Semaphore(1)
write = threading.Semaphore(1)
outcsv = "Out-OnTheMarket.csv"
errorfile = "Error-OnTheMarket.txt"
headers = ["Name", "PriceEUR", "Location", "Contact", "Description", "URL", "Features", "Images"]
scraped = []

wait403 = 10
forbidden = False


def main():
    # scrape("https://www.onthemarket.com/details/11139317/")
    # return
    global semaphore, scraped, forbidden
    logo()
    threadcount = input("Please enter number of threads: ")
    if threadcount == "":
        threadcount = 1
    else:
        threadcount = int(threadcount)
    while True:
        semaphore = threading.Semaphore(threadcount)
        start_url = "https://www.onthemarket.com/farms-land/property/england/"
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
        total = int(soup.find('ul', {"class": "pagination-tabs"}).find_all('li')[-2].text.strip())
        print("Total pages:", total)
        print("Listings per page:", 24)
        threads = []
        try:
            for i in range(1, total):
                print("URL", start_url)
                for a in soup.find_all('a', {"class": "more-details"}):
                    url = f"https://www.onthemarket.com{a['href']}"
                    if url not in scraped:
                        t = threading.Thread(target=scrape, args=(url,))
                        threads.append(t)
                        t.start()
                        # print(url)
                    else:
                        print("Already scraped", url)
                start_url = f"https://www.onthemarket.com/farms-land/property/england/?page={i}"
                # time.sleep(5)
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
                if retry:
                    soup = get(url)
                try:
                    price = int(getText(soup, 'span', 'class', 'price-data')[2:].replace(",", ""))
                except:
                    # traceback.print_exc()
                    price = 0
                js={"images":[]}
                for line in str(soup).split("\n"):
                    if line.startswith(' __OTM__.jsonData = '):
                        js = json.loads(line.replace(" __OTM__.jsonData = ","")[:-1])
                        break
                features = [x.text.strip() for x in soup.find('section', {"class": "property-icons"}).find_all('div')]
                try:
                    features.extend([x.text.strip() for x in
                                     soup.find('section', {"class": "property-features features-list"}).find_all('li')])
                except:
                    pass
                data = {
                    "Name": soup.find('h1').text.strip(),
                    "PriceEUR": price,
                    "Location": getText(soup, 'p', 'class', 'title-address'),
                    "Contact": getText(soup, 'div', 'class', 'agent-info-phone-link').replace("Call", ""),
                    "Description": getText(soup, 'div', 'class', "description-truncate"),
                    "URL": url,
                    "Features": " | ".join(features),
                    "Images": " | ".join([img['large-url'] for img in js['images']]),
                }
                print(json.dumps(data, indent=4))
                with open(f"./JSON/{url.split('/')[-2]}.json", 'w', encoding='utf8', errors='ignore') as outfile:
                    json.dump(data, outfile, indent=4)
                append(data)
                scraped.append(url)
            except:
                print("Error on", url)
                with open(errorfile, 'a') as efile:
                    efile.write(url + "\n")
                traceback.print_exc()


def append(data):
    with write:
        with open(outcsv, 'a', newline="", encoding='utf8', errors='ignore') as outfile:
            csv.DictWriter(outfile, fieldnames=headers).writerow(data)


def getText(soup, tag, attrib, Class):
    try:
        return soup.find(tag, {attrib: Class}).text.strip()
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
        Location TEXT,
        Contact TEXT,
        Description Text,
        URL Text,
        Features Text,
        Images Text
        )""")

    def createQuery(self, data):
        return f"""INSERT INTO {self.TABLE_NAME} (Name, PriceEUR, Location, Contact, Description,
         URL, Features, Images) VALUES 
        ('{escape_string(data["Name"])}','{data["PriceEUR"]}',
        '{escape_string(data["Location"])}','{data["Contact"]}','{escape_string(data["Description"])}',
        '{escape_string(data["URL"])}','{escape_string(data["Features"])}','{escape_string(data["Images"])}');"""

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
       ____        ______ __           __  ___              __          __ 
      / __ \ ____ /_  __// /_   ___   /  |/  /____ _ _____ / /__ ___   / /_
     / / / // __ \ / /  / __ \ / _ \ / /|_/ // __ `// ___// //_// _ \ / __/
    / /_/ // / / // /  / / / //  __// /  / // /_/ // /   / ,<  /  __// /_  
    \____//_/ /_//_/  /_/ /_/ \___//_/  /_/ \__,_//_/   /_/|_| \___/ \__/                     
================================================================================
         OnTheMarket.com scraper by github.com/evilgenius786
================================================================================
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
