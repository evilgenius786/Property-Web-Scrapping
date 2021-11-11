import csv
import os
import re
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
DB_NAME = 'Rightmove'
TABLE_NAME = 'Rightmove'

semaphore = threading.Semaphore(1)
write = threading.Semaphore(1)
outcsv = "Out-Rightmove.csv"
errorfile = "Error-Rightmove.txt"
headers = ["Name", "PriceUSD", "PricePerArea", "Location", "Contact", "Description", "URL", "Features",
           "Images", "Airports"]
scraped = []


def main():
    global semaphore, scraped
    while True:
        logo()
        if not os.path.isdir("json"):
            os.mkdir("json")
        if not os.path.isfile(outcsv):
            with open(outcsv, 'w', newline='') as outfile:
                csv.DictWriter(outfile, fieldnames=headers).writeheader()
        threadcount = input("Please enter number of threads: ")
        if threadcount == "":
            threadcount = 1
        else:
            threadcount = int(threadcount)
        semaphore = threading.Semaphore(threadcount)
        start_url = input("Please enter start URL: ")
        if start_url == "":
            start_url = "https://www.rightmove.co.uk/overseas-property-for-sale/USA.html"
        # scraped = [x.replace(".json", "") for x in os.listdir('./json')]
        with open(outcsv, encoding='utf8', errors='ignore') as ofile:
            for line in csv.DictReader(ofile):
                scraped.append(line['URL'])
        print("Already scraped listings", scraped)
        print("Loading data...")
        soup = get(start_url)
        perpage = int((re.search('numberOfPropertiesPerPage":"(.*?)","radius', str(soup.contents)).group(1)))
        total = int((getText(soup, "span", 'class', "searchHeader-resultCount")).replace(",", ""))
        print("Total pages:", total)
        print("Listings per page:", perpage)
        threads = []
        for i in range(0, total, perpage):
            print("URL", start_url)
            for div in soup.find_all('div', {"class": "l-searchResult is-list"}):
                lid = div['id'].split('-')[1]
                url = f'https://www.rightmove.co.uk/properties/{lid}/'
                if url not in scraped:
                    t = threading.Thread(target=scrape, args=(url,))
                    threads.append(t)
                    t.start()
                else:
                    print("Already scraped", lid)
            start_url = f"https://www.rightmove.co.uk/overseas-property-for-sale/USA.html?index={i}"
            time.sleep(5)
            soup = get(start_url)
        for thread in threads:
            thread.join()
        print("Done with scraping, now adding stuff to DB.")
        handler = DBHandler()
        with open(outcsv) as outfile:
            rows = [row for row in csv.DictReader(outfile)]
            handler.bulkInsert(rows)
        print("Done with DB insertion! Now waiting for 24 hrs")
        time.sleep(86400)


def scrape(url):
    with semaphore:
        try:
            print("Working on", url)
            soup = get(url)
            js = json.loads(soup.find_all('script')[-3].string.strip().replace('window.PAGE_MODEL = ', ''))
            features = js['propertyData']['keyFeatures']
            features.extend([f"{x['title']}: {x['primaryText']}" for x in js['propertyData']['infoReelItems']])
            data = {
                "Name": soup.find('title').text,
                "PriceUSD": int(js['propertyData']['prices']['primaryPrice'][6:].replace(",", "")),
                "PricePerArea": js['propertyData']['prices']['pricePerSqFt'].replace('\u00a3', "EUR ") if js['propertyData']['prices']['pricePerSqFt'] is not None else "",
                "Location": js['propertyData']['address']['displayAddress'],
                "Contact": js['propertyData']['contactInfo']['telephoneNumbers']['localNumber'],
                "Description": js['propertyData']['text']['description'],
                "URL": url,
                "Features": " | ".join(features).replace("\u00d7", "x"),
                "Images": " | ".join([img['url'] for img in js['propertyData']['images']]),
                "Airports": " | ".join(
                    [f"{x['name']} : {int(x['distance'])} {x['unit']}" for x in js['propertyData']['nearestAirports']])
            }
            print(json.dumps(data, indent=4))
            with open(f"./json/{url.split('/')[-2]}.json", 'w', encoding='utf8', errors='ignore') as outfile:
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
        return soup.find(tag, {'class': Class}).text.strip()
    except:
        return ""


def get(url):
    # with open('index.html') as ifile:
    #     return BeautifulSoup(ifile.read(), 'lxml')
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
        Airports TEXT, 
        PriceUSD INTEGER,
        PricePerArea TEXT,
        Location TEXT,
        Contact TEXT,
        Description Text,
        URL Text,
        Features Text,
        Images Text
        )""")

    def createQuery(self, data):
        return f"""INSERT INTO {self.TABLE_NAME} (Name, Airports, PriceUSD, PricePerArea, Location, Contact, Description,
         URL, Features, Images) VALUES 
        ('{escape_string(data["Name"])}','{data["Airports"]}','{data["PriceUSD"]}','{data["PricePerArea"]}',
        '{escape_string(data["Location"])}','{data["Contact"]}','{escape_string(data["Description"])}',
        '{escape_string(data["URL"])}','{escape_string(data["Features"])}','{data["Images"]}');"""

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
    __________ .__          .__       __                                   
    \______   \|__|   ____  |  |__  _/  |_   _____    ____  ___  __  ____  
     |       _/|  |  / ___\ |  |  \ \   __\ /     \  /  _ \ \  \/ /_/ __ \ 
     |    |   \|  | / /_/  >|   Y  \ |  |  |  Y Y  \(  <_> ) \   / \  ___/ 
     |____|_  /|__| \___  / |___|  / |__|  |__|_|  / \____/   \_/   \___  >
            \/     /_____/       \/              \/                     \/ 
=================================================================================
            rightmove.co.uk (US) scraper by github.com/evilgenius786
=================================================================================
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
