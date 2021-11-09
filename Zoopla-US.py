import csv
import os
import threading
import traceback

import cfscrape
import json
import pymysql
from bs4 import BeautifulSoup
from pymysql.converters import escape_string

DB_HOST = "localhost"
DB_USER = 'root'
DB_PW = ''
DB_NAME = 'ZooplaUS'
TABLE_NAME = 'ZooplaUS'


semaphore = threading.Semaphore(1)
write = threading.Semaphore(1)
outcsv = "Out-Zoopla-US.csv"
errorfile = "Error-Zoopla-US.txt"
headers = ["Name", "PriceEUR", "PriceUSD", "PricePerArea", "Location", "Contact", "Description", "URL", "Features",
           "Images"]
scraped = []


def main():
    global semaphore, scraped
    logo()
    if not os.path.isdir("json"):
        os.mkdir("json")
    if not os.path.isfile(outcsv):
        with open(outcsv, 'a', newline='') as outfile:
            csv.DictWriter(outfile, fieldnames=headers).writeheader()
    threadcount = input("Please enter number of threads: ")
    if threadcount == "":
        threadcount = 1
    else:
        threadcount = int(threadcount)
    semaphore = threading.Semaphore(threadcount)
    start_url = input("Please enter start URL: ")
    if start_url == "":
        start_url = "https://www.zoopla.co.uk/overseas/property/united-states/"
    # scraped = [x.replace(".json", "") for x in os.listdir('./json')]
    with open(outcsv, encoding='utf8', errors='ignore') as ofile:
        for line in csv.DictReader(ofile):
            scraped.append(line['URL'])
    print("Already scraped listings", scraped)
    if "page_size" not in start_url:
        start_url += "&page_size=100" if "?" in start_url else "?page_size=100"
    print("Loading data...")
    hope_soup = get(start_url)
    if "403 Forbidden" in hope_soup.text:
        print("======403 Forbidden======")
        return
    threads = []
    while len(hope_soup.find_all('a', string="Next")) != 0:
        print("Home URL", getText(hope_soup, 'span', 'listing-results-utils-count'), start_url)
        for li in hope_soup.find_all('li', {"data-listing-id": True}):
            lid = li["data-listing-id"]
            url = f'https://www.zoopla.co.uk/overseas/details/{lid}/'
            if url not in scraped:
                t = threading.Thread(target=scrape, args=(url,))
                threads.append(t)
                t.start()
            else:
                print("Already scraped", lid)
        start_url = "https://www.zoopla.co.uk" + hope_soup.find('a', string="Next")['href']
        hope_soup = get(start_url)
    for thread in threads:
        thread.join()
    print("Done with scraping, now adding stuff to DB.")
    handler = DBHandler()
    with open(outcsv) as outfile:
        rows = [row for row in csv.DictReader(outfile)]
        handler.bulkInsert(rows)
    print("Done with DB insertion!")


def scrape(url):
    with semaphore:
        try:
            print("Working on", url)
            soup = get(url)
            if "403 Forbidden" in soup.text:
                print("403 Forbidden", url)
                return
            data = {
                "Name": soup.find('title').text,
                "PriceEUR": int(getText(soup, 'p', "ui-pricing__main-price ui-text-t4")[1:].replace(",", "")),
                "PriceUSD": int(getText(soup, 'p', "ui-pricing__alt-price")[3:].replace(",", "")),
                "PricePerArea": getText(soup, 'p', 'ui-pricing__area-price').replace("(", "").replace(")", ""),
                "Location": getText(soup, 'h2', 'ui-property-summary__address'),
                "Contact": getText(soup, 'p', 'ui-agent__tel ui-agent__text').split("  ")[-1],
                "Description": getText(soup, 'div', 'dp-description__text'),
                "URL": url,
                "Features": " | ".join(
                    [li.text.strip() for li in soup.find_all('li', {"class": "dp-features-list__item"})]),
                "Images": " | ".join([img['src'] for img in soup.find_all('img', {'class': 'dp-gallery__image'})]),
            }
            print(json.dumps(data, indent=4))
            with open(f"./json/{url.split('/')[-2]}.json", 'w', encoding='utf8', errors='ignore') as outfile:
                json.dump(data, outfile, indent=4)
            append(data)
            scraped.append(url.split("/")[-2])
        except:
            print("Error on", url)
            with open(errorfile, 'a') as efile:
                efile.write(url + "\n")
            traceback.print_exc()


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
        return f"""INSERT INTO {self.TABLE_NAME} (Name, PriceEUR, PriceUSD, PricePerArea, Location,Contact, Description,
         URL, Features, Images) VALUES 
        ('{escape_string(data["Name"])}','{data["PriceEUR"]}','{data["PriceUSD"]}','{data["PricePerArea"]}',
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
    os.system('color 0a')
    print(rf"""
        __________                   .__          
        \____    /____   ____ ______ |  | _____   
          /     //  _ \ /  _ \\\\____ \|  | \__  \  
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
