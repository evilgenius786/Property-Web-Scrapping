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
DB_NAME = 'Woods4Sale'
TABLE_NAME = 'Woods4Sale'

semaphore = threading.Semaphore(1)
write = threading.Semaphore(1)
outcsv = "Out-Woods4Sale.csv"
errorfile = "Error-Woods4Sale.txt"
headers = ['Name', 'PriceEUR', 'Area', 'Plan', 'Brochure', 'Description', 'URL', 'Images', 'Summary']

scraped = []

wait403 = 10
forbidden = False


def main():
    # scrape("https://www.woods4sale.co.uk/woodlands/northern-england/1804.htm")
    # return
    # handler = DBHandler()
    # with open(outcsv, encoding='utf8') as outfile:
    #     rows = [row for row in csv.DictReader(outfile)]
    #     handler.bulkInsert(rows)
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
        start_url = "https://www.woods4sale.co.uk/woodlands"
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
        threads = []
        try:
            # for i in range(1, total):
            print("URL", start_url)
            for a in soup.find_all('a', {"class": "button fit special"}):
                url = f"https://www.woods4sale.co.uk/{a['href']}"
                if url not in scraped:
                    t = threading.Thread(target=scrape, args=(url,))
                    threads.append(t)
                    t.start()
                    # print(url)
                else:
                    print("Already scraped", url)
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
                p = soup.find('h3', text="Price").find_next_sibling('p').text.strip().replace("£", "").replace(",", "")
                price = [int(s) for s in p.split() if s.isdigit()][0]

                data = {
                    "Name": soup.find('title').text.strip(),
                    "Summary": soup.find('h3', text="Summary").find_next_sibling('p').text.strip() if soup.find('h3',
                                                                                                                text="Summary") is not None else "",
                    "PriceEUR": price,
                    "Area": soup.find('h3', text="Size").find_next_sibling('p').text.strip(),
                    "Plan": " | ".join([f"https://www.woods4sale.co.uk/{img['src']}" for img in
                                        soup.find('h3', text="Maps & Plans").find_next_sibling('div').find_all('img')]),
                    "Brochure": soup.find('h3', text="Brochure").find_next_sibling('p').find('a', {
                        "title": "download print format"})['href'],
                    "Description": soup.find('h3', text="Description").find_next_sibling('p').text.strip(),
                    "URL": url,
                    "Images": " | ".join([f"https://www.woods4sale.co.uk/{img['src']}" for img in
                                          soup.find_all('img', {"alt": "Woodland For Sale", "class": "image fit"})]),
                }
                print(json.dumps(data, indent=4))
                with open(f"./JSON/{url.split('/')[-1].replace('.htm', '')}.json", 'w', encoding='utf8',
                          errors='ignore') as outfile:
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
        Area TEXT,
        Plan TEXT,
        Brochure Text,
        Description Text,
        URL Text,
        Images Text,
        Summary Text
        )""")

    def createQuery(self, data):
        return f"""INSERT INTO {self.TABLE_NAME} (Name, PriceEUR, Area, Plan, Brochure, Description,
         URL, Images, Summary) VALUES 
        ('{escape_string(data["Name"])}','{data["PriceEUR"]}','{escape_string(data["Area"])}','{data["Plan"]}',
        '{escape_string(data["Brochure"])}','{escape_string(data["Description"])}','{escape_string(data["URL"])}',
        '{escape_string(data["Images"])}','{escape_string(data["Summary"])}'); """

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
 __      __                    .___          _____   _________        .__           
/  \    /  \ ____    ____    __| _/ ______  /  |  | /   _____/_____   |  |    ____  
\   \/\/   //  _ \  /  _ \  / __ | /  ___/ /   |  |_\_____  \ \__  \  |  |  _/ __ \ 
 \        /(  <_> )(  <_> )/ /_/ | \___ \ /    ^   //        \ / __ \_|  |__\  ___/ 
  \__/\  /  \____/  \____/ \____ |/____  >\____   |/_______  /(____  /|____/ \___  >
       \/                       \/     \/      |__|        \/      \/            \/                   
=====================================================================================
               woods4sale.co.uk scraper by github.com/evilgenius786
=====================================================================================
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
