import csv
import threading
import traceback

import json
import os

import cfscrape
from bs4 import BeautifulSoup

semaphore = threading.Semaphore(1)
write = threading.Semaphore(1)
outcsv = "out.csv"
errorfile = "error.txt"
headers = ["Name", "Price (EUR)", "Price (USD)", "Price/Area", "Location", "Contact", "Description", "URL", "Features",
           "Images"]
scraped = []


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
                "Price (EUR)": int(getText(soup, 'p', "ui-pricing__main-price ui-text-t4")[1:].replace(",", "")),
                "Price (USD)": int(getText(soup, 'p', "ui-pricing__alt-price")[3:].replace(",", "")),
                "Price/Area": getText(soup, 'p', 'ui-pricing__area-price').replace("(", "").replace(")", ""),
                "Location": getText(soup, 'h2', 'ui-property-summary__address'),
                "Contact": getText(soup, 'p', 'ui-agent__tel ui-agent__text').split("  ")[-1],
                "Description": getText(soup, 'div', 'dp-description__text'),
                "URL": url,
                "Features": [li.text.strip() for li in soup.find_all('li', {"class": "dp-features-list__item"})],
                "Images": [img['src'] for img in soup.find_all('img', {'class': 'dp-gallery__image'})],
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


def getText(soup, tag, Class):
    try:
        return soup.find(tag, {'class': Class}).text.strip()
    except:
        return ""


def get(url):
    return BeautifulSoup(cfscrape.create_scraper().get(url).text, 'lxml')


def logo():
    os.system('color 0a')
    print(f"""
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
