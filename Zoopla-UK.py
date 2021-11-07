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
headers = ["Name", "Price (EUR)", "Floor Plan", "Location", "Contact", "Description", "URL","Features1", "Features2", "Images",
           "Nearby amenities"]
scraped = []


def scrape(url, contact=""):
    with semaphore:
        try:
            print("Working on", url)
            soup = get(url)

            if "403 Forbidden" in soup.text:
                print("403 Forbidden", url)
                return
            js = json.loads(soup.find('script', {'id': '__NEXT_DATA__'}).string)
            ld = js['props']['pageProps']['data']['listingDetails']
            data = {
                "Name": soup.find('title').text,
                "Price (EUR)": int(soup.find('span', {'data-testid': "price"}).text[1:].replace(',', '')),
                "Floor Plan": [f"https://lid.zoocdn.com/u/2400/1800/{x['filename']}" for x in ld['floorPlan']['image']] if ld['floorPlan']['image'] is not None else [],
                "Location": soup.find('span', {'data-testid': 'address-label'}).text,
                "Contact": contact,
                "Description": ld['metaDescription'],
                "URL": url,
                "Features1": ld['features']['bullets'],
                "Features2": [span.text for span in
                              soup.find('span', {'data-testid': "beds-label"}).find_parent('div').find_parent(
                                  'div').find_all('span')] if "beds-label" in str(soup) else [],
                "Images": [f"https://lid.zoocdn.com/u/2400/1800/{x['filename']}" for x in ld['propertyImage']],
                "Nearby amenities": [li.text for li in
                                     soup.find('ul', {"data-testid": "amenities-list"}).find_all('li')]
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
        start_url = "https://www.zoopla.co.uk/for-sale/property/london/"
    # scraped = [x.replace(".json", "") for x in os.listdir('./json')]
    with open(outcsv, encoding='utf8', errors='ignore') as ofile:
        for line in csv.DictReader(ofile):
            scraped.append(line['URL'])
    print("Already scraped listings", scraped)
    if "page_size" not in start_url:
        start_url += "&page_size=100" if "?" in start_url else "?page_size=100"
    print("Loading data...")
    home_soup = get(start_url)
    if "403 Forbidden" in home_soup.text:
        print("======403 Forbidden======")
        return
    threads = []
    while len(home_soup.find_all('a', string="Next >")) != 0:
        print("Home URL", start_url)
        for div in home_soup.find_all('div', {"data-testid": 'search-result'}):
            a = div.find('a', {"data-testid": "listing-details-link"})
            url = f'https://www.zoopla.co.uk{a["href"]}'
            if url not in scraped:
                t = threading.Thread(target=scrape, args=(url, div.find('a', {"data-testid": "agent-phone-number"}).text,))
                threads.append(t)
                t.start()
            else:
                print("Already scraped", a['href'])
        start_url = "https://www.zoopla.co.uk" + home_soup.find('a', string="Next >")['href']
        home_soup = get(start_url)
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
      zoopla.co.uk (UK) scraper by github.com/evilgenius786
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
