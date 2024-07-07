import scraper
import requests
import csv
import json
import tqdm

import traceback

import logging

import concurrent.futures

from fake_useragent import UserAgent


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', filename='failed.log')


def scrape_page(url):
    user_agent = UserAgent()
    headers = {
        'User-Agent': user_agent.random,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate, br",
        "Alt-Used": "webcache.googleusercontent.com",
        "Connection": "keep-alive",
        "Cookie": "_ga_JB1DKYFLTX=GS1.1.1720318499.1.0.1720318499.0.0.0; _ga=GA1.1.252177526.1720318500",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "cross-site",
        "Sec-Fetch-User": "?1"
    }

    proxies = {
        "http": "http://us-pr.oxylabs.io:10000",
        "https": "http://us-pr.oxylabs.io:10000",
    }

    response = requests.get(url, headers=headers, proxies=proxies)
    response.raise_for_status()

    return response.text


def remove_cloudflare(url):
    return 'https://webcache.googleusercontent.com/search?q=cache:' + url


def get_page_source(url):
    new_url = remove_cloudflare(url)

    for i in range(5):
        try:
            return scrape_page(new_url)
        except Exception as e:
            print(f'Failed to scrape page {url} on attempt {i + 1}')
            print(e)
    print(f'Failed to scrape page {url} after final attempt')


if __name__ == '__main__':
    if False:
        url = 'https://www.truepeoplesearch.com/find/person/px4402l9rnn6r408nunn4'
        html = get_page_source(url)
        data = scraper.scrape_source(html)

        with open('data.json', 'w') as f:
            json.dump(data, f, indent=4)
        exit(0)
    
    with open('input.csv', 'r') as f:
        reader = csv.reader(f)
        headers = next(reader)

        urls = [row[0] for row in reader]
    
    with open('output.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(headers)

        bar = tqdm.tqdm(total=len(urls), desc='Scraping URLs', unit='URL')

        def scrape_url(url):
            html = get_page_source(url)
            data = scraper.scrape_source(html)
            bar.update(1)
            return html, data

        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            futures = []
            for url in urls:
                futures.append(executor.submit(scrape_url, url))
            
            for future in concurrent.futures.as_completed(futures):
                try:
                    result = future.result()
                    html = result[0]
                    data = result[1]

                    # Page URL,First Name,Middle Initial,Last Name,Age,Phone Number,City,State,Birth Month,Birth Year,Also Seen As,Current Address,Current Address Details,Phone Numbers,Email Addresses,Previous Addresses,Possible Relatives
                    writer.writerow([url, data['first_name'], data['middle_initial'], data['last_name'], data['age'], data['telephone'], data['city'], data['state'], data['born_month'], data['born_year'], ';'.join(data['also_seen_as']), data['current_address'], data['current_address_details'], ';'.join(['~'.join(x.values()) for x in data['phone_numbers']]), ';'.join(data['email_addresses']), ';'.join(['~'.join(x.values()) for x in data['previous_addresses']]), ';'.join(['~'.join(x.values()) for x in data['possible_relatives']])])
                except Exception as e:
                    logging.error(f'Failed to scrape {url}')
                    print(f'Failed to scrape {url}')
                    logging.error(traceback.format_exc())
                    continue
        
        bar.close()
