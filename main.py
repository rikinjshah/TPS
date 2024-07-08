import git
import scraper
import requests
import csv
import json
import tqdm
import traceback
import logging
import concurrent.futures
import os

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', filename='failed.log')

def scrape_page(url):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
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

    try:
        return scrape_page(new_url)
    except requests.exceptions.HTTPError as e:
        status_code = e.response.status_code
        log_failed_url(url, status_code)
        logging.error(f'HTTPError: {e}')
    except Exception as e:
        log_failed_url(url, 'unknown')
        logging.error(f'Error: {e}')

def log_failed_url(url, error_code):
    filename = f'failed_{error_code}.txt'
    with open(filename, 'a') as f:
        f.write(f'{url}\n')

def log_success_url(url):
    with open('success.txt', 'a') as f:
        f.write(f'{url}\n')

def push_to_github(file_path, commit_message):
    try:
        repo = git.Repo(os.getcwd())
        repo.git.add(file_path)
        repo.index.commit(commit_message)
        origin = repo.remote(name='origin')
        origin.push()
    except Exception as e:
        logging.error(f'Failed to push to GitHub: {e}')
        logging.error(traceback.format_exc())

if __name__ == '__main__':
    urls = [
        "https://www.truepeoplesearch.com/find/person/pxr4900ulunn0r4l4ur24",
        "https://www.truepeoplesearch.com/find/person/px4488nrrl2u0u4uuunrl",
        "https://www.truepeoplesearch.com/find/person/pr2u64lu4289rrr9r202",
        "https://www.truepeoplesearch.com/find/person/pxn0rnr8ln822rn2lur96",
        "https://www.truepeoplesearch.com/find/person/pxl8ll49r822nr26u4rn6",
        "https://www.truepeoplesearch.com/find/person/plnl8lll2n6n06680066",
        "https://www.truepeoplesearch.com/find/person/px8ln608l660l6r2n2n00",
        "https://www.truepeoplesearch.com/find/person/p4494l2ll228r044nu4r",
        "https://www.truepeoplesearch.com/find/person/p484r82r0unnrrlu4668",
        "https://www.truepeoplesearch.com/find/person/p88900096n0ln964l29l",
        "https://www.truepeoplesearch.com/find/person/pxul64urr2889unrll680",
        "https://www.truepeoplesearch.com/find/person/pxu8l692u90429r966n8u",
        "https://www.truepeoplesearch.com/find/person/px240l40l609n0nnr8r88",
        "https://www.truepeoplesearch.com/find/person/px409nl9u4rrr4rl602rr",
        "https://www.truepeoplesearch.com/find/person/prr4nr9r8lnlr06r04u4",
        "https://www.truepeoplesearch.com/find/person/pxl99l66u4r4uu0u02lrn"
    ]

    output_file = 'output.csv'
    file_exists = os.path.isfile(output_file)

    with open(output_file, 'a', newline='') as f:
        writer = csv.writer(f)

        # Define headers based on the data structure
        headers = [
            "url", "first_name", "middle_initial", "last_name", "age", 
            "telephone", "city", "state", "born_month", "born_year", 
            "also_seen_as", "current_address", "current_address_details", 
            "phone_numbers", "email_addresses", "previous_addresses", 
            "possible_relatives"
        ]

        # Write header only if the file is being created
        if not file_exists:
            writer.writerow(headers)

        bar = tqdm.tqdm(total=len(urls), desc='Scraping URLs', unit='URL')

        def scrape_url(url):
            html = get_page_source(url)
            if html:
                data = scraper.scrape_source(html)
                log_success_url(url)
                bar.update(1)
                return url, html, data
            else:
                bar.update(1)
                return url, None, None

        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            futures = [executor.submit(scrape_url, url) for url in urls]

            for future in concurrent.futures.as_completed(futures):
                try:
                    result = future.result()
                    url, html, data = result
                    if data:
                        writer.writerow([
                            url, data['first_name'], data['middle_initial'], data['last_name'], data['age'], 
                            data['telephone'], data['city'], data['state'], data['born_month'], data['born_year'], 
                            ';'.join(data['also_seen_as']), data['current_address'], data['current_address_details'], 
                            ';'.join(['~'.join(x.values()) for x in data['phone_numbers']]), 
                            ';'.join(data['email_addresses']), 
                            ';'.join(['~'.join(x.values()) for x in data['previous_addresses']]), 
                            ';'.join(['~'.join(x.values()) for x in data['possible_relatives']])
                        ])
                except Exception as e:
                    logging.error(f'Failed to scrape {url}')
                    logging.error(traceback.format_exc())
                    continue

        bar.close()

    # Push the output file to GitHub once after scraping all URLs
    push_to_github(output_file, "Update output.csv with scraped data")
