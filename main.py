import requests
import csv
import tqdm
import traceback
import logging
import concurrent.futures
import os
import scraper
from ratelimit import limits, sleep_and_retry
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

# Set the rate limit: 300 requests per minute
RATE_LIMIT = 300
RATE_PERIOD = 60  # seconds

INPUT_FOLDER = 'Inputs'
OUTPUT_FOLDER = 'Outputs'
GOOGLE_DRIVE_FOLDER_ID = '1f3tFSa_R1_YvW5py__8ohAAVgU4kG20-'

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', filename='failed.log')

@sleep_and_retry
@limits(calls=RATE_LIMIT, period=RATE_PERIOD)
def scrape_page(url):

    scrape_ninja_url = "https://scrapeninja.p.rapidapi.com/scrape"
    payload = {
        "url": url,
        "geo": "us",
        "followRedirects": 0,
        "statusNotExpected": [
            "403",
            "429"
        ],
        "textNotExpected": [
            "<title>Just a moment"
        ],
        "retryNum": 1
    }
    headers = {
        "x-rapidapi-key": 'f5099b8793msh9396441b8f28f11p122ca6jsn983a85f217d7',
        "x-rapidapi-host": "scrapeninja.p.rapidapi.com",
        "Content-Type": "application/json"
    }
    response = requests.post(scrape_ninja_url, json=payload, headers=headers)
    response.raise_for_status()
    response_json = response.json()
    
    if 'body' in response_json:
        return response_json['body']
    else:
        logging.error(f"Unexpected response structure: {response_json}")
        return None

def remove_cloudflare(url):

    return f'https://webcache.googleusercontent.com/search?q=cache:{url}'

def get_page_source(url):

    new_url = remove_cloudflare(url)
    try:
        return scrape_page(new_url)
    except requests.exceptions.HTTPError as e:
        status_code = e.response.status_code
        log_failed_url(url, status_code)
        logging.error(f'HTTPError: {e}')
        return f'HTTPError: {e}'
    except Exception as e:
        log_failed_url(url, 'unknown')
        logging.error(f'Error: {e}')
        logging.error(traceback.format_exc())
        return f'Error: {e}'
    return None

def log_failed_url(url, error_code):

    filename = f'failed_{error_code}.txt'
    with open(filename, 'a') as f:
        f.write(f'{url}\n')

def log_success_url(url):

    with open('success.txt', 'a') as f:
        f.write(f'{url}\n')

def scrape_url(url, writer, bar):

    html_or_error = get_page_source(url)
    if isinstance(html_or_error, str) and html_or_error.startswith('Error:'):
        writer.writerow([url, html_or_error])
        bar.update(1)
        return url, None, None
    elif html_or_error:
        html = html_or_error
        try:
            data = scraper.scrape_source(html)
            log_success_url(url)
            writer.writerow([
                url, '', data['first_name'], data['middle_initial'], data['last_name'], data['age'], 
                data['telephone'], data['city'], data['state'], data['born_month'], data['born_year'], 
                ';'.join(data['also_seen_as']), data['current_address'], data['current_address_details'], 
                ';'.join(['~'.join(x.values()) for x in data['phone_numbers']]), 
                ';'.join(data['email_addresses']), 
                ';'.join(['~'.join(x.values()) for x in data['previous_addresses']]), 
                ';'.join(['~'.join(x.values()) for x in data['possible_relatives']])
            ])
            bar.update(1)
            return url, html, data
        except Exception as e:
            error_msg = f'Error processing data: {e}'
            logging.error(error_msg)
            writer.writerow([url, error_msg])
            bar.update(1)
            return url, html, None
    else:
        writer.writerow([url, 'Unknown error'])
        bar.update(1)
        return url, None, None

def upload_to_google_drive(file_path, folder_id):
    credentials = service_account.Credentials.from_service_account_file('credentials.json', scopes=['https://www.googleapis.com/auth/drive'])
    service = build('drive', 'v3', credentials=credentials)

    file_metadata = {
        'name': os.path.basename(file_path),
        'parents': [folder_id]
    }
    media = MediaFileUpload(file_path, mimetype='text/csv')

    try:
        file = service.files().create(
            body=file_metadata,
            media_body=media,
            fields='id'
        ).execute()
        logging.info(f'Successfully uploaded {file_path} to Google Drive')
    except Exception as e:
        logging.error(f'Failed to upload {file_path} to Google Drive: {e}')

if __name__ == '__main__':
    input_files = sorted([f for f in os.listdir(INPUT_FOLDER) if f.startswith('input_') and f.endswith('.csv')])

    for input_file in input_files:
        file_number = input_file.split('_')[1].split('.')[0]
        output_file = f'{OUTPUT_FOLDER}/output_{file_number}.csv'

        with open(f'{INPUT_FOLDER}/{input_file}', 'r') as f:
            reader = csv.reader(f)
            headers = next(reader)
            urls = [row[0] for row in reader]

        file_exists = os.path.isfile(output_file)

        with open(output_file, 'a', newline='') as f:
            writer = csv.writer(f)
            if not file_exists:
                writer.writerow(['url', 'error'] + headers)

            bar = tqdm.tqdm(total=len(urls), desc=f'Scraping URLs from {input_file}', unit='URL')

            with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
                futures = {executor.submit(scrape_url, url, writer, bar): url for url in urls}
                
                for future in concurrent.futures.as_completed(futures):
                    url = futures[future]
                    try:
                        result = future.result()
                        if result[2] is not None:
                            logging.info(f'Successfully scraped {url}')
                    except Exception as e:
                        logging.error(f'Failed to scrape {url}')
                        logging.error(traceback.format_exc())
            bar.close()

        os.remove(f'{INPUT_FOLDER}/{input_file}')
        upload_to_google_drive(output_file, GOOGLE_DRIVE_FOLDER_ID)
