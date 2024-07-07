import re
import json

from bs4 import BeautifulSoup


FULL_NAME_SELECTOR = '.oh1'
BASE_INFO_SELECTOR = '#personDetails > div > div'
ALSO_SEEN_AS_CONTAINER_SELECTOR = '#personDetails > div.pl-md-1:nth-child(5) > div:nth-child(2) > div:nth-child(2) > div:nth-child(1) > div:nth-child(1)'
CURRENT_ADDRESS_SELECTOR = '#personDetails > div:nth-child(7) > div.col-12.col-sm-11.pl-sm-1 > div.row.pl-sm-2 > div > div:nth-child(1) > a'
CURRENT_ADDRESS_INFO_SELECTOR = 'div.row:nth-child(7) > div:nth-child(2) > div:nth-child(2) > div:nth-child(1) > div:nth-child(1) > div:nth-child(2)'
EMAIL_ADDRESSES_CONTAINER_SELECTOR = 'div.row:nth-child(13) > div:nth-child(2)'
PREVIOUS_ADDRESSES_CONTAINER_SELECTOR = 'div.row:nth-child(17) > div:nth-child(2)'
POSSIBLE_RELATIVES_CONTAINER_SELECTOR = '#personDetails > div:nth-child(22) > div.col-12.col-sm-11.pl-sm-1'
PHONE_NUMBERS_CONTAINER_SELECTOR = 'div.row:nth-child(9) > div:nth-child(2)'

AGE_AND_BORN_REGEX = re.compile(r'Age (\d+), Born (January|February|March|April|May|June|July|August|September|October|November|December) (\d{4})', re.IGNORECASE)
LIVES_IN_REGEX = re.compile(r'Lives in (.*)')


def try_selector(soup, selector, selector_name=None):
    try:
        return soup.select(selector)[0]
    except Exception:
        if selector_name:
            print(f'Could not find {selector_name} on page')
        return None


def assemble_address_from_children(text_children):
    address = ''

    for text_child in text_children:
        if not address:
            address += text_child
            continue

        if text_child != ',' and not address.endswith(' '):
            address += ' '
        
        address += text_child
    
    return address


def scrape_source(html):
    soup = BeautifulSoup(html, 'html.parser')

    full_name = try_selector(soup, FULL_NAME_SELECTOR, 'full name').text.strip()

    parts = full_name.split(' ')
    if len(parts) == 2:
        first_name, last_name = parts
        middle_initial = ''
    else:
        first_name, middle_initial, last_name = full_name.split(' ')

    base_info = try_selector(soup, BASE_INFO_SELECTOR, 'base info')
    base_info = list(base_info.children)
    base_info = [item.text.strip() for item in base_info if hasattr(item, 'text') and item.text.strip()]

    person_detail_containers = soup.select('#personDetails > div.row.pl-md-1')
    titled_containers = {container.select('div.h5')[0].text.strip(): container for container in person_detail_containers if container.select('div.h5')}

    try:
        age_and_born = base_info[1]
        lives_in = base_info[2]
        telephone = base_info[3]

        city = lives_in.split(', ')[0]
        state = lives_in.split(', ')[1]
    except Exception:
        print('Failed to extract either age and born, lives in, or telephone number from base info')
        age_and_born = lives_in = telephone = None

    age_and_born_match = AGE_AND_BORN_REGEX.match(age_and_born)
    if age_and_born_match:
        person_age = age_and_born_match.group(1)
        born_month = age_and_born_match.group(2)
        born_year = age_and_born_match.group(3)
    else:
        person_age = born_month = born_year = None
    
    lives_in_match = LIVES_IN_REGEX.match(lives_in)
    if lives_in_match:
        lives_in = lives_in_match.group(1)
    else:
        lives_in = None
    
    also_seen_as_container = titled_containers.get('Also Seen As', None)
    also_seen_as = []

    if also_seen_as_container:
        also_seen_as = also_seen_as_container.text.strip().split(', ')
        also_seen_as = [item for item in also_seen_as if not 'Also Seen As' in item]
    
    current_address_container = try_selector(soup, CURRENT_ADDRESS_SELECTOR, 'current address')
    current_address_children = list(current_address_container.children)
    current_address_text_children = [item.text.strip() for item in current_address_children if hasattr(item, 'text') and item.text.strip()]
    current_address = assemble_address_from_children(current_address_text_children)

    current_address_details = try_selector(soup, CURRENT_ADDRESS_INFO_SELECTOR, 'current address info')
    current_address_details = current_address_details.text.strip() if current_address_details else None

    email_addresses_container = titled_containers.get('Email Addresses', None)
    email_addresses = []

    if email_addresses_container:
        for i, email_address_item in enumerate(email_addresses_container.find_all('div', class_='col')):
            if i == 0:
                continue
            email_addresses.append(email_address_item.text.strip())
    
    previous_addresses_container = titled_containers.get('Previous Addresses', None)
    previous_addresses = []

    if previous_addresses_container:
        for previous_address_item in previous_addresses_container.find_all('a'):
            parent = previous_address_item.parent
            child_div = parent.find('div')

            child_div = list(child_div.children)
            non_empty_texts = [item.text.strip() for item in child_div if hasattr(item, 'text') and item.text.strip()]

            try:
                county = non_empty_texts[0]
                date_range = non_empty_texts[1]
            except Exception:
                print('Failed to extract county or date range from previous address')
                county = date_range = ''
            
            previous_address_children = list(previous_address_item.children)
            previous_address_text_children = [item.text.strip() for item in previous_address_children if hasattr(item, 'text') and item.text.strip()]
            previous_address = assemble_address_from_children(previous_address_text_children)

            previous_addresses.append({
                'address': previous_address,
                'county': county,
                'date_range': date_range
            })
    
    possible_relatives_container = titled_containers.get('Possible Relatives', None)
    possible_relatives = []

    if possible_relatives_container:
        for possible_relative_item in possible_relatives_container.find_all('a'):
            parent = possible_relative_item.parent
            child_div = parent.find('div')

            name = possible_relative_item.text.strip()
            age = child_div.text.strip()

            age = age.split('Age ')[1] if age.startswith('Age ') else age

            if '\n' in age:
                age = age.split('\n')[0].strip()

            possible_relatives.append({
                'name': name,
                'age': age
            })
    
    phone_numbers_container = titled_containers.get('Phone Numbers', None)
    phone_numbers = []

    if phone_numbers_container:
        for phone_number_item in phone_numbers_container.find_all('a'):
            parent = phone_number_item.parent
            child_div = parent.find('div')
            child_div.extract()

            number_and_type = parent.text.strip().split(' - ')

            number = number_and_type[0]
            type = number_and_type[1] if len(number_and_type) > 1 else ''

            child_div_children = list(child_div.children)
            non_empty_texts = [item.text.strip() for item in child_div_children if hasattr(item, 'text') and item.text.strip()]
            last_reported = [item for item in non_empty_texts if item.startswith('Last reported')]
            last_reported = last_reported[0].split('Last reported ')[1] if last_reported else ''

            phone_numbers.append({
                'number': number,
                'type': type,
                'last_reported': last_reported
            })
    
    return {
        'first_name': first_name,
        'middle_initial': middle_initial,
        'last_name': last_name,
        'age': person_age,
        'born_month': born_month,
        'born_year': born_year,
        'city': city,
        'state': state,
        'telephone': telephone,
        'also_seen_as': also_seen_as,
        'current_address': current_address,
        'email_addresses': email_addresses,
        'previous_addresses': previous_addresses,
        'possible_relatives': possible_relatives,
        'phone_numbers': phone_numbers,
        'current_address_details': ';'.join([value.strip() for value in ';'.join([item.strip() for item in current_address_details.split('\n') if item.strip()]).split('|') if value.strip()]) if current_address_details else ''
    }

