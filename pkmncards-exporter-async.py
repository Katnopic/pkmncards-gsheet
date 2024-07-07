import gspread
from oauth2client.service_account import ServiceAccountCredentials
import csv
import requests
import aiohttp
import asyncio
from bs4 import BeautifulSoup
import csv
from datetime import datetime
import os

google_api_scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
base_url = 'https://pkmncards.com/'


def remove_csv_file(filename):
    print("Removing local csv file...")
    os.remove(filename)
    print("Local csv file removed")

def get_cards_from_page(url, page_num):
    response = requests.get(url + "/page/" + str(page_num), verify=False)
    soup = BeautifulSoup(response.content, 'html.parser')

    card_div = soup.find('main', class_='content')
    return card_div.find_all('article', class_='type-pkmn_card')

def generate_csv_card_row(card_info, card_url):
    card_info_soup = BeautifulSoup(card_info, 'html.parser')
    card_info_div = card_info_soup.find('div', class_='card-tabs')

    card_number = card_info_div.find('span', class_="number").text
    card_name = card_info_div.find('div', class_="name-hp-color").find('span', class_="name").text
    card_set_major = card_info_div.find('span', title="Series").text.strip()
    card_set_minor = card_info_div.find('span', title="Set").text.strip()
    card_illustrator = card_info_div.find('a', title="Illustrator").text
    card_release_date = card_info_div.find('div', class_="release-meta").find('span', class_="date").text
    card_cleaned_release_date_string = card_release_date.strip("↘ ")
    date_format = "%b %d, %Y"
    card_parsed_release_date = datetime.strptime(card_cleaned_release_date_string, date_format)

    card_name_final = card_name + "#" + card_number
    card_set_final = card_set_major + " - " + card_set_minor
    card_illustrator_final = card_illustrator
    card_releasedate_final = card_parsed_release_date
    card_link_final = card_url

    return [card_name_final, card_set_final, card_illustrator_final, card_releasedate_final, card_link_final]

def create_gsheet_from_csv(credentials_file, gsheet_filename, csv_filename, drive_folder_id):
    credentials = ServiceAccountCredentials.from_json_keyfile_name(credentials_file, google_api_scope)
    client = gspread.authorize(credentials)

    print("Creating spreadsheet from CSV")

    spreadsheet = client.create(gsheet_filename, folder_id=drive_folder_id)

    worksheet = spreadsheet.get_worksheet(0)

    with open(csv_filename, 'r') as f:
        values = [r for r in csv.reader(f)]
        worksheet.update(values)

    print(f"Spreadsheet created: {spreadsheet.url}")

async def generate_csv(uri, csv_filename):
    page_counter = 1
    print(f"Getting all card based on filter: {uri}")

    print("Scanning first page...")
    cards = get_cards_from_page(base_url + uri, page_counter)

    with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
        csv_writer = csv.writer(csvfile)
        csv_writer.writerow(['Name', 'Set', 'Illustrator', 'Release Date','Link'])

        while cards:
            card_urls = [card.find('a')['href'] for card in cards]
            async with aiohttp.ClientSession() as session:
                tasks = [fetch_data(session, url) for url in card_urls]
                results = await asyncio.gather(*tasks)

            for result in results:
                csv_writer.writerow(result)

            page_counter = page_counter + 1
            print(f"Scanning page number {page_counter}...")
            cards = get_cards_from_page(base_url + uri, page_counter)

    print(f"Finished scanning cards for filter: {uri}")


async def fetch_data(session, url):
    async with session.get(url, verify_ssl=False) as response:
        # Return the JSON content of the response using 'response.json()'
        card_info_soup = BeautifulSoup(await response.text(), 'html.parser')
        card_info_div = card_info_soup.find('div', class_='card-tabs')

        card_number = card_info_div.find('span', class_="number").text
        card_name = card_info_div.find('div', class_="name-hp-color").find('span', class_="name").text
        card_set_major = card_info_div.find('span', title="Series").text.strip()
        card_set_minor = card_info_div.find('span', title="Set").text.strip()
        card_illustrator = card_info_div.find('a', title="Illustrator").text
        card_release_date = card_info_div.find('div', class_="release-meta").find('span', class_="date").text
        card_cleaned_release_date_string = card_release_date.strip("↘ ")
        date_format = "%b %d, %Y"
        card_parsed_release_date = datetime.strptime(card_cleaned_release_date_string, date_format)

        card_name_final = card_name + "#" + card_number
        card_set_final = card_set_major + " - " + card_set_minor
        card_illustrator_final = card_illustrator
        card_releasedate_final = card_parsed_release_date
        card_link_final = url

        return [card_name_final, card_set_final, card_illustrator_final, card_releasedate_final, card_link_final]

async def main():
    # TODO: as parameter?
    credentials_file = 'credentials.json'
    uri = "pokemon/cleffa"
    drive_folder_id = "1zYdS8GL1kttrMAR7glcMdgWOMmWKdoey"

    page_counter = 1
    uri = uri[:-1] if uri.endswith('/') else uri
    gsheet_filename = uri.replace('/', '-')
    csv_filename = gsheet_filename + '.csv'

    await generate_csv(uri, csv_filename)

    create_gsheet_from_csv(credentials_file, gsheet_filename, csv_filename, drive_folder_id)

    remove_csv_file(csv_filename)

if __name__ == "__main__":
    asyncio.run(main())
