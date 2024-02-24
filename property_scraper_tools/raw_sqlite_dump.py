import json
import logging
import sqlite3
from contextlib import closing
from datetime import date
from math import ceil
from pprint import pprint
from random import randint
from time import sleep

from queries import RealtorAPI
from requests import HTTPError
from tqdm import tqdm

logging.basicConfig(level=logging.DEBUG)

logger = logging.getLogger(__name__)
#
# with open("property_search_post.json", "r") as f:
#     response = json.load(f)

SORT_VALUES = {
    "Newest": "6-D",
    "Oldest": "6-A",
    "Lowest price": "1-A",
    "Highest price": "1-D",
}


class RealtorRawScraper:
    def __init__(self, db_name="mls_raw.db"):
        self.connection = sqlite3.connect(f"mls_raw_{str(date.today())}.db")
        self.min_sleep_time = 100
        self.total_parsed = 0
        self.parsed_mls_numbers = []
        self.api = RealtorAPI()

        with closing(self.connection.cursor()) as cursor:
            cursor.execute(
                "CREATE TABLE IF NOT EXISTS listings (id INTEGER PRIMARY KEY, details TEXT NOT NULL, last_updated TEXT NOT NULL)"
            )
            # cursor.execute("CREATE TABLE IF NOT EXISTS parse_times (page_number INTEGER, parse_time TEXT NOT NULL)")
            self.connection.commit()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.connection.close()

    def cooldown(self, success: bool = True):
        sleep_time = randint(
            # WARN: If you set cookies from the browser this can be pretty low at 100s/page
            self.min_sleep_time if success else self.min_sleep_time * 7,
            self.min_sleep_time * 2 if success else self.min_sleep_time * 9,
        )
        logger.debug(f"Sleeping {sleep_time} seconds")
        sleep(sleep_time)

    def write_response_results_to_db(self, response, partition: str):
        """

        :param response: The raw response from realtor.ca
        :param partition: A string representation of today's date
        :return:
        """
        logger.info(response["Paging"])

        response_data = [(x["Id"], json.dumps(x), partition) for x in response["Results"]]
        batch_of_parsed_mls_numbers = [x.get("MlsNumber") for x in response["Results"]]
        number_already_parsed = len(set(self.parsed_mls_numbers).intersection(batch_of_parsed_mls_numbers))
        if number_already_parsed > 2:
            logger.info(
                f"{number_already_parsed} of the {len(batch_of_parsed_mls_numbers)} items we parsed this batch had already been parsed"
            )

        self.parsed_mls_numbers.extend(batch_of_parsed_mls_numbers)

        if not response_data:
            logger.error(f"Response data had 0 listings")
        with closing(self.connection.cursor()) as cursor:
            cursor.executemany(
                "INSERT OR IGNORE INTO listings (id, details, last_updated) VALUES(?, ?, ?)",
                response_data,
            )
            self.connection.commit()
            self.total_parsed += len(response_data)

        logger.info(f'Parsed {self.total_parsed}/{response["Paging"]["TotalRecords"]}')

    def parse_listings(self, city="Montreal, QC"):
        latitude_min = "45.32146"
        latitude_max = "45.79359"
        longitude_min = "-74.20945"
        longitude_max = "-73.23648"
        coords = [latitude_min, latitude_max, longitude_min, longitude_max]
        total_pages = 1
        current_date = str(date.today())

        try:
            # Parse the first page because it contains details about how many pages there are
            response = self.api.get_property_list(
                latitude_min, latitude_max, longitude_min, longitude_max, current_page=1
            )

            total_pages = ceil(response["Paging"]["TotalRecords"] / response["Paging"]["RecordsPerPage"])
            self.write_response_results_to_db(response, current_date)

        except HTTPError:
            logger.error(f"Failed retrieving response for first page of {city} ({coords})")
            self.connection.close()
            raise

        if total_pages > 50:
            logger.info(
                f"There are {total_pages} listed and we can only parse 50 so we will need to parse forwards and backwards"
            )
        if total_pages > 100:
            logger.warning(f"There are {total_pages} listed but we can only parse 100 so data will be missed")

        parse_forward = 50 if total_pages > 50 else total_pages
        parse_backward = total_pages - 50 if total_pages > 50 else 0

        pages_to_parse = [(page_number, "Newest") for page_number in range(2, parse_forward + 1)] + [
            (page_number, "Oldest") for page_number in range(1, parse_backward + 1)
        ]

        self.cooldown()

        # WARN: We can't seem to go past 50 pages, when this happens we get back zero results
        for page_number, sort_name in tqdm(pages_to_parse):
            sort_value = SORT_VALUES[sort_name]
            logger.info(f"Parsing page #{page_number} going by {sort_name} order")
            success = False
            attempts = 1

            while not success:
                try:
                    response = self.api.get_property_list(
                        latitude_min,
                        latitude_max,
                        longitude_min,
                        longitude_max,
                        current_page=page_number,
                        sort=sort_value,
                    )

                    self.write_response_results_to_db(response, current_date)

                    success = True
                # Too many damn errors to handle
                except Exception:
                    logger.error(f"Attempt #{attempts}: Error occurred on city: {city}")
                    attempts += 1
                    if attempts > 4:
                        raise Exception("Too many failed attempts. Refresh cookies and try again")
                finally:
                    self.cooldown(success)

        logger.info(f"Completed while parsing {total_pages} pages and {len(self.parsed_mls_numbers)} listings")
        self.api.save_cookies()

    def parse_listing_details(self, property_id, mls_reference_number):
        response = self.api.get_property_details(property_id, mls_reference_number)
        pprint(response)


scraper = RealtorRawScraper()
scraper.parse_listings()
# scraper.parse_listing_details(26418653, 13680165)
