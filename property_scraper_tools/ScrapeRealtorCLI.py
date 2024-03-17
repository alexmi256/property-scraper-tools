import argparse
import json
import logging
import sqlite3
from contextlib import closing
from datetime import date
from math import ceil
from pathlib import Path
from random import randint
from time import sleep
from typing import Optional

from queries import RealtorAPI
from RealtorJSONtoSQLAnalyzer import RealtorJSONtoSQLAnalyzer
from requests import HTTPError
from tqdm import tqdm
from utils import CITIES, SORT_VALUES

logging.basicConfig(
    level=logging.INFO, handlers=[logging.FileHandler("scrape_realtor_cli.log"), logging.StreamHandler()]
)

logger = logging.getLogger(__name__)


class RealtorRawScraper:
    def __init__(self, city_name: str, db_type: str, database_file: Optional[Path] = None, create_db: bool = False):
        self.city = city_name
        self.create_db = create_db
        self.db_type = db_type
        self.parse_date = date.today()
        self.database_file = database_file if database_file else f"{city_name}_{db_type}_{self.parse_date}.sqlite"

        self.parsed_mls_numbers = []

        self.connection = sqlite3.connect(self.database_file)
        self.min_sleep_time = 100
        self.total_parsed = 0

        self.api = RealtorAPI()

        if create_db and db_type == "raw":
            with closing(self.connection.cursor()) as cursor:
                cursor.execute(
                    "CREATE TABLE IF NOT EXISTS listings (id INTEGER PRIMARY KEY, details TEXT NOT NULL, last_updated TEXT NOT NULL)"
                )
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

    def write_response_results_to_raw_db(self, response: dict):
        """
        Dumps the listing responses from realtor.ca as a json string blob

        :param response: The raw response from realtor.ca
        :return:
        """
        logger.info(response["Paging"])
        scraped_date_str = str(self.parse_date)

        response_data = [(x["Id"], json.dumps(x), scraped_date_str) for x in response["Results"]]
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

    def parse_responses_and_update_db(self, response: dict):
        """
        Converts the raw responses and appends them to either a minimal or full db

        :param response: The raw response from realtor.ca
        :return:
        """
        logger.info(response["Paging"])

        response_data = response["Results"]
        batch_of_parsed_mls_numbers = [x.get("MlsNumber") for x in response_data]
        number_already_parsed = len(set(self.parsed_mls_numbers).intersection(batch_of_parsed_mls_numbers))
        if number_already_parsed > 2:
            logger.info(
                f"{number_already_parsed} of the {len(batch_of_parsed_mls_numbers)} items we parsed this batch had already been parsed"
            )

        self.parsed_mls_numbers.extend(batch_of_parsed_mls_numbers)

        if not response_data:
            logger.error(f"Response data had 0 listings")

        analyzer = RealtorJSONtoSQLAnalyzer(
            self.database_file,
            city=self.city,
            item_mutator=RealtorJSONtoSQLAnalyzer.data_mutator,
            auto_convert_simple_types=True,
        )

        price_data = analyzer.get_existing_price_data(self.database_file, batch_of_parsed_mls_numbers)

        analyzer.insert_listings_into_db(
            response_data,
            db_name=self.database_file,
            parsed_date=self.parse_date,
            price_data=price_data,
            add_computed_columns=True,
            minimal_config=self.db_type == "minimal",
        )
        self.total_parsed += len(response_data)

        logger.info(f'Parsed {self.total_parsed}/{response["Paging"]["TotalRecords"]}')

    def parse_listings(self):
        latitude_min = CITIES[self.city]["LatitudeMin"]
        latitude_max = CITIES[self.city]["LatitudeMax"]
        longitude_min = CITIES[self.city]["LongitudeMin"]
        longitude_max = CITIES[self.city]["LongitudeMax"]
        raw_responses = []

        coords = [
            latitude_min,
            latitude_max,
            longitude_min,
            longitude_max,
        ]
        total_pages = 1

        try:
            # Parse the first page because it contains details about how many pages there are
            response = self.api.get_property_list(
                latitude_min, latitude_max, longitude_min, longitude_max, current_page=1
            )

        except HTTPError:
            logger.error(f"Failed retrieving response for first page of {self.city} ({coords})")
            self.connection.close()
            raise

        total_pages = ceil(response["Paging"]["TotalRecords"] / response["Paging"]["RecordsPerPage"])

        # Insert the date for the first page
        if self.db_type == "raw":
            self.write_response_results_to_raw_db(response)
        else:
            # TODO: Support parsing the whole db and then analyzing responses to create a DB
            if self.create_db:
                raise Exception("Currently analyzing all responses and creating a new DB is not supported")
                # raw_responses.extend(response["Results"])
            else:
                self.parse_responses_and_update_db(response)

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

                    # Insert the date for the current page
                    if self.db_type == "raw":
                        self.write_response_results_to_raw_db(response)
                    else:
                        # TODO: Support parsing the whole db and then analyzing responses to create a DB
                        if self.create_db:
                            raise Exception("Currently analyzing all responses and creating a new DB is not supported")
                            # raw_responses.extend(response["Results"])
                        else:
                            self.parse_responses_and_update_db(response)

                    success = True
                # Too many damn errors to handle
                except Exception:
                    logger.error(f"Attempt #{attempts}: Error occurred on city: {self.city}")
                    attempts += 1
                    if attempts > 4:
                        raise Exception("Too many failed attempts. Refresh cookies and try again")
                finally:
                    self.cooldown(success)

        logger.info(f"Completed while parsing {total_pages} pages and {len(self.parsed_mls_numbers)} listings")

        if self.create_db:
            logger.info(f"Analyzing {len(raw_responses)} listings in order to create a new DB")

        self.api.save_cookies()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="Realtor.ca Scraper and Database Updater",
        description="""
        Downloads Realtor.ca data for an area and stores it in a semi efficient database
        """,
    )

    parser.add_argument(
        "database",
        type=Path,
        help="Name of the database to store listings in",
    )

    parser.add_argument(
        "--city",
        default="montreal",
        choices=list(CITIES.keys()),
        help="Which city should be data be parsed for. Also affects the name of the output DB.",
    )

    parser.add_argument(
        "--store",
        default="full",
        choices=["raw", "full", "minimal"],
        help="What data format the JSON should be stored in the SQWLite DB. Raw unprocessed JSON, Processed JSON, or a minimal subset of Processed JSON",
    )

    parser.add_argument(
        "--new-db",
        action="store_true",
        help="TODO: Analyze and create a new DB from all the results we run the first time. Only useful if you have not created a db to store items before",
    )

    args = parser.parse_args()

    if not args.database.exists():
        raise Exception("Existing database does not exist")

    # realtor_json_to_sql_analyzer_config = {
    #     "city": args.city,
    #     "skip_existing_db_dates": args,
    #     "update_output_db": args,
    #     "minimal": args.minimal,
    #     "output_database": args,
    #     "auto_cast_value_types": args,
    #     "new_table_name": args,
    # }

    scraper = RealtorRawScraper(
        city_name=args.city, db_type=args.store, database_file=args.database, create_db=args.new_db
    )
    scraper.parse_listings()
