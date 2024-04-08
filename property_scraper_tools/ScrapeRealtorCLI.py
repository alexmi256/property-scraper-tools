import argparse
import json
import logging
import re
import sqlite3
from contextlib import closing
from datetime import datetime
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

from geopy import distance
from shapely import Point, Polygon

logging.basicConfig(
    level=logging.INFO, handlers=[logging.FileHandler("scrape_realtor_cli.log"), logging.StreamHandler()]
)

logger = logging.getLogger(__name__)


class RealtorRawScraper:
    def __init__(self, city_name: str, db_type: str, database_file: Optional[Path] = None, create_db: bool = False):
        self.city = city_name
        self.create_db = create_db
        self.db_type = db_type
        self.parse_date = datetime.now()
        self.database_file = (
            database_file if database_file else f"{city_name}_{db_type}_{self.parse_date.date()}.sqlite"
        )

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
        scraped_date_str = str(self.parse_date.date())

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

    def parse_listings(
        self, min_price: Optional[int] = None, max_price: Optional[int] = None, min_bedrooms: Optional[int] = None
    ):
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
                f"There are {total_pages} pages listed and we can only parse 50 so we will need to parse forwards and backwards"
            )
        if total_pages > 100:
            logger.warning(f"There are {total_pages} pages listed but we can only parse 100 so data will be missed")

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
                        price_min=min_price,
                        price_max=max_price,
                        bed_range=min_bedrooms,
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

    def parse_raw_listings_details(self, listings: list[dict], details_db: Path):
        previously_parsed_ids = []
        with closing(sqlite3.connect(details_db)) as connection:
            with closing(connection.cursor()) as cursor:
                cursor.execute(
                    "CREATE TABLE IF NOT EXISTS listings (id INTEGER PRIMARY KEY, details TEXT NOT NULL, last_updated TEXT NOT NULL)"
                )
                connection.commit()

            with closing(connection.cursor()) as cursor:
                results = cursor.execute("SELECT id FROM listings").fetchall()
                if results:
                    previously_parsed_ids = [x[0] for x in results]

        with closing(sqlite3.connect(details_db)) as connection:
            for listing in tqdm(listings):
                if listing["Id"] in previously_parsed_ids:
                    logger.debug(f"Already parsed {listing['Id']}")
                    continue
                # TODO: Retry mechanism
                # TODO: Remove listings which we already have recent data for withing x days, likely via other function
                try:
                    # Parse the first page because it contains details about how many pages there are
                    response = self.api.get_property_details(
                        property_id=listing["Id"], mls_reference_number=listing["MlsNumber"]
                    )
                    with closing(connection.cursor()) as cursor:
                        cursor.execute(
                            "INSERT OR REPLACE INTO listings (id, details, last_updated) VALUES(?, ?, ?)",
                            [listing["Id"], json.dumps(response), datetime.now().isoformat()],
                        )
                        connection.commit()

                except Exception:
                    logger.error(f"Failed retrieving details for Mls Number {listing['MlsNumber']} ({listing['Id']})")
                    self.cooldown(success=False)
                finally:
                    sleep(randint(7, 17))


def get_listings_from_db(
    db_file,
    min_price: int = 100000,
    max_price: int = 10000000,
    must_have_int_sqft: bool = False,
    must_have_price_change: bool = False,
    no_new_listings: bool = True,
    no_vacant_land: bool = True,
    no_high_rise: bool = True,
    within_area_of_interest: bool = True,
    min_metro_distance_meters: Optional[int] = None,
    min_bedroom: Optional[int] = None,
    min_sqft: Optional[int] = None,
    max_price_per_sqft: Optional[int] = None,
    last_updated_days_ago: Optional[int] = 7,
    has_garage: bool = False,
    has_parking_details: bool = False,
    has_upcoming_openhouse: bool = False,
    area_of_interest: Optional[list[tuple]] = None,
    points_of_interest: Optional[list[tuple]] = None,
    limit: int = -1,
) -> list[dict]:
    with closing(sqlite3.connect(db_file)) as connection:
        # This helps maintain the row as a dict
        connection.row_factory = sqlite3.Row
        with closing(connection.cursor()) as cursor:
            conditions = []
            if no_vacant_land:
                conditions.append(
                    "(Property_ZoningType IS NULL OR Property_ZoningType NOT IN ('Agricultural')) AND Property_Type != 'Vacant Land'"
                )
            if no_high_rise:
                conditions.append("Building_StoriesTotal IS NULL OR CAST (Building_StoriesTotal AS INTEGER) < 5")
            if no_new_listings:
                conditions.append("ComputedNewBuild IS NOT TRUE")
            if must_have_int_sqft:
                conditions.append("Building_SizeInterior IS NOT NULL")
            if must_have_price_change:
                # FIXME: Query the DB and find earliest date we have prices for
                conditions.append("PriceChangeDateUTC IS NOT NULL AND DATE(PriceChangeDateUTC) > DATE('2023-11-19')")
            if min_bedroom:
                conditions.append(f"Building_Bedrooms IS NULL OR Building_Bedrooms >= {min_bedroom}")
            if last_updated_days_ago:
                conditions.append(f"DATE(ComputedLastUpdated) >= DATE('now', '-{last_updated_days_ago} day')")
            if min_sqft:
                conditions.append(f"ComputedSQFT IS NULL OR ComputedSQFT >= {min_sqft}")
            if max_price_per_sqft:
                conditions.append(f"ComputedPricePerSQFT IS NULL OR ComputedPricePerSQFT <= {max_price_per_sqft}")
            if has_garage:
                conditions.append(f"Property_Parking LIKE '%Garage%'")
            elif has_parking_details:
                conditions.append(f"Property_Parking IS NOT NULL")

            conditions = [f"({x})" for x in conditions]

            columns_to_select = [
                "Id",
                "MlsNumber",
                "Property_Address_Latitude",
                "Property_Address_Longitude",
                "Property_Address_AddressText",
            ]

            where_clause = f"""
                Property_PriceUnformattedValue > {min_price} AND 
                Property_PriceUnformattedValue < {max_price} AND 
                {' AND '.join(conditions)}
            """

            if has_upcoming_openhouse:
                columns_to_select.append("FormattedDateTime")
                columns_to_select_str = ",\n".join(columns_to_select)
                query = f"""
                   WITH open_house_unnested AS (
                       SELECT MlsNumber,
                              value AS OpenHouseGeneratedId
                         FROM Listings,
                              json_each(OpenHouse) 
                        WHERE OpenHouse IS NOT NULL
                   ),
                   open_house_in_future AS (
                       SELECT MlsNumber,
                              FormattedDateTime
                         FROM open_house_unnested
                              JOIN
                              OpenHouse USING (
                                  OpenHouseGeneratedId
                              )
                        WHERE DATE(StartDateTime) >= DATE('now') 
                        GROUP BY MlsNumber
                   )
                   SELECT {columns_to_select_str}
                     FROM open_house_in_future
                          JOIN
                          Listings USING (
                              MlsNumber
                          )
                   WHERE 
                      {where_clause}
                   """
            else:
                columns_to_select_str = ",\n".join(columns_to_select)
                query = f"""
                       SELECT {columns_to_select_str}
                         FROM Listings
                        WHERE 
                            {where_clause}
                   """

            if limit != -1:
                query += f" LIMIT {limit}"

            # WARN: Use this to rest specific properties
            # query = 'SELECT * FROM Listings WHERE MlsNumber = 13315392'

            rows = cursor.execute(query).fetchall()
            listings = [dict(x) for x in rows]
            logging.info(f"Received {len(listings)} listings from the DB")
            # specific_listing = [x for x in listings if x['MlsNumber'] == 26295500]
            # if specific_listing:
            #     pass
            if within_area_of_interest and area_of_interest:
                listings = list(
                    filter(
                        lambda x: Polygon(area_of_interest).contains(
                            Point(x["Property_Address_Latitude"], x["Property_Address_Longitude"])
                        ),
                        listings,
                    )
                )
                logging.info(f"Filtered down to {len(listings)} listings because of area of interest")

            if min_metro_distance_meters:
                listings = list(
                    filter(
                        lambda listing: any(
                            [
                                distance.distance(
                                    [listing["Property_Address_Latitude"], listing["Property_Address_Longitude"]],
                                    poi,
                                ).meters
                                < min_metro_distance_meters
                                for poi in points_of_interest
                            ]
                        ),
                        listings,
                    )
                )
                logging.info(f"Filtered down to {len(listings)} listings because of points of interest")

            if no_high_rise:
                listings = list(
                    filter(
                        lambda x: not re.search(r"\|#([5-9]\d{2}|\d{4})\|", x["Property_Address_AddressText"]),
                        listings,
                    )
                )
                logging.info(f"Filtered down to {len(listings)} listings because of high apartments")

            return listings


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

    parser.add_argument(
        "--raw-details",
        action="store_true",
        help="Save the raw details of a selection ",
    )

    parser.add_argument(
        "--with-area-of-interest",
        action="store_true",
        help="When retrieving individual listing details, only look at listings within our area of interest",
    )

    parser.add_argument(
        "--near-poi-distance",
        type=int,
        help="When retrieving individual listing details, filter out any listings that are X meters farther away from points of interest",
    )

    parser.add_argument(
        "--max-price",
        type=int,
        default=2000000,
        help="When scraping listings, search for listings only below this price",
    )

    parser.add_argument(
        "--min-price",
        type=int,
        default=300000,
        help="When scraping listings, search for listings only above this price",
    )

    parser.add_argument(
        "--min-bedrooms",
        type=int,
        choices=[1, 2, 3, 4, 5],
        default=1,
        help="When scraping listings, search for listings with bedrooms only equal to or above this price",
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

    parse_options = {}
    if args.min_price:
        parse_options['min_price'] = args.min_price
    if args.max_price:
        parse_options['max_price'] = args.max_price
    if args.min_bedrooms:
        parse_options['min_bedrooms'] = f'{args.min_bedrooms}-0'


    if args.raw_details:
        points_of_interest = None
        if args.near_poi_distance:
            points_of_interest = []
            # https://www.donneesquebec.ca/recherche/dataset/vmtl-stm-traces-des-lignes-de-bus-et-de-metro
            poi_file = Path("stations.geojson")
            if poi_file.exists():
                with open(poi_file) as f:
                    poi = json.load(f)
                    points_of_interest.extend([list(reversed(x["geometry"]["coordinates"])) for x in poi["features"]])

        area_of_interest = None
        if args.with_area_of_interest:
            area_of_interest = [
                (45.546780742201165, -73.65807533729821),
                (45.5241750187359, -73.67472649086267),
                (45.51022227302072, -73.69086266029626),
                (45.50156020795671, -73.67524147499353),
                (45.48796289057615, -73.65258217323571),
                (45.467741340888665, -73.61258507240564),
                (45.45690538269222, -73.59181404579431),
                (45.454256276138466, -73.563661579974),
                (45.46990828260759, -73.55662346351892),
                (45.48038065986003, -73.54512215126306),
                (45.50601171342892, -73.5449504898861),
                (45.53241273092978, -73.54306221473962),
                (45.56006337665252, -73.6131000565365),
                (45.547682377783296, -73.63163948524743),
                (45.54972603156036, -73.65429878700525),
            ]

        relevant_listings = get_listings_from_db(
            db_file=args.database,
            min_price=args.min_price,
            max_price=args.max_price,
            within_area_of_interest=args.near_poi_distance,
            area_of_interest=area_of_interest,
            min_metro_distance_meters=args.near_poi_distance,
            points_of_interest=points_of_interest,
            # min_bedroom=2,
            # min_sqft=600,
            last_updated_days_ago=2,
            # max_price_per_sqft=700,
            has_upcoming_openhouse=False,
            # has_parking_details=True,
            # has_garage=True,
            # limit=5,
        )
        scraper.parse_raw_listings_details(relevant_listings, details_db=Path("listing_details_raw.sqlite"))
    else:
        scraper.parse_listings(**parse_options)
