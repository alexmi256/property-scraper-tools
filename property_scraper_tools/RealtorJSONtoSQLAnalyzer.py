import argparse
import logging
import re
import sqlite3
from collections import Counter
from contextlib import closing
from datetime import datetime
from pathlib import Path
from pprint import pprint
from typing import Callable, Optional

import xxhash
from JSONtoSQLAnalyzer import JSONtoSQLAnalyzer
from sortedcontainers import SortedDict
from tqdm import tqdm
from utils import CITIES

logging.basicConfig(level=logging.INFO)

logger = logging.getLogger(__name__)


class RealtorJSONtoSQLAnalyzer(JSONtoSQLAnalyzer):
    def __init__(
        self,
        db_file: str,
        city: str = "montreal",
        item_mutator: Optional[Callable] = None,
        auto_convert_simple_types: bool = False,
    ):
        super().__init__(db_file, item_mutator=item_mutator, auto_convert_simple_types=auto_convert_simple_types)
        self.city = city
        self.minimal_columns = [
            "AlternateURL_DetailsLink",
            "AlternateURL_VideoLink",
            "Building_BathroomTotal",
            "Building_Bedrooms",
            "Building_SizeExterior",
            "Building_SizeInterior",
            "Building_StoriesTotal",
            "Building_Type",
            "Building_UnitTotal",
            "Id",
            "InsertedDateUTC",
            "Land_SizeFrontage",
            "Land_SizeTotal",
            "MlsNumber",
            "PostalCode",
            "PriceChangeDateUTC",
            "Property_Address_AddressText",
            "Property_Address_Latitude",
            "Property_Address_Longitude",
            "Property_AmmenitiesNearBy",
            "Property_OwnershipType",
            "Property_Parking",
            "Property_ParkingSpaceTotal",
            "Property_Photo_HighResPath",
            "Property_PriceUnformattedValue",
            "Property_ZoningType",
            "Property_Type",
            "PublicRemarks",
            "RelativeDetailsURL",
        ]

    @staticmethod
    def data_mutator(current_dict_path, item):
        # Here $ represents the main dictionary
        if current_dict_path == "$":
            for key_name in ["Media", "Tags", "OpenHouse"]:
                if key_name in item:
                    for list_item in item[key_name]:
                        list_item[f"{key_name}GeneratedId"] = xxhash.xxh32(str(SortedDict(list_item))).intdigest()

            # The Distance KV in $ is useless so just delete it
            for key_name in ["Distance"]:
                del item[key_name]

            # This is for that 1/16000 listings that are not numbers but a valid Letter + Number
            if "MlsNumber" in item:
                item["MlsNumber"] = int(re.sub(r"[^0-9]", "", item["MlsNumber"]))

            # It seems like they're using C#'s DateTime ticks which is 621355968000000000L
            # https://stackoverflow.com/questions/1832714/18-digit-timestamp/1832746#1832746
            if "InsertedDateUTC" in item:
                c_ticks_time = 62135596800
                item["InsertedDateUTC"] = datetime.utcfromtimestamp(
                    int(item["InsertedDateUTC"][:11]) - c_ticks_time
                ).isoformat()

            almost_iso8601_dates = [
                x
                for x in item.keys()
                if (x.endswith("DateUTC") or x.endswith("LastUpdated")) and x != "InsertedDateUTC"
            ]
            if almost_iso8601_dates:
                for key_name in almost_iso8601_dates:
                    item[key_name] = datetime.strptime(item[key_name], "%Y-%m-%d %I:%M:%S %p").isoformat()

        if current_dict_path.endswith("Address"):
            useless_keys = ["DisseminationArea"]
            for useless_key in useless_keys:
                if useless_key in item:
                    # Doesn't have any data ever
                    del item[useless_key]

        if "OpenHouse" in current_dict_path:
            if "StartDateTime" in item:
                item["StartDateTime"] = datetime.strptime(item["StartDateTime"], "%d/%m/%Y %I:%M:%S %p").isoformat()
            if "EndDateTime" in item:
                item["EndDateTime"] = datetime.strptime(item["EndDateTime"], "%d/%m/%Y %I:%M:%S %p").isoformat()

        if current_dict_path == "$.Property":
            for key_name in ["Photo"]:
                # Note: We're only doing this because we've analyzed a bunch fo responses and they only give one photo for the main listings
                # Do note that listings can have multiple photos, I just don't care about them
                if key_name in item:
                    if item[key_name]:
                        # Low and Med res are what they say ethey are so remove them
                        item[key_name][0].pop("LowResPath")
                        item[key_name][0].pop("MedResPath")
                        item[key_name] = item[key_name][0]
                    else:
                        item[key_name] = None
                    if item[key_name].get("LastUpdated"):
                        item[key_name]["LastUpdated"] = datetime.strptime(
                            item[key_name]["LastUpdated"], "%Y-%m-%d %I:%M:%S %p"
                        ).isoformat()
                    # for list_item in item[key_name]:
                    #     list_item[f"{key_name}GeneratedId"] = xxhash.xxh32(str(SortedDict(list_item))).intdigest()

            # Parking can be simplified a lot
            for key_name in ["Parking"]:
                if key_name in item:
                    item[key_name] = ",".join([x["Name"] for x in item[key_name]])

            # This key is useless trash
            for key_name in ["OwnershipTypeGroupIds"]:
                if key_name in item:
                    del item[key_name]

            # A few listings do actually have decimal pricing, likely sqft items
            if "PriceUnformattedValue" in item:
                try:
                    item["PriceUnformattedValue"] = int(item["PriceUnformattedValue"])
                except ValueError:
                    item["PriceUnformattedValue"] = round(float(item["PriceUnformattedValue"]))

        # This seems backwards since we ussually want to avoid lists but assumption is that multiple people
        # can belong to a single org so let's split it out
        if current_dict_path == "$.Individual.[]":
            for key_name in ["Organization"]:
                item[key_name] = [item[key_name]]

            if "AgentPhotoLastUpdated" in item:
                item["AgentPhotoLastUpdated"] = datetime.strptime(
                    item["AgentPhotoLastUpdated"], "%Y-%m-%d %H:%M:%S"
                ).isoformat()

        if current_dict_path == "$.Individual.[].Organization.[]" or current_dict_path == "$.Individual.[]":
            # Phones can be simplified
            for key_name in ["Phones"]:
                if key_name in item:
                    for list_item in item[key_name]:
                        list_item[f"{key_name}GeneratedId"] = int(
                            re.sub(
                                r"[^\d]",
                                "",
                                list_item.get("AreaCode", "") + list_item.get("PhoneNumber", ""),
                            )
                        )
            # Same for websites
            for key_name in ["Websites"]:
                if key_name in item:
                    item[key_name] = ",".join([x["Website"] for x in item[key_name]])
            # And emails, no need to store these as separate items
            for key_name in ["Emails"]:
                if key_name in item:
                    item[key_name] = ",".join([x["ContactId"] for x in item[key_name]])
            if "PhotoLastupdate" in item:
                item["PhotoLastupdate"] = datetime.strptime(item["PhotoLastupdate"], "%Y-%m-%d %I:%M:%S %p").isoformat()

    @staticmethod
    def convert_interior_size_to_sqft(building_size_interior: str) -> float:
        size_details = building_size_interior.split(" ")
        size_number = float(size_details[0])
        size_measurement = size_details[1]

        if size_measurement == "sqft":
            return size_number
        elif size_measurement == "m2":
            return size_number * 10.764
        else:
            return 0

    # def keep_only_table_and_column_values(self, results: dict, table_name: str = '$', columns_to_keep: list[str]|None = None):
    #     if columns is None:
    #         columns = []
    #
    #     # Remove all data but the main Listings/$ object
    #     for keyname in list(results.keys()):
    #         if keyname != table_name:
    #             del results[keyname]
    #
    #     for item in results[table_name]:
    #         keys_to_remove = [x for x in item.keys() if x not in columns_to_keep]
    #         for key_name in keys_to_remove:
    #             del item[key_name]

    def get_sqlite_sql_for_dbschema_from_raw_items(
        self,
        default_table_key_name="Listings",
        columns_to_keep: Optional[list[str]] = None,
        add_computed_columns: bool = False,
        keep_only_main_item: bool = False,
    ) -> list[str]:
        """
        Returns SQL statements to create the necessary tables determined by looking at all DB items and merging them

        This function however supports keeping only the main `$` item (and thus only creating one table) as well as
        keeping limiting the columns in that table to a specific list.

        TODO: Eventually this should support a dict config of JSON-like paths and column names to be more flexible.
         One that is done we can merge it into the main class.
        TODO: Support config for adding columns

        :param default_table_key_name: This is the new table name that will be created for main items to store
        :param columns_to_keep: Only the columns in this list will created and have data inserted into them
        :param add_computed_columns: Add the following columns to the DB table schema
        :param keep_only_main_item:
        :return:
        """
        merged_item = self.convert_raw_db_to_json_and_merge_to_get_raw_schema()

        results = {}
        # WARNING: This is what flattens our dict item by default
        self.split_lists_from_item(merged_item, items_to_create=results)

        # Remove all data but the main Listings/$ object
        if keep_only_main_item:
            for keyname in list(results.keys()):
                if keyname != "$":
                    del results[keyname]

        # Remove all but the given columns
        if columns_to_keep:
            for item in results["$"]:
                keys_to_remove = [x for x in item.keys() if x not in columns_to_keep]
                for key_name in keys_to_remove:
                    del item[key_name]

        # Add the following computed columns
        if add_computed_columns:
            for item in results["$"]:
                item["ComputedSQFT"] = Counter(int=1)
                item["ComputedPricePerSQFT"] = Counter(int=1)
                item["ComputedLastUpdated"] = Counter(str=1)
                item["ComputedNewBuild"] = Counter(bool=1)

        tables_to_create = self.get_sqlite_sql_for_merged_counter_dict(
            results, default_table_key_name=default_table_key_name, cannot_be_not_null=["Property_FarmType"]
        )

        return tables_to_create

    def generate_sqlite_sql_for_inserting_split_item(
        self,
        insert_item_sql_statements: dict,
        default_table_key_name: str,
        limit_to_columns: Optional[list[str]] = None,
    ) -> list[tuple]:
        """
        Insert the items found in the listing but discard all but the main table and keep only specific columns
        :param insert_item_sql_statements:

        :param default_table_key_name: The name of the table items wil be inserted into
        :param limit_to_columns: If provided, only the columns in this array will be inserted
        :return:
        """
        statements = []
        for item_path, items_to_create in insert_item_sql_statements.items():
            path_without_arrays = [x for x in item_path.split(".") if x != "[]"]
            table_name = default_table_key_name if item_path == "$" else path_without_arrays[-1]

            for dict_item in items_to_create:
                available_keys = (
                    list(set(dict_item.keys()).intersection(limit_to_columns))
                    if limit_to_columns
                    else list(dict_item.keys())
                )
                available_values = [dict_item[x] for x in available_keys]
                available_values = [str(x) if isinstance(x, list) else x for x in available_values]
                items_as_question_marks = ", ".join(["?"] * len(available_values))

                template = f"REPLACE INTO {table_name} {tuple(available_keys)} VALUES ({items_as_question_marks})"

                statements.append((template, available_values))

        return statements

    def create_initial_tables(self, new_db_name: str, add_computed_columns: bool, minimal_config: bool):
        create_table_sql_statements = self.get_sqlite_sql_for_dbschema_from_raw_items(
            columns_to_keep=self.minimal_columns if minimal_config else None,
            default_table_key_name="Listings",
            keep_only_main_item=minimal_config,
            # Only applicable for this custom class
            add_computed_columns=add_computed_columns,
        )
        self.create_sqlite_tables_from_statements(new_db_name, create_table_sql_statements)

        price_history_table_sql = """
                            CREATE TABLE IF NOT EXISTS PriceHistory (
                                MlsNumber INTEGER,
                                Price     INTEGER NOT NULL,
                                Date      TEXT    NOT NULL
                            );
                        """
        self.create_sqlite_tables_from_statements(new_db_name, [price_history_table_sql])

    def get_existing_price_data(self, db_name: str, listings: Optional[list[int]] = None) -> dict:
        limit_to_listings = f" WHERE MlsNumber IN {tuple(listings)};" if listings else ";"
        sql = f"""
        SELECT MlsNumber,
               Property_PriceUnformattedValue,
               ComputedLastUpdated
          FROM Listings {limit_to_listings}
        """
        price_data = {}
        with closing(sqlite3.connect(db_name)) as connection:
            with closing(connection.cursor()) as cursor:
                rows = cursor.execute(sql).fetchall()
                for row in rows:
                    price_data[row[0]] = {"price": row[1], "date": datetime.strptime(row[2], "%Y-%m-%d")}
        return price_data

    def find_latest_price_date_from_db(self, db_name: str) -> Optional[datetime]:
        sql = """
        SELECT DISTINCT Date
          FROM PriceHistory
         ORDER BY DATE(Date) DESC
         LIMIT 1;
        """
        with closing(sqlite3.connect(db_name)) as connection:
            with closing(connection.cursor()) as cursor:
                row = cursor.execute(sql).fetchone()
                # FIXME: Return today's date if nothing found
                if row:
                    return datetime.strptime(row[0], "%Y-%m-%d")

    # FIXME: Variables need to be changed not to reflect a db file but a grouping of listings
    def insert_listings_into_db(
        self,
        listings: list,
        db_name: str,
        parsed_date: datetime,
        price_data: dict,
        add_computed_columns: bool = True,
        minimal_config: bool = False,
    ):
        parsed_date_str = str(parsed_date.date())
        with closing(sqlite3.connect(db_name)) as connection:
            for listing in tqdm(listings, desc=f"Rows inserted into {db_name}"):
                self.modify_dict(listing)
                computed_columns = {
                    "ComputedSQFT": None,
                    "ComputedPricePerSQFT": None,
                    "ComputedLastUpdated": parsed_date_str,
                    "ComputedNewBuild": False,
                }

                listing_is_new_build = "GST +  QST" in listing.get("Property", {}).get("Price", "")
                if listing_is_new_build:
                    computed_columns["ComputedNewBuild"] = True
                    # If it's a new build it will have 15% taxes which we want to auto add cuz that's just a hidden fee
                    if listing.get("Property", {}).get("PriceUnformattedValue"):
                        listing["Property"]["PriceUnformattedValue"] = round(
                            listing["Property"]["PriceUnformattedValue"] * 1.14975
                        )

                listing_interior_size = listing.get("Building", {}).get("SizeInterior")
                if listing_interior_size:
                    computed_sqft = round(RealtorJSONtoSQLAnalyzer.convert_interior_size_to_sqft(listing_interior_size))
                    if computed_sqft > 0:
                        computed_columns["ComputedSQFT"] = computed_sqft
                        listing_price = listing.get("Property", {}).get("PriceUnformattedValue")
                        if listing_price:
                            computed_columns["ComputedPricePerSQFT"] = round(
                                float(listing_price) / computed_columns["ComputedSQFT"]
                            )

                if add_computed_columns:
                    listing.update(computed_columns)

                results = {}
                # WARNING: This is what flattens our dict item by default
                self.split_lists_from_item(listing, items_to_create=results)

                # Remove all data but the main Listings/$ object
                if minimal_config:
                    for keyname in list(results.keys()):
                        if keyname != "$":
                            del results[keyname]

                # TODO: Make function below support config for keeping only columns and keys
                # NOTE: With current setup there will only ever be one statement since we're only inserting the main item
                statements = self.generate_sqlite_sql_for_inserting_split_item(
                    results,
                    # FIXME: This needs to be baked into argparse
                    default_table_key_name="Listings",
                    limit_to_columns=(self.minimal_columns + list(computed_columns.keys()) if minimal_config else None),
                )
                with closing(connection.cursor()) as cursor:
                    # Insert the items
                    for statement in statements:
                        cursor.execute(statement[0], statement[1])

                    # only insert price history if we don't have data for the current listing OR
                    # the price differs and the db date is greater than what the last saved price was
                    # WARNING: This of course assumes that we're parsing databases by oldest to newest
                    # I'm doing this just so that I only have to store the latest price
                    if listing["MlsNumber"] not in price_data or (
                        price_data[listing["MlsNumber"]]["price"] != listing["Property"]["PriceUnformattedValue"]
                        and parsed_date > price_data[listing["MlsNumber"]]["date"]
                    ):
                        price_data[listing["MlsNumber"]] = {
                            "price": listing["Property"]["PriceUnformattedValue"],
                            "date": parsed_date,
                        }
                        sql = """
                        INSERT INTO PriceHistory (MlsNumber, Price, Date) VALUES (?, ?, ?);
                        """
                        price_history_values = [
                            listing["MlsNumber"],
                            listing["Property"]["PriceUnformattedValue"],
                            parsed_date_str,
                        ]
                        cursor.execute(
                            sql,
                            price_history_values,
                        )
            # Need to commit after every db?
            connection.commit()

    def merge_multiple_raw_dbs_into_single_db(
        self,
        new_db_name: str,
        raw_dbs: list[str],
        create_new_tables: bool = True,
        db_date: Optional[datetime] = None,
        add_computed_columns: bool = True,
        minimal_config: bool = False,
        skip_existing_dates: bool = False,
    ) -> None:

        if create_new_tables:
            self.create_initial_tables(new_db_name, add_computed_columns, minimal_config)

        # FIXME: This could be a memory hog and should be done in SQL but all my attempts have sucked and didn't
        #  work or were super slow. The memory thing is only an issue when dealing with a whole DB and not prominent
        #  with live scraping which only has about 200 listings at a time
        price_data = self.get_existing_price_data(new_db_name)

        latest_date_from_db = None
        if skip_existing_dates:
            latest_date_from_db = self.find_latest_price_date_from_db(new_db_name)

        for old_db_name in raw_dbs:
            # WARN: This is hacky and no guarantee on actual dates
            # If were merging multiple tables then we can't really rely on user input

            # We have a DB date from the file which we should prefer in the case of multiple DBs
            date_in_old_file_name = re.search(r"\d{4}-\d{2}-\d{2}", str(old_db_name))
            if date_in_old_file_name and (len(raw_dbs) > 1 or db_date is None):
                db_date = datetime.strptime(date_in_old_file_name.group(), "%Y-%m-%d")
            elif not db_date:
                raise Exception("Could not figure out date for DB to convert which will cause issues")

            if skip_existing_dates and latest_date_from_db and db_date <= latest_date_from_db:
                logger.info(
                    f'Skipping {old_db_name} because its date "{db_date}" is before the latest date "{latest_date_from_db}" found in the db'
                )
                continue
            else:
                logger.info(
                    f'Processing "{old_db_name}" because its date "{db_date}" is after the latest date "{latest_date_from_db}" found in the db'
                )

            listings = self.get_items_from_db(db_file=old_db_name)
            self.insert_listings_into_db(
                listings,
                db_name=new_db_name,
                parsed_date=db_date,
                price_data=price_data,
                add_computed_columns=add_computed_columns,
                minimal_config=minimal_config,
            )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="Realtor.ca Database Analyzer and Updater",
        description="""
        Analyzes databases storing raw JSON responses from realtor.ca and converts them into a database relational format.

        Extra functionality includes creating and updating minimal copies of such databases
        """,
    )

    parser.add_argument(
        "database",
        nargs="+",
        type=Path,
        help="""The raw database file to perform analysis on. It must have a table named "Listings" and column "details".
             Multiple tables can be provided for extra actions but analysis will only be perform on the first one""",
    )

    parser.add_argument(
        "--city",
        default="montreal",
        choices=list(CITIES.keys()),
        help="Which city should be data be parsed for",
    )

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "-a",
        "--analyze",
        action="store_true",
        help="""Perform the analysis on the database which will print out a dict object with counters of all the merged items.
        """,
    )
    group.add_argument(
        "-c",
        "--convert",
        action="store_true",
        help="""Silently analyze the raw JSON database and then convert it database to a SQL schemafull database
        """,
    )

    # These flags only affect analysis
    parser.add_argument(
        "--print-sql",
        action="store_true",
        help="""Analyzes the database and prints the SQL statements for creating the tables
        Note that further modifications will be performed on your data beyond your supplied "data_mutator" such as dict flattening.
        """,
    )

    # TODO: Option to print minimal dict
    # TODO: Option to print the flattened version
    # TODO: Option to decided to flatten or not (default is flatten)

    # These flags only affect conversion

    # The flags below impact both analysis and conversion
    # FIXME: Make these work
    # parser.add_argument(
    #     "--table-name",
    #     type=str,
    #     default="listings",
    #     help="The name of the SQLite table that contains the raw JSON data",
    # )
    #
    # parser.add_argument(
    #     "--column-name",
    #     type=str,
    #     default="details",
    #     help="The name of the column containing JSON blobs",
    # )

    parser.add_argument(
        "--new-table-name",
        type=str,
        default="Listings",
        help="""The name of the SQLite table that will be used to store the newly converted object items.
        Since we receive just a list of items we can't properly guess the name of what the list of items should be.
        """,
    )

    parser.add_argument(
        "--auto-cast-value-types",
        default=True,
        action=argparse.BooleanOptionalAction,
        help="""When analyzing the database for printing or converting, we will try to cast string value types to simple data types.
        The simple data types are string, bool, int, float which have semi-equivalent SQLite data types.
        The caveat of running with this option however is that depending on the raw data we can end up with multiple data type conversions.
        When this happens we fall back to string/TEXT data type.
        The fix for this is write custom logic the your "data_mutator" to handle these conflicting conversions before we try to auto-convert.
        """,
    )

    # These options only really apply when converting databases

    parser.add_argument(
        "-o",
        "--output-database",
        type=Path,
        help="The name of the db that the analyzed results will be output to",
        default="output.sqlite",
    )

    parser.add_argument(
        "--db-date",
        type=str,
        help="The date of the given database. This affects some of the generated data we store for items.",
    )
    parser.add_argument(
        "-m",
        "--minimal",
        action="store_true",
        help="If the output database format should be minimal instead of full",
    )

    parser.add_argument(
        "-u",
        "--update-output-db",
        action="store_true",
        help="Assume databases were already created so this db creation will be skipped and we'll just insert items",
    )

    parser.add_argument(
        "--skip-existing-db-dates",
        action="store_true",
        help="When multiple databases are provided, with this option we will skip any databases that appear to already have been parsed",
    )

    args = parser.parse_args()

    analyzer = RealtorJSONtoSQLAnalyzer(
        args.database[0],
        city=args.city,
        item_mutator=RealtorJSONtoSQLAnalyzer.data_mutator,
        auto_convert_simple_types=args.auto_cast_value_types,
    )

    if args.analyze:
        merged_items = analyzer.convert_raw_db_to_json_and_merge_to_get_raw_schema()
        pprint(merged_items)
        if args.print_sql:
            results = {}
            # WARNING: This is what flattens our dict item by default
            analyzer.split_lists_from_item(merged_items, items_to_create=results)

            tables_to_create = analyzer.get_sqlite_sql_for_merged_counter_dict(
                results,
                default_table_key_name=args.new_table_name,
            )
            for table in tables_to_create:
                print(table)
    elif args.convert:
        analyzer.merge_multiple_raw_dbs_into_single_db(
            new_db_name=args.output_database,
            raw_dbs=args.database,
            create_new_tables=not args.update_output_db,
            db_date=args.db_date,
            minimal_config=args.minimal,
            skip_existing_dates=args.skip_existing_db_dates,
        )
