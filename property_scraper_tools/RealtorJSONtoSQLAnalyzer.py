import logging
import re
import sqlite3
from collections import Counter
from contextlib import closing
from datetime import datetime

import xxhash
from JSONtoSQLAnalyzer import JSONtoSQLAnalyzer
from sortedcontainers import SortedDict
from tqdm import tqdm

logging.basicConfig(level=logging.INFO)

logger = logging.getLogger(__name__)


class RealtorJSONtoSQLAnalyzer(JSONtoSQLAnalyzer):
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
                item["InsertedDateUTC"] = str(
                    datetime.utcfromtimestamp(int(item["InsertedDateUTC"][:11]) - c_ticks_time)
                )

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

        if current_dict_path == "$.Individual.[].Organization.[]" or current_dict_path == "$.Individual.[]":
            # Phones can be simplified
            for key_name in ["Phones"]:
                if key_name in item:
                    for list_item in item[key_name]:
                        list_item[f"{key_name}GeneratedId"] = int(
                            re.sub(r"[^\d]", "", list_item.get("AreaCode", "") + list_item.get("PhoneNumber", ""))
                        )
            # Same for websites
            for key_name in ["Websites"]:
                if key_name in item:
                    item[key_name] = ",".join([x["Website"] for x in item[key_name]])
            # And emails, no need to store these as separate items
            for key_name in ["Emails"]:
                if key_name in item:
                    item[key_name] = ",".join([x["ContactId"] for x in item[key_name]])

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
        columns_to_keep: list[str] | None = None,
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

        :param default_table_key_name:
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
            results,
            default_table_key_name=default_table_key_name,
        )

        return tables_to_create

    def generate_sqlite_sql_for_inserting_split_item(
        self, insert_item_sql_statements: dict, default_table_key_name: str, limit_to_columns: list[str] | None = None
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

                template = (
                    f"INSERT OR REPLACE INTO {table_name} {tuple(available_keys)} VALUES ({items_as_question_marks})"
                )

                statements.append((template, available_values))

        return statements

    def merge_multiple_raw_dbs_into_single_minimal_db(self, new_db_name: str, raw_dbs: list[str]):
        columns_to_keep = [
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

        create_table_sql_statements = self.get_sqlite_sql_for_dbschema_from_raw_items(
            columns_to_keep=columns_to_keep,
            default_table_key_name="Listings",
            keep_only_main_item=True,
            add_computed_columns=True,
        )
        self.create_sqlite_tables_from_statements(new_db_name, create_table_sql_statements)

        price_history_table_sql = """
                CREATE TABLE IF NOT EXISTS PriceHistory (
                    MlsNumber INTEGER,
                    Price     INTEGER NOT NULL,
                    Date      TEXT    NOT NULL,
                    FOREIGN KEY (
                        MlsNumber
                    )
                    REFERENCES Listings (MlsNumber),
                    UNIQUE (
                        MlsNumber,
                        Price,
                        Date
                    )
                );
            """
        self.create_sqlite_tables_from_statements(new_db_name, [price_history_table_sql])

        with closing(sqlite3.connect(new_db_name)) as connection:
            for old_db_name in raw_dbs:
                db_date = old_db_name.split("_")[-1][:-3]
                listings = self.get_items_from_db(db_file=old_db_name)

                for listing in tqdm(listings, desc=f"Rows Processed for {old_db_name}"):
                    self.modify_dict(listing)
                    computed_columns = {
                        "ComputedSQFT": None,
                        "ComputedPricePerSQFT": None,
                        "ComputedLastUpdated": db_date,
                        "ComputedNewBuild": False,
                    }

                    listing_is_new_build = "GST +  QST" in listing.get("Property", {}).get("Price", "")
                    if listing_is_new_build:
                        computed_columns["ComputedNewBuild"] = True
                        if listing.get("Property", {}).get("PriceUnformattedValue"):
                            listing["Property"]["PriceUnformattedValue"] = round(listing["Property"]["PriceUnformattedValue"] * 1.14975)

                    listing.update(computed_columns)

                    listing_interior_size = listing.get("Building", {}).get("SizeInterior")
                    if listing_interior_size:
                        listing["ComputedSQFT"] = round(
                            RealtorJSONtoSQLAnalyzer.convert_interior_size_to_sqft(listing_interior_size)
                        )
                        listing_price = listing.get("Property", {}).get("PriceUnformattedValue")
                        if listing_price:
                            listing["ComputedPricePerSQFT"] = round(float(listing_price) / listing["ComputedSQFT"])

                    results = {}
                    # WARNING: This is what flattens our dict item by default
                    self.split_lists_from_item(listing, items_to_create=results)

                    # Remove all data but the main Listings/$ object
                    for keyname in list(results.keys()):
                        if keyname != "$":
                            del results[keyname]

                    # TODO: Make function below support config for keeping only columns and keys
                    # NOTE: With current setup there will only ever be one statement since we're only inserting the main item
                    statements = self.generate_sqlite_sql_for_inserting_split_item(
                        results,
                        default_table_key_name="Listings",
                        limit_to_columns=columns_to_keep + list(computed_columns.keys()),
                    )
                    with closing(connection.cursor()) as cursor:
                        for statement in statements:
                            cursor.execute(statement[0], statement[1])
                        price_history_values = [
                            listing["MlsNumber"],
                            listing["Property"]["PriceUnformattedValue"],
                            db_date,
                        ]
                        cursor.execute(
                            "REPLACE INTO PriceHistory (MlsNumber, Price, Date) VALUES (?, ?, ?);", price_history_values
                        )

            connection.commit()


config = {}

analyzer = RealtorJSONtoSQLAnalyzer(
    "mls_raw_2024-02-20.db", item_mutator=RealtorJSONtoSQLAnalyzer.data_mutator, auto_convert_simple_types=True
)

# analyzer.convert_raw_json_db_to_sqlite()

analyzer.merge_multiple_raw_dbs_into_single_minimal_db(
    "mls_complete_minimal.db",
    [
        "mls_raw_2023-11-19.db",
        "mls_raw_2024-01-30.db",
        "mls_raw_2024-01-31.db",
        "mls_raw_2024-02-13.db",
        "mls_raw_2024-02-18.db",
        "mls_raw_2024-02-20.db",
        "mls_raw_2024-02-21.db",
    ],
)
