import collections
import json
import logging
import re
import sqlite3
from contextlib import closing
from datetime import datetime
from collections import Counter
import xxhash
from JSONtoSQLAnalyzer import JSONtoSQLAnalyzer
from sortedcontainers import SortedDict
from tqdm import tqdm
from utils import SQLITE_RESERVED_WORDS
from typing import Callable

logging.basicConfig(level=logging.INFO)

logger = logging.getLogger(__name__)


class RealtorJSONtoSQLAnalyzer(JSONtoSQLAnalyzer):

    # def __init__(self, db_file: str, item_mutator: Callable | None = None, auto_convert_simple_types: bool = False):
    #     super().__init__(db_file, item_mutator, auto_convert_simple_types)

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

    def create_minimal_table(self, new_db_name, create_table_sql_statements):
        with closing(sqlite3.connect(new_db_name)) as connection:
            with closing(connection.cursor()) as cursor:
                for sql_statement in create_table_sql_statements:
                    cursor.execute(sql_statement)

            connection.commit()

    # def get_sqlite_sql_for_dbschema_from_raw_items(
    #     self, default_table_key_name="Listings",
    # ) -> list[str]:
    #     merged_item = self.convert_raw_db_to_json_and_merge_to_get_raw_schema()
    #
    #     results = {}
    #     # WARNING: This is what flattens our dict item by default
    #     self.split_lists_from_item(merged_item, items_to_create=results)
    #
    #     # Remove all data but the main Listings/$ object
    #     for keyname in list(results.keys()):
    #         if keyname != '$':
    #             del results[keyname]
    #
    #     # Remove all but these columns
    #     keep_only_columns =[
    #         "AlternateURL_DetailsLink",
    #         "AlternateURL_VideoLink",
    #         "Building_BathroomTotal",
    #         "Building_Bedrooms",
    #         "Building_SizeExterior",
    #         "Building_SizeInterior",
    #         "Building_StoriesTotal",
    #         "Building_Type",
    #         "Building_UnitTotal",
    #         "Id",
    #         "InsertedDateUTC",
    #         "Land_SizeFrontage",
    #         "Land_SizeTotal",
    #         "MlsNumber",
    #         "PostalCode",
    #         "PriceChangeDateUTC",
    #         "Property_Address_AddressText",
    #         "Property_Address_Latitude",
    #         "Property_Address_Longitude",
    #         "Property_AmmenitiesNearBy",
    #         "Property_OwnershipType",
    #         "Property_Parking",
    #         "Property_ParkingSpaceTotal",
    #         "Property_Photo_HighResPath",
    #         "Property_PriceUnformattedValue",
    #         "Property_Type",
    #         "PublicRemarks",
    #         "RelativeDetailsURL",
    #     ]
    #     for item in results['$']:
    #         keys_to_remove = [x for x in item.keys() if x not in keep_only_columns]
    #         for key_name in keys_to_remove:
    #             del item[key_name]
    #
    #         # Add the following computed columns
    #         item['ComputedSQFT'] = Counter(int=1)
    #         item['ComputedPricePerSQFT'] = Counter(int=1)
    #         item['ComputedLastUpdated'] = Counter(str=1)
    #
    #     tables_to_create = self.get_sqlite_sql_for_merged_counter_dict(
    #         results, default_table_key_name=default_table_key_name,
    #     )
    #
    #     return tables_to_create

    def merge_multipe_raw_dbs_into_single_minimal_db(self, new_db_name: str, raw_dbs: list[str]):
        create_table_sql_statements = self.get_sqlite_sql_for_dbschema_from_raw_items(
            default_table_key_name="Listings",
        )
        pass

        # for i, old_db_name in enumerate(raw_dbs):
        #     listings = []
        #     db_date = old_db_name.split("_")[-1][:-3]
        #     table_name = f"Listings_{db_date}".replace("-", "_")
        #     statements_for_current_table = [x.replace("Listings", table_name) for x in create_table_sql_statements]
        #
        #     # Create the new table
        #     self.create_minimal_table(new_db_name, statements_for_current_table)
        #
        #     # Select items from old DB
        #     with closing(sqlite3.connect(old_db_name)) as connection:
        #         with closing(connection.cursor()) as cursor:
        #             rows = cursor.execute(f"SELECT details from listings").fetchall()
        #             listings = [json.loads(x[0]) for x in rows]
        #
        #     with closing(sqlite3.connect(new_db_name)) as connection:
        #         with closing(connection.cursor()) as cursor:
        #             for listing in tqdm(listings, desc="Rows Processed and Inserted"):
        #                 self.modify_dict(listing)
        #                 created_items = {}
        #                 # WARNING: This is what flattens our dict item by default
        #                 self.split_lists_from_item(listing, items_to_create=created_items)
        #
        #                 for item_path, items_to_create in created_items.items():
        #                     if item_path != "$":
        #                         continue
        #                     path_without_arrays = [x for x in item_path.split(".") if x != "[]"]
        #                     table_name = table_name if item_path == "$" else path_without_arrays[-1]
        #
        #                     for dict_item in items_to_create:
        #                         # Trim down the dict to minimal columns
        #                         for key_name in list(dict_item.keys()):
        #                             if key_name not in keep_tables[table_name]:
        #                                 del dict_item[key_name]
        #
        #                         item_keys = [f"`{x}`" if x in SQLITE_RESERVED_WORDS else x for x in dict_item.keys()]
        #                         item_values = [str(x) if isinstance(x, list) else x for x in dict_item.values()]
        #                         item_values_template = str(tuple(["?"] * len(item_values))).replace("'", "")
        #
        #                         sql_str = f"INSERT OR IGNORE INTO {table_name} {tuple(item_keys)} VALUES {item_values_template};"
        #                         pass
        #                         cursor.execute(sql_str, item_values)
        #         connection.commit()


config = {}

analyzer = RealtorJSONtoSQLAnalyzer(
    "mls_raw_2024-02-18.db", item_mutator=RealtorJSONtoSQLAnalyzer.data_mutator, auto_convert_simple_types=False
)

analyzer.convert_raw_json_db_to_sqlite('new_test_db.db')

# analyzer.merge_multipe_raw_dbs_into_single_minimal_db(
#     "mls_complete_minimal.db",
#     [
#         "mls_raw_2024-02-19.db",
#         "mls_raw_2024-02-18.db",
#         "mls_raw_2024-01-31.db",
#         "mls_raw_2024-01-30.db",
#         "mls_raw_2023-11-19.db",
#     ],
# )
