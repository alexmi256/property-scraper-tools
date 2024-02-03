import re
import sqlite3
import json
from contextlib import closing
import xxhash
from sortedcontainers import SortedDict
from collections import Counter
import functools
from deepmerge import Merger, STRATEGY_END
from pprint import pprint
from json import dumps
import collections

"""
Root
Individual[] gets removed completely as it needs to be a new table
Individual_ITEMS gets created inside Root level as a replacement
a new root Individual_ITEMS_TABLE is created to store Individual_ITEMS
Individual_ITEMS in root now contains IDs referincing items in Individual_ITEMS_TABLE


Problems are:
1. How do we store Root.Individual_ITEMS since it's still a list
Store this as a TEXT list/JSONB in newer versions 
2. How do we determine an ID for each item in Individual_ITEMS_TABLE so that it's linked
3. It may be that certain lists only ever have one item and it would be better to flatten them
4. 

"""
# GENERATE_DICT_VALUE_VIA_XXHASH = lambda _, item_dict: xxhash.xxh32(str(SortedDict(item_dict))).intdigest()
#
# GENERATE_KEYNAME_VIA_PARENT_NAME = lambda item_key, _: f"{item_key}GeneratedId"
#
# KEEP_ONLY_FIRST_LIST_ITEM = lambda item_list: next((x for x in item_list), {})

PYTHON_TO_SQLITE_DATA_TYPES = {"int": "INTEGER", "str": "TEXT", "float": "REAL"}


def merge_counters(config, path, base, nxt):
    """
    use all values in either base or nxt.
    """
    if isinstance(base, Counter) and isinstance(nxt, Counter):
        return base + nxt
    else:
        return STRATEGY_END


def merge_lists_with_dict_items(config, path, base, nxt):
    """
    use all values in either base or nxt.
    """
    if isinstance(base, list) and isinstance(nxt, list):
        custom_merger = Merger(
            [
                (Counter, merge_counters),
                (list, merge_lists_with_dict_items),
                (dict, ["merge"]),
                (set, ["union"]),
            ],
            ["override"],
            ["override"],
        )

        merged_items = {}
        items = base + nxt

        functools.reduce(lambda a, b: custom_merger.merge(a, b), items, merged_items)
        return [merged_items]
    else:
        return STRATEGY_END


class JSONtoSQLAnalyzer:
    def __init__(self, config: dict | None = None):
        self.created_tables = {}
        self.items_to_create = {}
        # self.rows = rows
        self.config = config or {}

    def get_items_from_db(self, db_file: str, limit: int = 1):
        """
        Open a previosuly saved raw DB and return the top X rows

        :param limit: Amount of rows to fetch
        :param db_file:
        :return:
        """
        with closing(sqlite3.connect(db_file)) as connection:
            with closing(connection.cursor()) as cursor:
                rows = cursor.execute(f"SELECT details from listings LIMIT {limit}").fetchall()
                return [json.loads(x[0]) for x in rows]

    def get_item_from_db(self, db_file: str):
        return self.get_items_from_db(db_file, limit=1)[0]

    @staticmethod
    def get_dict_id_keys(item: dict) -> list[str]:
        # This regex is funky like this because a key ending with "ID" should get precedence over a key with "Id"
        potential_ids = [re.search(r".+GeneratedId|^[iI][dD]$|.+[a-z]ID|.+Id$", x) for x in item.keys()]
        potential_ids = [x.group() for x in potential_ids if x]
        # Special check for *GeneratedId which are added later and thus will show up last in out list
        generated_id_key = next((x for x in potential_ids if "GeneratedId" in x), None)
        if generated_id_key:
            potential_ids.remove(generated_id_key)
            potential_ids.insert(0, generated_id_key)
        return potential_ids

    @staticmethod
    def flatten(dictionary, parent_key=False, separator="."):
        """
        Turn a nested dictionary into a flattened dictionary
        https://stackoverflow.com/questions/6027558/flatten-nested-dictionaries-compressing-keys
        :param dictionary: The dictionary to flatten
        :param parent_key: The string to prepend to dictionary's keys
        :param separator: The string used to separate flattened keys
        :return: A flattened dictionary
        """

        items = []
        for key, value in dictionary.items():
            new_key = str(parent_key) + separator + key if parent_key else key
            if isinstance(value, collections.abc.MutableMapping) and type(value) is not Counter:
                items.extend(JSONtoSQLAnalyzer.flatten(value, new_key, separator).items())
            elif isinstance(value, list):
                # FIXME: Need to deal with lists so they're not List.# format
                if value:
                    pass
                    if type(value[0]) is Counter:
                        items.extend(JSONtoSQLAnalyzer.flatten(value, new_key, separator).items())
                else:
                    pass
                    # The list is empty so don't do anything
                for k, v in enumerate(value):
                    items.extend(JSONtoSQLAnalyzer.flatten({str(k): v}, new_key).items())
            else:
                items.append((new_key, value))
        return dict(items)

    def modify_dict(
        self,
        item: dict,
        item_path: list[str] | None = None,
        came_from_a_list: bool = False,
    ):
        """
        This function modifies the dict in place to either simplify IDs or reduce lists of items

        :param item:
        :param item_path:
        :param came_from_a_list:
        :return:
        """
        if item_path is None:
            item_path = ["$"]

        if came_from_a_list:
            item_path.append("[]")

        current_dict_path = f'{".".join(item_path)}'

        # print(f"Current path is: {current_dict_path}")

        if current_dict_path == "$":
            for key_name in ["Media", "Tags", "OpenHouse"]:
                if key_name in item:
                    for list_item in item[key_name]:
                        list_item[f"{key_name}GeneratedId"] = xxhash.xxh32(str(SortedDict(list_item))).intdigest()

        if current_dict_path == "$.Property":
            for key_name in ["Photo"]:
                if key_name in item:
                    for list_item in item[key_name]:
                        list_item[f"{key_name}GeneratedId"] = xxhash.xxh32(str(SortedDict(list_item))).intdigest()

            for key_name in ["Parking"]:
                if key_name in item:
                    item[key_name] = ",".join([x["Name"] for x in item[key_name]])

            for key_name in ["OwnershipTypeGroupIds"]:
                if key_name in item:
                    del item[key_name]

        if current_dict_path == "$.Individual.[].Organization" or "$.Individual.[]":
            for key_name in ["Phones"]:
                if key_name in item:
                    for list_item in item[key_name]:
                        list_item[f"{key_name}GeneratedId"] = int(
                            re.sub(r"[^\d]", "", list_item.get("AreaCode", "") + list_item.get("PhoneNumber", ""))
                        )

            for key_name in ["Websites"]:
                if key_name in item:
                    item[key_name] = ",".join([x["Website"] for x in item[key_name]])

            for key_name in ["Emails"]:
                if key_name in item:
                    item[key_name] = ",".join([x["ContactId"] for x in item[key_name]])

        # Here we check what to do based on the key's value type
        for key, value in item.items():
            if isinstance(value, list):
                # Add these keys to the list since we will need to remove/modify them later with references
                # keys_that_are_lists.append(key)

                current_dict_path = f'{".".join(item_path + [key])}:'

                if all(isinstance(x, dict) for x in value):
                    # All the items in the list are dicts which makes life easy
                    for dict_in_list in value:
                        self.modify_dict(dict_in_list, item_path + [key], True)
                else:
                    # This complicates life a lot and we hope it won't happen
                    pass
                    # print(
                    #     f'{current_dict_path} Key "{key}" has a value of type {type(value)} which is currently unsupported'
                    # )

            elif isinstance(value, dict):
                # print(f'{current_dict_path} Key "{key}" has a value of type dict')
                # If it's a dict we need to go through this whole loop again
                self.modify_dict(value, item_path + [key], False)

    def modify_dict_to_counter_types(
        self,
        item: dict,
        item_path: list[str] | None = None,
        came_from_a_list: bool = False,
        counter_to_string: bool = False
    ):
        """
        This function modifies the dict in place so that all values are Counters of the value types


        :param item:
        :param item_path:
        :param came_from_a_list:
        :param counter_to_string: When True, we will convert Counter values to the most common counter string
        :return:
        """
        if item_path is None:
            item_path = ["$"]

        if came_from_a_list:
            item_path.append("[]")

        # Here we check what to do based on the key's value type
        for key, value in item.items():
            if isinstance(value, list):
                current_dict_path = f'{".".join(item_path + [key])}:'

                if all(isinstance(x, dict) for x in value):
                    # All the items in the list are dicts which makes life easy
                    for dict_in_list in value:
                        self.modify_dict_to_counter_types(dict_in_list, item_path + [key], True, counter_to_string)
                else:
                    # This complicates life a lot and we hope it won't happen
                    pass
                    # print(
                    #     f'{current_dict_path} Key "{key}" has a value of type {type(value)} which is currently unsupported'
                    # )

            elif isinstance(value, dict):
                # If it's a dict we need to go through this whole loop again
                self.modify_dict_to_counter_types(value, item_path + [key], False, counter_to_string)
            # elif counter_to_string and isinstance(value, Counter):
            #     item[key] = value.most_common(1)[0][0] if len(value) else 'str'
            else:
                item[key] = Counter([type(value).__name__])

    def split_lists_from_item(
        self,
        item: dict,
        item_path: list[str] | None = None,
        came_from_a_list: bool = False,
    ):
        """
        This function modifies the dict in place to either simplify IDs or reduce lists of items

        :param item:
        :param item_path:
        :param came_from_a_list:
        :return:
        """
        if item_path is None:
            item_path = ["$"]

        if came_from_a_list:
            item_path.append("[]")

        current_dict_path = f'{".".join(item_path)}'

        # print(f"Current path is: {current_dict_path}")

        keys_that_are_lists = []

        # Here we check what to do based on the key's value type
        for key, value in item.items():
            if isinstance(value, list):
                # Add these keys to the list since we will need to remove/modify them later with references
                keys_that_are_lists.append(key)

            elif isinstance(value, dict):
                # print(f'{current_dict_path} Key "{key}" has a value of type dict')
                # If it's a dict we need to go through this whole loop again
                self.split_lists_from_item(value, item_path + [key], False)

        for key in keys_that_are_lists:
            value = item.pop(key)
            current_dict_path = f'{".".join(item_path + [key])}:'
            if current_dict_path not in self.items_to_create:
                self.items_to_create[current_dict_path] = value
            # If it's an empty list we can't make assumptions about it's items
            if len(value) == 0:
                print(f'Key "{key}" is an empty list, cannot assume values')
                continue

            # We only care about the first value as all items in the list should have the same ID key
            possible_ids_for_first_item_dict = JSONtoSQLAnalyzer.get_dict_id_keys(next(iter(value)))
            item[key] = [x.get(next(iter(possible_ids_for_first_item_dict), "NO_ID_FOR_KEY"), None) for x in value]

            # Continue looking each item in the list
            if all(isinstance(x, dict) for x in value):
                for single_item in value:
                    self.split_lists_from_item(single_item, item_path + [key], True)
            else:
                print(f'{current_dict_path} Key "{key}" has list items where each one is not a dict')

        if ".".join(item_path) == "$":
            self.items_to_create["$"] = [item]

        return self.items_to_create

    def merge_items_to_determine_db_schema(self, db_file: str):
        """
        Converts all row values to theyr object type and then merges all rows into one super row
        This allows us to see all possible keys that will exist for a scraped dataset

        :param db_file:
        :return:
        """
        items = self.get_items_from_db(db_file, 100)
        for item in items:
            # This is needed when we want to do modifications to the data such as adding GeneratedIDs or reducing number of lists
            self.modify_dict(item)
            self.modify_dict_to_counter_types(item)

        custom_merger = Merger(
            [
                (Counter, merge_counters),
                (list, merge_lists_with_dict_items),
                (dict, ["merge"]),
                (set, ["union"]),
            ],
            ["override"],
            ["override"],
        )

        merged_items = {}

        functools.reduce(lambda a, b: custom_merger.merge(a, b), items, merged_items)
        # print(dumps(merged_items, indent=2))

        return merged_items

    def print_schema_for_item(self, merged_item, default_table_key_name: str = "items"):
        self.split_lists_from_item(merged_item)
        schemas = []
        created_paths = set()

        for item_path, items_to_create in self.items_to_create.items():
            table_name = default_table_key_name if item_path == '$' else item_path.split(".")[-1].strip(":")

            # WARN: You wamy want to disable this and deal with table names differently
            #  In cases where you can come across the same object types at different keys it makes sense to ignore
            #  duplicate tables which would end up being the same
            if table_name in created_paths:
                continue

            for dict_item in items_to_create:
                # Flatten nested dicts here
                # FIXME: Items that have lists will get a Name.0 key created
                dict_item = JSONtoSQLAnalyzer.flatten(dict_item, separator="_")

                possible_primary_keys = JSONtoSQLAnalyzer.get_dict_id_keys(dict_item)
                selected_primary_key = possible_primary_keys[0] if possible_primary_keys else None

                columns = []
                for column_name, column_type_counter in dict_item.items():
                    if column_type_counter is None:
                        print(
                            f"Column {column_name} has unknown data type and thus is messing up data and needs to be dealt with"
                        )
                        continue
                    most_common_type_for_column = column_type_counter.most_common(1)
                    if not most_common_type_for_column:
                        print("failed...")
                    sql_column_type = PYTHON_TO_SQLITE_DATA_TYPES.get(most_common_type_for_column[0][0], "TEXT")

                    should_be_not_null = "NOT NULL" if int(most_common_type_for_column[0][1]) == 16896 else ""

                    creation_text = re.sub(
                        r"\s+",
                        " ",
                        f'{column_name} {sql_column_type} {"PRIMARY KEY" if selected_primary_key == column_name else ""} {should_be_not_null}',
                    )

                    columns.append(creation_text.strip())

                schemas.append(f"CREATE TABLE {table_name}({', '.join(columns)});")
                created_paths.add(table_name)

        print("\n".join(schemas))

        pass


config = {
    "$": {
        "Media": [],
        "Tags": [],
    },
    "$.Property": {
        "Photo": [],
        "Parking": [],
    },
    "$.Individual.[].Organization": {"Phones": [], "Websites": [], "Emails": []},
    "$.Individual.[]": {"Phones": [], "Websites": [], "Emails": []},
}

analyzer = JSONtoSQLAnalyzer(config)
# item = analyzer.get_item_from_db("mls_raw_2024-01-31.db")
#
#
# analyzer.modify_dict(item)
# items_to_create = analyzer.split_lists_from_item(item)

merged_item = analyzer.merge_items_to_determine_db_schema("mls_raw_2024-01-31.db")
analyzer.print_schema_for_item(merged_item, default_table_key_name='Listings')
