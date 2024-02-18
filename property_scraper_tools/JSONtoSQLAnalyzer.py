import collections
import functools
import json
import logging
import re
import sqlite3
import time
from collections import Counter
from contextlib import closing

import xxhash
from deepmerge import Merger
from deepmerge_strategies import merge_counters, merge_lists_with_dict_items
from sortedcontainers import SortedDict
from utils import PYTHON_TO_SQLITE_DATA_TYPES, SQLITE_RESERVED_WORDS

logging.basicConfig(level=logging.INFO)

logger = logging.getLogger(__name__)


# GENERATE_DICT_VALUE_VIA_XXHASH = lambda _, item_dict: xxhash.xxh32(str(SortedDict(item_dict))).intdigest()
#
# GENERATE_KEYNAME_VIA_PARENT_NAME = lambda item_key, _: f"{item_key}GeneratedId"
#
# KEEP_ONLY_FIRST_LIST_ITEM = lambda item_list: next((x for x in item_list), {})


class JSONtoSQLAnalyzer:
    def __init__(self, db_file: str, config: dict | None = None):
        self.created_tables = {}
        # self.items_to_create = {}
        # self.rows = rows
        self.config = config or {}
        self.db_file = db_file

    def get_items_from_db(self, limit: int = -1) -> list[dict]:
        """
        Open a previously saved raw DB and return the top X rows

        :param limit: Amount of rows to fetch
        :return:
        """
        with closing(sqlite3.connect(self.db_file)) as connection:
            with closing(connection.cursor()) as cursor:
                rows = cursor.execute(f"SELECT details from listings LIMIT {limit}").fetchall()
                return [json.loads(x[0]) for x in rows]

    def get_items_count_from_db(self) -> int:
        """
        Get the number of items in the DB
        :return:
        """
        with closing(sqlite3.connect(self.db_file)) as connection:
            with closing(connection.cursor()) as cursor:
                return cursor.execute(f"SELECT COUNT(*) from listings").fetchone()[0]

    def get_item_from_db(self):
        with closing(sqlite3.connect(self.db_file)) as connection:
            with closing(connection.cursor()) as cursor:
                return cursor.execute(f"SELECT details from listings").fetchone()

    @staticmethod
    def get_dict_id_keys(item: dict) -> list[str]:
        """
        Return a list of keys which are likely to be the ID key for the given dict

        :param item:
        :return:
        """
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
    def flatten(dictionary: dict, parent_key: bool = False, separator: str = "_") -> dict:
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
            # elif isinstance(value, list):
            #     # FIXME: Need to deal with lists so they're not List.# format
            #     if value:
            #         pass
            #         if type(value[0]) is Counter:
            #             items.extend(JSONtoSQLAnalyzer.flatten(value, new_key, separator).items())
            #     else:
            #         pass
            #         # The list is empty so don't do anything
            #     for k, v in enumerate(value):
            #         items.extend(JSONtoSQLAnalyzer.flatten({str(k): v}, new_key).items())
            else:
                items.append((new_key, value))
        return dict(items)

    def modify_dict(
        self, item: dict, item_path: list[str] | None = None, came_from_a_list: bool = False, config: dict = None
    ) -> None:
        """
        This function modifies the dict in place with various functions that are currently hardcoded

        The goal is to convert an unwieldy JSON text blob dict into something we can store into a relation SQL SB
        This usually requires that we do a bunch of changes to it

        TODO: Remove the hardcoded functions here and somehoe use a config dict

        :param config:
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

        # if 'Phones' in item:
        #     logger.debug(f"Current path is: {current_dict_path}")

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

        # This seems backwards but assumption is that multiple people belong to a single org so let's split it out
        if current_dict_path == "$.Individual.[]":
            for key_name in ["Organization"]:
                item[key_name] = [item[key_name]]

        if current_dict_path == "$.Individual.[].Organization.[]" or current_dict_path == "$.Individual.[]":
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
                        self.modify_dict(dict_in_list, item_path + [key], True, config)
                else:
                    # This complicates life a lot and we hope it won't happen
                    logger.warning(
                        f'{current_dict_path} Key "{key}" has a value of type {type(value)} which is currently unsupported'
                    )

            elif isinstance(value, dict):
                logger.debug(f'{current_dict_path} Key "{key}" has a value of type dict')
                # If it's a dict we need to go through this whole loop again
                self.modify_dict(value, item_path + [key], False, config)

    def modify_dict_to_counter_types(
        self,
        item: dict,
        item_path: list[str] | None = None,
        came_from_a_list: bool = False,
        counter_to_string: bool = False,
    ) -> None:
        """
        This function modifies the dict in place so that all values are Counters of the value types
        This is only useful when analyzing a large dataset so that we can see the data type consistency for each column
        If we end up with multiple types in the Counters it means the data is inconsistent

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
                    logger.warning(
                        f'{current_dict_path} Key "{key}" has a value of type {type(value)} which is currently unsupported'
                    )

            elif isinstance(value, dict):
                # If it's a dict we need to go through this whole loop again
                self.modify_dict_to_counter_types(value, item_path + [key], False, counter_to_string)
            # elif counter_to_string and isinstance(value, Counter):
            #     item[key] = value.most_common(1)[0][0] if len(value) else 'str'
            else:
                item[key] = Counter([type(value).__name__])

    def split_lists_from_item(
        self, item: dict, item_path: list[str] | None = None, flatten=True, items_to_create: dict | None = None
    ) -> int | str:
        """
        Splits up the given item into the given items_to_create dict so that each key will have a list of items to create
        WARNING: This flattens dicts by default

        Currently this function is pretty wonky
        Ideally it returns the ID value which should be an int or str
        If data is not pretreated it could end up being something else and which will have unintended consequences
        To add to the wonkyness, it's the 'items_to_create' dict you pass in which gets modified in-place that will have
        the results you're looking for.
        TODO: Make this return a tuple of (actual desired data dict, id for the current item)

        :param item:
        :param item_path:
        :param flatten:
        :param items_to_create:
        :return:
        """
        if items_to_create is None:
            items_to_create = {}

        if item_path is None:
            item_path = ["$"]

        if flatten:
            item = self.flatten(item)

        current_item_path = f'{".".join(item_path)}'

        logger.debug(f"Current path is: {current_item_path}")

        dicts_to_create = []

        removable_keys = [
            k for k, v in item.items() if (isinstance(v, list) or isinstance(v, dict)) and type(v) is not Counter
        ]
        for key_name in removable_keys:
            object_value = item.pop(key_name)
            if type(object_value) is list:
                for list_item in object_value:
                    dicts_to_create.append({"key_name": key_name, "came_from_a_list": True, "key_value": list_item})
            elif type(object_value) is dict:
                dicts_to_create.append({"key_name": key_name, "came_from_a_list": False, "key_value": object_value})
            else:
                raise Exception(
                    f"Item dict had a removable value type that was neither a list nor a dict: {type(object_value)}"
                )

        for dict_item in dicts_to_create:
            new_item_path = item_path + [dict_item["key_name"]]
            if dict_item["came_from_a_list"]:
                new_item_path.append("[]")
            discovered_item_id = self.split_lists_from_item(
                dict_item["key_value"], item_path=new_item_path, flatten=flatten, items_to_create=items_to_create
            )

            if dict_item["key_name"] not in item:
                item[dict_item["key_name"]] = []

            item[dict_item["key_name"]].append(discovered_item_id)

        # NOTE: While we could generate keys automatically here or in get_dict_id_keys this would likely result in
        # our predetermined schema breaking
        possible_id_keys_for_item = JSONtoSQLAnalyzer.get_dict_id_keys(item)

        determined_item_id_key = next(iter(possible_id_keys_for_item), "NO_ID_FOR_KEY")

        if current_item_path not in items_to_create:
            items_to_create[current_item_path] = []

        items_to_create[current_item_path].append(item)

        return item.get(determined_item_id_key, "NO_ID_FOR_KEY")

    def get_sqlite_sql_for_dbschema_from_raw_items(self) -> list[str]:
        # The items are the raw JSON blobs converted to dicts
        items = self.get_items_from_db()

        for item in items:
            # Modify the item so that it will fit into a relational db better
            # i.e. Adding GeneratedIDs, making some list become dicts, etc...
            self.modify_dict(item)
            # Modify the dict so that each value is a Counter with the data types
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

        # Merge all the items into one which will now have Counter of data types so that we can see data consistency
        functools.reduce(lambda a, b: custom_merger.merge(a, b), items, merged_items)
        # logger.debug(dumps(merged_items, indent=2))

        tables_to_create = self.get_sqlite_sql_for_merged_counter_dict(merged_items, default_table_key_name="Listings")
        return tables_to_create

    def generate_sqlite_sql_for_insertion(self, limit: int = 1) -> None:
        """
        Queries raw DB items, modifies them, and then prints out SQLite code for inserting said items into a relational DB based on previously determined schema
        FIXME: Currently broken due to quotes
        :param limit:
        :return:
        """
        listings = self.get_items_from_db(limit)

        for listing in listings:
            self.modify_dict(listing)
            self.print_sqlite_sql_for_insertion(listing, "Listings")

    def print_sqlite_sql_for_insertion(self, merged_item, default_table_key_name: str = "items") -> None:
        """
        Prints out SQLite code for inserting given item into a relational DB based on previously determined schema
        FIXME: This is broken because quotation is hard
        :param merged_item:
        :param default_table_key_name:
        :return:
        """
        created_items = {}
        # WARNING: This is what flattens our dict item by default
        self.split_lists_from_item(merged_item, items_to_create=created_items)

        for item_path, items_to_create in created_items.items():
            path_without_arrays = [x for x in item_path.split(".") if x != "[]"]
            table_name = default_table_key_name if item_path == "$" else path_without_arrays[-1]

            for dict_item in items_to_create:
                item_keys = [f"`{x}`" if x in SQLITE_RESERVED_WORDS else x for x in dict_item.keys()]
                item_values = [str(x) if isinstance(x, list) else x for x in dict_item.values()]
                # FIXME: Quoting is completely broken atm
                sql_str = f"INSERT OR IGNORE INTO {table_name} {tuple(item_keys)} VALUES {tuple(item_values)};"
                print(sql_str)

    def convert_raw_json_db_to_sqlite(
        self, new_db_name: str | None = None, limit: int = -1, default_table_key_name: str = "Listings"
    ):
        # These listings are the ones that will get inserted
        listings = self.get_items_from_db(limit)
        if new_db_name is None:
            new_db_name = f"mls_raw_{int(time.time())}.db"

        with closing(sqlite3.connect(f"{new_db_name}")) as connection:
            with closing(connection.cursor()) as cursor:
                # The reason we don't use the listsings above is because determining the SQL schema is destructive
                # TODO: This should actually be it's own separate function for creating from raw DB
                create_table_sql_statements = self.get_sqlite_sql_for_dbschema_from_raw_items()
                # Create the tables one by one
                for sql_statement in create_table_sql_statements:
                    cursor.execute(sql_statement)

                for listing in listings:
                    self.modify_dict(listing)
                    created_items = {}
                    # WARNING: This is what flattens our dict item by default
                    self.split_lists_from_item(listing, items_to_create=created_items)

                    for item_path, items_to_create in created_items.items():
                        path_without_arrays = [x for x in item_path.split(".") if x != "[]"]
                        table_name = default_table_key_name if item_path == "$" else path_without_arrays[-1]

                        for dict_item in items_to_create:
                            item_keys = [f"`{x}`" if x in SQLITE_RESERVED_WORDS else x for x in dict_item.keys()]
                            item_values = [str(x) if isinstance(x, list) else x for x in dict_item.values()]
                            item_values_template = str(tuple(["?"] * len(item_values))).replace("'", "")

                            sql_str = (
                                f"INSERT OR IGNORE INTO {table_name} {tuple(item_keys)} VALUES {item_values_template};"
                            )
                            cursor.execute(sql_str, item_values)
            connection.commit()

    def get_sqlite_sql_for_merged_counter_dict(self, merged_item, default_table_key_name: str = "items"):
        num_items_in_db = self.get_items_count_from_db()

        results = {}
        # WARNING: This is what flattens our dict item by default
        self.split_lists_from_item(merged_item, items_to_create=results)
        schemas = []
        created_paths = set()

        for item_path, items_to_create in results.items():
            path_without_arrays = [x for x in item_path.split(".") if x != "[]"]
            table_name = default_table_key_name if item_path == "$" else path_without_arrays[-1]

            # WARN: You may want to disable this and deal with table names differently
            #  In cases where you can come across the same object types at different keys it makes sense to ignore
            #  duplicate tables which would end up being the same
            if table_name in created_paths:
                continue

            for dict_item in items_to_create:
                possible_primary_keys = JSONtoSQLAnalyzer.get_dict_id_keys(dict_item)
                selected_primary_key = possible_primary_keys[0] if possible_primary_keys else None

                columns = []
                for column_name, column_type_counter in dict_item.items():
                    if column_type_counter is None:
                        logger.error(
                            f"Column {column_name} has unknown data type and thus is messing up data and needs to be dealt with"
                        )
                        continue
                    if type(column_type_counter) is list:
                        # WARN: The commented code resulted in the "list" value type to be determined as type of the
                        #  most popular item in the "list". This doesn't make sense since the value is a list type and
                        #  not whatever type the list items are.
                        #  The only way this would be useful is if in our Database we could define something like the
                        #  Python type list[str], but since were using SQLite the best we can store it as is TEXT
                        #  (or JSONB in SQLite 3.55.0+)
                        #  Thus we store all 'list' type items as a TEXT and code must deal with the TEXT -> JSON_ARRAY
                        #  conversion
                        # if len(column_type_counter) == 1 and column_type_counter[0] is not None:
                        #     most_common_type_for_column = column_type_counter[0].most_common(1)
                        # elif len(column_type_counter) > 1 and column_type_counter[0] is not None:
                        #     logger.warning(f'Column {column_name} had a list with multiple counter items which is unexpected, will use the first one')
                        #     most_common_type_for_column = column_type_counter[0].most_common(1)
                        # else:
                        #     logger.warning(f'Column {column_name} had an empty list (or a list of None items) which means we cannot know its type, will assume str:TEXT')
                        #     most_common_type_for_column = [('str', 0)]
                        most_common_type_for_column = [("list", 0)]
                    else:
                        most_common_type_for_column = column_type_counter.most_common(1)

                    if not most_common_type_for_column:
                        logger.error(f"Failed to determine a data type for column {column_name}")
                    sql_column_type = PYTHON_TO_SQLITE_DATA_TYPES.get(most_common_type_for_column[0][0], "TEXT")

                    # WARN: For large sample sizes this is ok but could be error prone by bad luck or small sample sizes
                    should_be_not_null = "NOT NULL" if int(most_common_type_for_column[0][1]) == num_items_in_db else ""

                    if column_name.upper() in SQLITE_RESERVED_WORDS:
                        column_name = f"`{column_name}`"

                    creation_text = re.sub(
                        r"\s+",
                        " ",
                        f'{column_name} {sql_column_type} {"PRIMARY KEY" if selected_primary_key == column_name else ""} {should_be_not_null}',
                    )

                    columns.append(creation_text.strip())

                schemas.append(f"CREATE TABLE {table_name}({', '.join(columns)});")
                created_paths.add(table_name)

        return schemas


config = {
    "$": {
        "Media": [],
        "Tags": [],
    },
    "$.Property": {
        "Photo": [],
        "Parking": [],
    },
    "$.Individual.[].Organization.[]": {"Phones": [], "Websites": [], "Emails": []},
    "$.Individual.[]": {"Phones": [], "Websites": [], "Emails": []},
}

analyzer = JSONtoSQLAnalyzer("mls_raw_2024-01-31.db", config)

analyzer.convert_raw_json_db_to_sqlite()

# listings = analyzer.get_items_from_db(1)
#
# for listing in listings:
#     analyzer.modify_dict(listing)
#     analyzer.print_sqlite_sql_for_insertion(listing, 'Listings')
#     pass

# merged_item = analyzer.merge_items_to_determine_db_schema()
# pprint(merged_item)
# analyzer.print_schema_for_item(merged_item, default_table_key_name='Listings')