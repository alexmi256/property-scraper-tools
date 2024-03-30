import collections
import json
import logging
import re
import sqlite3
import time
from collections import Counter
from contextlib import closing
from pathlib import Path
from typing import Callable, Optional, Union

from deepmerge import Merger
from deepmerge_strategies import merge_counters, merge_lists_with_dict_items
from tqdm import tqdm
from utils import PYTHON_TO_SQLITE_DATA_TYPES, SQLITE_RESERVED_WORDS, PYTHON_TO_POSTGRESQL_DATA_TYPES

logging.basicConfig(level=logging.INFO)

logger = logging.getLogger(__name__)


class JSONtoSQLAnalyzer:
    def __init__(
        self,
        db_file: str,
        item_mutator: Optional[Callable] = None,
        auto_convert_simple_types: bool = False,
        is_postgresql: bool = False,
    ):
        self.created_tables = {}
        # self.items_to_create = {}
        # self.rows = rows
        self.item_mutator = item_mutator
        self.db_file = db_file
        self.auto_convert_simple_types = auto_convert_simple_types
        self.is_postgresql: bool = is_postgresql
        self.user_was_warned_about_mutators = False

    def get_items_from_db(self, db_file: Optional[str] = None, limit: int = -1) -> list[dict]:
        """
        Open a previously saved raw DB and return the top X rows

        :param db_file: Which DB file to query, if None is specified the one from the class will be used
        :param limit: Amount of rows to fetch
        :return:
        """
        db_to_open = db_file if db_file else self.db_file
        if not db_to_open or not Path(db_to_open).exists():
            raise Exception(f"Database {db_file} does not exist!")
        with closing(sqlite3.connect(db_to_open)) as connection:
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

    def cast_value_to_sqlite(self, value: Union[str, bool, None]) -> Union[int, float, None, bool]:
        """
        Casts a string data type to one that's likely to have better native support i.e. int, float
        :param value:
        :return:
        """
        # Empty strings which should be None
        if type(value) is str and len(value) == 0:
            return None
        # Boolean types
        elif type(value) is bool:
            if self.is_postgresql:
                return value
            else:
                return int(value)
        # JSON boolean strings
        elif value.lower() in ["true", "false"]:
            # Just SQLite things
            return True if value.lower() == "true" else False
        # Simple numbers
        elif re.match("-?\d+$", value):
            return int(value)
        # Simple floats
        elif re.match(r"-?\d+\.\d+$", value):
            return float(value)
        else:
            return value

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
        self,
        item: dict,
        item_path: Optional[list[str]] = None,
        came_from_a_list: bool = False,
    ) -> None:
        """
        This function modifies the dict in place with various functions that are currently hardcoded

        The goal is to convert an unwieldy JSON text blob dict into something we can store into a relation SQL SB
        This usually requires that we do a bunch of changes to it

        TODO: Remove the hardcoded functions here and somehow use a config dict

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

        # WARN: This is where your custom function that modifies data will be used
        if self.item_mutator:
            logger.debug("Custom item mutator provided and will be used")
            self.item_mutator(current_dict_path, item)
        elif not self.user_was_warned_about_mutators:
            logger.warning("Custom item mutator was not provided, pray that your data does not need modifications")
            self.user_was_warned_about_mutators = True

        # Here we check what to do based on the key's value type
        for key, value in item.items():
            if isinstance(value, list):
                # Add these keys to the list since we will need to remove/modify them later with references
                # keys_that_are_lists.append(key)

                current_dict_path = f'{".".join(item_path + [key])}:'

                if all(isinstance(x, dict) for x in value):
                    # All the items in the list are dicts which makes life easy
                    for dict_in_list in value:
                        self.modify_dict(
                            dict_in_list,
                            item_path=item_path + [key],
                            came_from_a_list=True,
                        )
                else:
                    # This complicates life a lot and we hope it won't happen
                    logger.warning(
                        f'{current_dict_path} Key "{key}" has a value of type {type(value)} which is currently unsupported'
                    )

            elif isinstance(value, dict):
                logger.debug(f'{current_dict_path} Key "{key}" has a value of type dict')
                # If it's a dict we need to go through this whole loop again
                self.modify_dict(value, item_path=item_path + [key], came_from_a_list=False)
            elif self.auto_convert_simple_types and (type(value) is str or type(value) is bool):
                item[key] = self.cast_value_to_sqlite(value)

    def modify_dict_to_counter_types(
        self,
        item: dict,
        item_path: Optional[list[str]] = None,
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
        self,
        item: dict,
        item_path: Optional[list[str]] = None,
        flatten=True,
        items_to_create: Optional[dict] = None,
    ) -> Union[int, str]:
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
                    dicts_to_create.append(
                        {
                            "key_name": key_name,
                            "came_from_a_list": True,
                            "key_value": list_item,
                        }
                    )
            elif type(object_value) is dict:
                dicts_to_create.append(
                    {
                        "key_name": key_name,
                        "came_from_a_list": False,
                        "key_value": object_value,
                    }
                )
            else:
                raise Exception(
                    f"Item dict had a removable value type that was neither a list nor a dict: {type(object_value)}"
                )

        for dict_item in dicts_to_create:
            new_item_path = item_path + [dict_item["key_name"]]
            if dict_item["came_from_a_list"]:
                new_item_path.append("[]")
            discovered_item_id = self.split_lists_from_item(
                dict_item["key_value"],
                item_path=new_item_path,
                flatten=flatten,
                items_to_create=items_to_create,
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

    def convert_raw_db_to_json_and_merge_to_get_raw_schema(self):
        """
        This function process all raw JSON responses in your DB and merges them into one schema dict item
        You can then use this item to generate or execute SQL commands
        :return:
        """
        items = self.get_items_from_db()

        for item in tqdm(items, desc=f"Items Modified for SQL Schema Analysis of {self.db_file}", miniters=1):
            # Modify the item so that it will fit into a relational db better
            # i.e. Adding GeneratedIDs, making some list become dicts, etc...
            # This is also where we can control whether we do simple value conversions
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
        for item in tqdm(items, desc=f"Items Merged for SQL Schema Analysis of {self.db_file}"):
            merged_items = custom_merger.merge(merged_items, item)

        return merged_items

    def get_sqlite_sql_for_dbschema_from_raw_items(
        self,
        default_table_key_name="Listings",
    ) -> list[str]:
        """
        Returns SQL statements to create the necessary tables determined by looking at all DB items and merging them

        :param default_table_key_name:
        :return:
        """
        merged_item = self.convert_raw_db_to_json_and_merge_to_get_raw_schema()

        results = {}
        # WARNING: This is what flattens our dict item by default
        self.split_lists_from_item(merged_item, items_to_create=results)

        tables_to_create = self.get_sqlite_sql_for_merged_counter_dict(
            results,
            default_table_key_name=default_table_key_name,
        )

        return tables_to_create

    def generate_sqlite_sql_for_inserting_split_item(
        self, insert_item_sql_statements: dict, default_table_key_name: str
    ) -> list[tuple]:
        statements = []
        for item_path, items_to_create in insert_item_sql_statements.items():
            path_without_arrays = [x for x in item_path.split(".") if x != "[]"]
            table_name = default_table_key_name if item_path == "$" else path_without_arrays[-1]

            for dict_item in items_to_create:
                item_keys = [f"`{x}`" if x in SQLITE_RESERVED_WORDS else x for x in dict_item.keys()]
                item_values = [str(x) if isinstance(x, list) else x for x in dict_item.values()]
                item_values_template = str(tuple(["?"] * len(item_values))).replace("'", "")

                statements.append(
                    (
                        f"REPLACE INTO {table_name} {tuple(item_keys)} VALUES {item_values_template};",
                        item_values,
                    )
                )
        return statements

    def create_sqlite_tables_from_statements(self, db_name: str, create_table_sql_statements: list[str]):
        with closing(sqlite3.connect(db_name)) as connection:
            with closing(connection.cursor()) as cursor:
                # Create the tables one by one
                for sql_statement in create_table_sql_statements:
                    cursor.execute(sql_statement)
            connection.commit()

    def convert_raw_json_db_to_sqlite(
        self,
        new_db_name: Optional[str] = None,
        limit: int = -1,
        default_table_key_name: str = "Listings",
    ):
        # These listings are the ones that will get inserted
        listings = self.get_items_from_db(limit=limit)
        if new_db_name is None:
            new_db_name = self.db_file.split(".")[0] + f"_parsed_full_{int(time.time())}.db"

        # Create the table
        create_table_sql_statements = self.get_sqlite_sql_for_dbschema_from_raw_items()
        self.create_sqlite_tables_from_statements(new_db_name, create_table_sql_statements)

        # Insert the listings into the table
        with closing(sqlite3.connect(new_db_name)) as connection:
            for listing in tqdm(listings, desc="Rows Processed"):
                self.modify_dict(listing)
                created_items = {}
                # WARNING: This is what flattens our dict item by default
                self.split_lists_from_item(listing, items_to_create=created_items)

                statements = self.generate_sqlite_sql_for_inserting_split_item(created_items, default_table_key_name)
                with closing(connection.cursor()) as cursor:
                    for template, values in statements:
                        cursor.execute(template, values)
            connection.commit()

    def get_sqlite_sql_for_merged_counter_dict(
        self, merged_item_results, default_table_key_name: str = "items", cannot_be_not_null: Optional[list[str]] = None
    ) -> list[str]:
        """
        This will return you a list of SQL statements used for creating the necessary table given your derived schema

        :param cannot_be_not_null: A list of column names that should never be made NOT NULL, useful when sample size is small or misleading
        :param merged_item_results:
        :param default_table_key_name: The name of the default table your "items" will be placed in
        :return:
        """
        num_items_in_db = self.get_items_count_from_db()

        schemas = []
        created_paths = set()
        multiple_datatype_errors = False

        for item_path, items_to_create in merged_item_results.items():
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
                            f"Column {item_path}.{column_name} has no data type and thus is messing up data and needs to be dealt with"
                        )
                        continue
                    # This is hell
                    could_be_non_null = False if cannot_be_not_null and column_name in cannot_be_not_null else True
                    if type(column_type_counter) is Counter and "NoneType" in column_type_counter:
                        # WARNING: Another special check to see if all values were none in which case this column is either
                        #   useless trash or we didn't parse enough results to see what it could be
                        if column_type_counter.get("NoneType") == num_items_in_db:
                            logger.error(
                                f"Column {item_path}.{column_name} only had None/NULL data types across all {num_items_in_db} instances so it will be skipped"
                            )
                            continue
                        del column_type_counter["NoneType"]
                        could_be_non_null = False
                        # FIXME: This may be outdated and never reachable
                        if len(column_type_counter) == 0:
                            logger.error(
                                f"Column {item_path}.{column_name} has no data in its counter after removing NoneType counters, assuming TEXT"
                            )
                            continue

                    if len(column_type_counter) > 1:
                        logger.error(
                            f"""Column {item_path}.{column_name} has multiple data types: {column_type_counter}. This will cause errors upon data insertion.
                            You should add custom functions to the "modify_dict" function to make all the values the same.
                            Defaulting {item_path}.{column_name} to a TEXT data type as a workaround for now."""
                        )
                        multiple_datatype_errors = True
                        # Force TEXT type which should support all item types
                        most_common_type_for_column = [("str", 0)]

                    elif type(column_type_counter) is list:
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

                    db_type_conversion = (
                        PYTHON_TO_POSTGRESQL_DATA_TYPES if self.is_postgresql else PYTHON_TO_SQLITE_DATA_TYPES
                    )
                    sql_column_type = db_type_conversion.get(most_common_type_for_column[0][0], "TEXT")

                    # WARN: For large sample sizes this is ok but could be error prone by bad luck or small sample sizes
                    should_be_not_null = (
                        "NOT NULL"
                        if could_be_non_null and int(most_common_type_for_column[0][1]) == num_items_in_db
                        else ""
                    )

                    if column_name.upper() in SQLITE_RESERVED_WORDS:
                        column_name = f"`{column_name}`"

                    creation_text = re.sub(
                        r"\s+",
                        " ",
                        f'{column_name} {sql_column_type} {"PRIMARY KEY" if selected_primary_key == column_name else ""} {should_be_not_null}',
                    )

                    columns.append(creation_text.strip())

                schemas.append(f"CREATE TABLE IF NOT EXISTS {table_name}({', '.join(columns)});")
                created_paths.add(table_name)

        # if multiple_datatype_errors:
        #     raise Exception('Multiple datatypes were detected for columns, insertion will fail, fix these and try again')

        return schemas


# analyzer = JSONtoSQLAnalyzer('mls_raw_2024-02-20.db')
# analyzer.convert_raw_json_db_to_sqlite('new_test_db.db', limit=100)
