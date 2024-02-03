# What
Scrape realtor.ca listing data and ~~efficiently~~ store them in an SQLite database.

# Why
* The filters on realtor.ca are terrible
* To track price changes and visualize them

# How
## raw_sqlite_dump.py
Responses are requested for an area and stored in a SQLite `mls_raw_{str(date.today())}.db` database file with the following structure:
```sql
 CREATE TABLE IF NOT EXISTS listings
  (
     id           INTEGER PRIMARY KEY,
     details      TEXT NOT NULL,
     last_updated TEXT NOT NULL
  )  
```
where `id` is the MLS id and `details` is the raw JSON response for each listing

## JSONtoSQLAnalyzer
This uses a database created above and does the following:
1. modify_dict: Modfies each listing in place to:
  * Add a unique generatedID based on xxhashed contents for items missing ID like keys
  * Make some lists text objects as they're too small to be stored
  * Delete some useless keys
2. modify_dict_to_counter_types: Looks at all modified listings and converts key values into Counter objects
3. merge_items_to_determine_db_schema: Merges all modified items into one object so we can determine a schema
4. split_lists_from_item: Splits up the schema dict so that all list items will be their own separate entry
5. print_schema_for_item: Flattens the schema dicts and prints out SQLite code to create appropriate tables

# TODO
- [ ] Fix the dict merging so that lists are not merged with key numbers
- [ ] Separate hardcoded functions in JSONtoSQL to a config file
- [ ] Compare raw JSON text to split tables of JSON
- [ ] Compare split tables of JSON with new SQLite JSONB extension
- [ ] Use logging instead of printing
- [ ] Figure out multi-day data storage

# Thanks
* https://github.com/harry-s-grewal/mls-real-estate-scraper-for-realtor.ca
* https://stackoverflow.com/questions/6027558/flatten-nested-dictionaries-compressing-keys