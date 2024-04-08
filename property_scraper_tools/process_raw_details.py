from RealtorJSONtoSQLAnalyzer import RealtorJSONtoSQLAnalyzer
from pprint import pprint
import json


analyzer = RealtorJSONtoSQLAnalyzer(
    "listing_details_raw.sqlite",
    city="montreal",
    item_mutator=RealtorJSONtoSQLAnalyzer.data_mutator,
    auto_convert_simple_types=True,
)

merged_items = analyzer.convert_raw_db_to_json_and_merge_to_get_raw_schema()
pprint(merged_items)
print(json.dumps(merged_items, indent=2))
