# What
Scrape realtor.ca listing data and ~~efficiently~~ store them in an SQLite database.

# Why
* The filters on realtor.ca are terrible
* To track price changes and visualize them

# How
## raw_sqlite_dump.py

### Cookies  
For best results you should use a real browser to navigate the site once and save the cookies given by the search query.
Browsers will usually give you a string format of `cookie1=value1;cookie2=value2` for but this script supports converting this to JSON.

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
This uses a database created above. The main code flow occurs in `convert_raw_json_db_to_sqlite`
```txt
WIP
```

You will almost certainly need to write and use "data mutators" in order modify each row in such a way that it can be stored.

### Why
The response data returned by some servers is a giant JSON blob.
If we want to efficiently store this data in a relational database then we need to analyze it, split it up, 
and determine an appropriate schema.

This script can help with that process.

You could ofcourse use a NoSQL solution to store the data directly or even store the blob as a string but there are some things to consider listed below.

Even though I've targeted SQLite as the DB of choice for portability/accessibility I think this was probably a poor and limiting choice.

#### Comparisons
**SQLite Pros**
- Super simple, even has [Fiddle](https://sqlite.org/fiddle/)
- Can share DBs
- Can even place a DBs online as a file and access it
- 
**SQLite Cons**
- Limited support for data types (IMO)
  - I realize this is an intentional tradeoff but it means you're going to need to handle a bunch more cases in queries or code
- I have no idea how to efficiently store time series/event data
- No plugins for complex functionality like [PostGIS](https://postgis.net/), your code will need logic for this

Storage TL:DR

##### Raw JSON storage as TEXT in SQlite
**Pros**
- Super simple
- Super quick to do

**Cons**
- Need to understand response schema and how to deal with it
- Long queries since you need to destructure data every time you access it
- If you have poorly structured JSON where every value is formatted as a string, you'll have even longer queries and embedded logic for how to handle specific data
- Largest storage size (94M)

##### Raw JSON storage as JSONB in SQlite
**Pros**
- 🦗

**Cons**
- Requires SQLite +3.45.0 which probably won't be supported on most systems as of yet
- Doesn't actually help with JSON queries
- Doesn't actually reduce storage space by any significance (94M -> 93M)

##### JSONtoSQLAnalyzer in SQlite
**Pros**
- Reduces storage space by 2.7x (94M -> 32M)
- Makes data more relational
- Simplifies queries a bit 
- Could also be ported to other DBs

**Cons**
- Still need to understand schema
- You'll need to write custom code to clean up the response data so that the data can be stored

##### JSONtoSQLAnalyzer with automatic datatype guessing in SQlite
**Pros**
- It's free to do check
- Automatically tries to guess data types for column values
- Fallback to TEXT when we created inconsistent data by trying to auto cast

**Cons**
- Not perfect, logic for some data types will still live in queries/code
- No real improvement in storage size ¯\_(ツ)_/¯
- Increased risk of having multiple data types, these will fallback to TEXT but it means more manual fixes
- Somehow data size increases (32M -> 33M)

**Neutral**
- Further separates raw response data from stored response data

### Example Usage
#### Analyzing JSON Structure
The first thing you'll want to do is get a bunch of JSON responses so you can analyze the schema.
**Code**
```python
analyzer = JSONtoSQLAnalyzer("mls.db")
merged_item = analyzer.merge_items_to_determine_db_schema()
pprint(merged_item)
```
**Output**
```python
{'AlternateURL': {'DetailsLink': Counter({'str': 16325}),
                  'VideoLink': Counter({'str': 2690})},
 'Building': {'BathroomTotal': Counter({'str': 15626}),
              'Bedrooms': Counter({'str': 15474}),
              'SizeExterior': Counter({'str': 1345}),
              'SizeInterior': Counter({'str': 8401}),
              'StoriesTotal': Counter({'str': 16896}),
              'Type': Counter({'str': 15858}),
              'UnitTotal': Counter({'str': 2725})},
 'Business': {},
 'Distance': Counter({'str': 16896}),
 'HasNewImageUpdate': Counter({'bool': 530}),
 'HasOpenHouseUpdate': Counter({'bool': 245}),
 'HasPriceUpdate': Counter({'bool': 105}),
 'HistoricalDataIsCleared': Counter({'bool': 16896}),
 'Id': Counter({'str': 16896}),
 'Individual': [{'AgentPhotoLastUpdated': Counter({'str': 23879}),
                 'CccMember': Counter({'bool': 104}),
                 'CorporationDisplayTypeId': Counter({'str': 23924}),
                 'CorporationName': Counter({'str': 10323}),
                 'CorporationType': Counter({'str': 2040}),
                 'EducationCredentials': Counter({'str': 67}),
                 'Emails': Counter({'str': 23924}),
                 'FirstName': Counter({'str': 23924}),
                 'IndividualID': Counter({'int': 23924}),
                 'LastName': Counter({'str': 23924}),
                 'Name': Counter({'str': 23924}),
                 'Organization': [{'Address': {'AddressText': Counter({'str': 23924}),
                                               'PermitShowAddress': Counter({'bool': 23924})},
                                   'Designation': Counter({'str': 23924}),
                                   'Emails': Counter({'str': 23923}),
                                   'HasEmail': Counter({'bool': 23923}),
                                   'Logo': Counter({'str': 23311}),
                                   'Name': Counter({'str': 23924}),
                                   'OrganizationID': Counter({'int': 23924}),
                                   'OrganizationType': Counter({'str': 23924}),
                                   'PermitFreetextEmail': Counter({'bool': 23924}),
                                   'PermitShowListingLink': Counter({'bool': 23924}),
                                   'Phones': [{'AreaCode': Counter({'str': 42931}),
                                               'Extension': Counter({'str': 26}),
                                               'PhoneNumber': Counter({'str': 42931}),
                                               'PhoneType': Counter({'str': 42931}),
                                               'PhoneTypeId': Counter({'str': 42931}),
                                               'PhonesGeneratedId': Counter({'int': 42931})}],
                                   'PhotoLastupdate': Counter({'str': 23924}),
                                   'RelativeDetailsURL': Counter({'str': 23924}),
                                   'Websites': Counter({'str': 17350})}],
                 'PermitFreetextEmail': Counter({'bool': 23924}),
                 'PermitShowListingLink': Counter({'bool': 23924}),
                 'Phones': [{'AreaCode': Counter({'str': 30593}),
                             'Extension': Counter({'str': 96}),
                             'PhoneNumber': Counter({'str': 30593}),
                             'PhoneType': Counter({'str': 30593}),
                             'PhoneTypeId': Counter({'str': 30593}),
                             'PhonesGeneratedId': Counter({'int': 30593})}],
                 'Photo': Counter({'str': 23879}),
                 'PhotoHighRes': Counter({'str': 23879}),
                 'Position': Counter({'str': 23924}),
                 'RankMyAgentKey': Counter({'str': 23924}),
                 'RealSatisfiedKey': Counter({'str': 23924}),
                 'RelativeDetailsURL': Counter({'str': 23924}),
                 'TestimonialTreeKey': Counter({'str': 23924}),
                 'Websites': Counter({'str': 16835})}],
 'InsertedDateUTC': Counter({'str': 16896}),
 'Land': {'SizeFrontage': Counter({'str': 8067}),
          'SizeTotal': Counter({'str': 10387})},
 'ListingBoundary': Counter({'str': 4090}),
 'ListingGMT': Counter({'str': 4090}),
 'ListingTimeZone': Counter({'str': 4090}),
 'Media': [{'Description': Counter({'str': 19185}),
            'MediaCategoryId': Counter({'str': 19185}),
            'MediaCategoryURL': Counter({'str': 19185}),
            'MediaGeneratedId': Counter({'int': 19185}),
            'Order': Counter({'int': 19015}),
            'VideoType': Counter({'str': 2690})}],
 'MlsNumber': Counter({'str': 16896}),
 'OpenHouse': [{'EndDateTime': Counter({'str': 755}),
                'EventTypeID': Counter({'str': 755}),
                'FormattedDateTime': Counter({'str': 755}),
                'OpenHouseGeneratedId': Counter({'int': 755}),
                'StartDateTime': Counter({'str': 755}),
                'StartTime': Counter({'str': 755})}],
 'OpenHouseInsertDateUTC': Counter({'str': 4090}),
 'PhotoChangeDateUTC': Counter({'str': 16870}),
 'PostalCode': Counter({'str': 16896}),
 'PriceChangeDateUTC': Counter({'str': 5175}),
 'Property': {'Address': {'AddressText': Counter({'str': 16896}),
                          'Latitude': Counter({'str': 16896}),
                          'Longitude': Counter({'str': 16896}),
                          'PermitShowAddress': Counter({'bool': 16896})},
              'AmmenitiesNearBy': Counter({'str': 14333}),
              'FarmType': Counter({'str': 16895}),
              'LeaseRent': Counter({'str': 63}),
              'LeaseRentUnformattedValue': Counter({'str': 63}),
              'OwnershipType': Counter({'str': 6670}),
              'Parking': Counter({'str': 13003}),
              'ParkingSpaceTotal': Counter({'str': 12998}),
              'ParkingType': Counter({'str': 13003}),
              'Photo': [{'Description': Counter({'str': 16869}),
                         'HighResPath': Counter({'str': 16896}),
                         'LastUpdated': Counter({'str': 16896}),
                         'LowResPath': Counter({'str': 16896}),
                         'MedResPath': Counter({'str': 16896}),
                         'PhotoGeneratedId': Counter({'int': 16896}),
                         'SequenceId': Counter({'str': 16896}),
                         'SvgPath': Counter({'str': 26}),
                         'TypeId': Counter({'str': 16896})}],
              'Price': Counter({'str': 16896}),
              'PriceUnformattedValue': Counter({'str': 16896}),
              'Type': Counter({'str': 16896}),
              'TypeId': Counter({'str': 16896}),
              'ZoningType': Counter({'str': 16046})},
 'ProvinceName': Counter({'str': 16896}),
 'PublicRemarks': Counter({'str': 16896}),
 'RelativeDetailsURL': Counter({'str': 16896}),
 'RelativeURLEn': Counter({'str': 16896}),
 'RelativeURLFr': Counter({'str': 16896}),
 'StandardStatusId': Counter({'str': 222}),
 'StatusId': Counter({'str': 16896}),
 'Tags': [{'HTMLColorCode': Counter({'str': 1661}),
           'Label': Counter({'str': 1661}),
           'ListingTagTypeID': Counter({'str': 1661}),
           'TagsGeneratedId': Counter({'int': 1661})}],
 'TimeOnRealtor': Counter({'str': 16896}),
 'UploadedBy': Counter({'int': 16896})}
```
This schema will show you the general structure of all responses merged as one object.
Additionally, a Counter of each key's value will be calculated. 
This can be used for figuring out which keys are always present or not.
If the Counters have different types, it can mean that:
1. Server responses follow no logic for data types
2. You may have messed up in guessing the type for that value if you're trying to change values in `modify_dict` to have better data types 

#### Output SQLite code for Table Creation
Once you have an idea of what the structure is you can try storing it as different tables.
You'll probably want to modify the schema in the code to suit your needs, most of these changes are hardcoded in `modify_dict`.
**Code**
```python
analyzer = JSONtoSQLAnalyzer("mls.db")
merged_item = analyzer.merge_items_to_determine_db_schema()
analyzer.print_schema_for_item(merged_item, default_table_key_name='Listings')
```
**Output**
```sql
CREATE TABLE Phones(PhoneType TEXT, PhoneNumber TEXT, AreaCode TEXT, PhoneTypeId TEXT, PhonesGeneratedId INTEGER PRIMARY KEY, Extension TEXT);
CREATE TABLE Organization(OrganizationID INTEGER PRIMARY KEY, Name TEXT, Logo TEXT, Address_AddressText TEXT, Address_PermitShowAddress TEXT, Emails TEXT, Websites TEXT, OrganizationType TEXT, Designation TEXT, HasEmail TEXT, PermitFreetextEmail TEXT, PermitShowListingLink TEXT, RelativeDetailsURL TEXT, PhotoLastupdate TEXT, Phones TEXT);
CREATE TABLE Individual(IndividualID INTEGER PRIMARY KEY, Name TEXT, Websites TEXT, Emails TEXT, Photo TEXT, Position TEXT, PermitFreetextEmail TEXT, FirstName TEXT, LastName TEXT, CorporationName TEXT, CorporationDisplayTypeId TEXT, PermitShowListingLink TEXT, RelativeDetailsURL TEXT, AgentPhotoLastUpdated TEXT, PhotoHighRes TEXT, RankMyAgentKey TEXT, RealSatisfiedKey TEXT, TestimonialTreeKey TEXT, CorporationType TEXT, CccMember TEXT, EducationCredentials TEXT, Organization TEXT, Phones TEXT);
CREATE TABLE Property_Photo(SequenceId TEXT NOT NULL, HighResPath TEXT NOT NULL, MedResPath TEXT NOT NULL, LowResPath TEXT NOT NULL, Description TEXT, LastUpdated TEXT NOT NULL, TypeId TEXT NOT NULL, PhotoGeneratedId INTEGER PRIMARY KEY NOT NULL, SvgPath TEXT);
CREATE TABLE Media(MediaCategoryId TEXT, MediaCategoryURL TEXT, Description TEXT, `Order` INTEGER, MediaGeneratedId INTEGER PRIMARY KEY, VideoType TEXT);
CREATE TABLE Tags(Label TEXT, HTMLColorCode TEXT, ListingTagTypeID TEXT, TagsGeneratedId INTEGER PRIMARY KEY);
CREATE TABLE OpenHouse(StartTime TEXT, StartDateTime TEXT, EndDateTime TEXT, FormattedDateTime TEXT, EventTypeID TEXT, OpenHouseGeneratedId INTEGER PRIMARY KEY);
CREATE TABLE Listings(Id TEXT PRIMARY KEY NOT NULL, MlsNumber TEXT NOT NULL, PublicRemarks TEXT NOT NULL, Building_StoriesTotal TEXT NOT NULL, Building_BathroomTotal TEXT, Building_Bedrooms TEXT, Building_Type TEXT, Building_UnitTotal TEXT, Building_SizeInterior TEXT, Building_SizeExterior TEXT, Property_Price TEXT NOT NULL, Property_Type TEXT NOT NULL, Property_Address_AddressText TEXT NOT NULL, Property_Address_Longitude TEXT NOT NULL, Property_Address_Latitude TEXT NOT NULL, Property_Address_PermitShowAddress TEXT NOT NULL, Property_TypeId TEXT NOT NULL, Property_FarmType TEXT, Property_ZoningType TEXT, Property_PriceUnformattedValue TEXT NOT NULL, Property_AmmenitiesNearBy TEXT, Property_Parking TEXT, Property_ParkingSpaceTotal TEXT, Property_ParkingType TEXT, Property_OwnershipType TEXT, Property_LeaseRent TEXT, Property_LeaseRentUnformattedValue TEXT, Land_SizeTotal TEXT, Land_SizeFrontage TEXT, AlternateURL_DetailsLink TEXT, AlternateURL_VideoLink TEXT, PostalCode TEXT NOT NULL, HistoricalDataIsCleared TEXT NOT NULL, ProvinceName TEXT NOT NULL, RelativeDetailsURL TEXT NOT NULL, StatusId TEXT NOT NULL, StandardStatusId TEXT, PhotoChangeDateUTC TEXT, Distance TEXT NOT NULL, RelativeURLEn TEXT NOT NULL, RelativeURLFr TEXT NOT NULL, InsertedDateUTC TEXT NOT NULL, TimeOnRealtor TEXT NOT NULL, UploadedBy INTEGER NOT NULL, PriceChangeDateUTC TEXT, OpenHouseInsertDateUTC TEXT, HasOpenHouseUpdate TEXT, ListingTimeZone TEXT, ListingBoundary TEXT, ListingGMT TEXT, HasNewImageUpdate TEXT, HasPriceUpdate TEXT, Individual TEXT, Property_Photo TEXT, Media TEXT, Tags TEXT, OpenHouse TEXT);
```

#### Output SQLite code for Table Insertion
Now that you've created some tables it's time to test out insertion
**Code**
```python
analyzer = JSONtoSQLAnalyzer("mls.db")
analyzer.generate_sqlite_sql_for_insertion()
```
**Output**
```sql
INSERT OR IGNORE INTO Phones ('PhoneType', 'PhoneNumber', 'AreaCode', 'PhoneTypeId', 'PhonesGeneratedId') VALUES ('Telephone', '<redacted>', '<redacted>', '1', <redacted>);
INSERT OR IGNORE INTO Phones ('PhoneType', 'PhoneNumber', 'AreaCode', 'PhoneTypeId', 'PhonesGeneratedId') VALUES ('Fax', '<redacted>', '<redacted>', '4', <redacted>);
INSERT OR IGNORE INTO Phones ('PhoneType', 'PhoneNumber', 'AreaCode', 'PhoneTypeId', 'PhonesGeneratedId') VALUES ('Telephone', '<redacted>', '<redacted>', '1', <redacted>);
INSERT OR IGNORE INTO Phones ('PhoneType', 'PhoneNumber', 'AreaCode', 'PhoneTypeId', 'PhonesGeneratedId') VALUES ('Fax', '<redacted>', '<redacted>', '4', <redacted>);
INSERT OR IGNORE INTO Organization ('OrganizationID', 'Name', 'Logo', 'Address_AddressText', 'Address_PermitShowAddress', 'Emails', 'Websites', 'OrganizationType', 'Designation', 'HasEmail', 'PermitFreetextEmail', 'PermitShowListingLink', 'RelativeDetailsURL', 'PhotoLastupdate', 'Phones') VALUES (<redacted>, 'PROPRIO DIRECT', 'https://cdn.realtor.ca/organization/en-CA/TS<redacted>/lowres/<redacted>.jpg', '<redacted> - <redacted> aut. des Laurentides |Laval, Quebec <redacted>', True, '<redacted>', 'http://www.propriodirect.com/', 'Firm', 'Real Estate Agency', True, True, True, '/office/firm/<redacted>/proprio-direct<redacted>aut-des-laurentides-laval-quebec-<redacted>', '<redacted> 6:<redacted>:<redacted> PM', [<redacted>, <redacted>]);
INSERT OR IGNORE INTO Organization ('OrganizationID', 'Name', 'Logo', 'Address_AddressText', 'Address_PermitShowAddress', 'Emails', 'Websites', 'OrganizationType', 'Designation', 'HasEmail', 'PermitFreetextEmail', 'PermitShowListingLink', 'RelativeDetailsURL', 'PhotoLastupdate', 'Phones') VALUES (<redacted>, 'PROPRIO DIRECT', 'https://cdn.realtor.ca/organization/en-CA/TS<redacted>/lowres/<redacted>.jpg', '<redacted> - <redacted> aut. des Laurentides |Laval, Quebec <redacted>', True, '<redacted>', 'http://www.propriodirect.com/', 'Firm', 'Real Estate Agency', True, True, True, '/office/firm/<redacted>/proprio-direct<redacted>aut-des-laurentides-laval-quebec-<redacted>', '<redacted> 6:<redacted>:<redacted> PM', [<redacted>, <redacted>]);
INSERT OR IGNORE INTO Phones ('PhoneType', 'PhoneNumber', 'AreaCode', 'PhoneTypeId', 'PhonesGeneratedId') VALUES ('Telephone', '<redacted>', '<redacted>', '1', <redacted>);
INSERT OR IGNORE INTO Phones ('PhoneType', 'PhoneNumber', 'AreaCode', 'PhoneTypeId', 'PhonesGeneratedId') VALUES ('Telephone', '<redacted>', '<redacted>', '1', <redacted>);
INSERT OR IGNORE INTO Individual ('IndividualID', 'Name', 'Websites', 'Emails', 'Photo', 'Position', 'PermitFreetextEmail', 'FirstName', 'LastName', 'CorporationName', 'CorporationDisplayTypeId', 'PermitShowListingLink', 'RelativeDetailsURL', 'AgentPhotoLastUpdated', 'PhotoHighRes', 'RankMyAgentKey', 'RealSatisfiedKey', 'TestimonialTreeKey', 'Organization', 'Phones') VALUES (<redacted>, '<redacted> <redacted>', 'https://propriodirect.com/', '<redacted>', 'https://cdn.realtor.ca/individual/TS<redacted>/lowres/<redacted>.jpg', 'Residential and Commercial Real Estate Broker', True, '<redacted>', '<redacted>', 'Gestion Immobilière <redacted> <redacted> Inc.', '1', True, '/agent/<redacted>/<redacted>-<redacted><redacted>aut-des-laurentides-laval-quebec-<redacted>', '<redacted> <redacted>:<redacted>:<redacted>', 'https://cdn.realtor.ca/individual/TS<redacted>/highres/<redacted>.jpg', '', '', '', [<redacted>], [<redacted>]);
INSERT OR IGNORE INTO Individual ('IndividualID', 'Name', 'Websites', 'Emails', 'Photo', 'Position', 'PermitFreetextEmail', 'FirstName', 'LastName', 'CorporationDisplayTypeId', 'PermitShowListingLink', 'RelativeDetailsURL', 'AgentPhotoLastUpdated', 'PhotoHighRes', 'RankMyAgentKey', 'RealSatisfiedKey', 'TestimonialTreeKey', 'Organization', 'Phones') VALUES (<redacted>, '<redacted> <redacted>', 'https://propriodirect.com/<redacted>-<redacted>/', '<redacted>', 'https://cdn.realtor.ca/individual/TS<redacted>/lowres/<redacted>.jpg', 'Residential and Commercial Real Estate Broker', True, '<redacted>', '<redacted>', '0', True, '/agent/<redacted>/<redacted>-<redacted><redacted>aut-des-laurentides-laval-quebec-<redacted>', '<redacted> <redacted>:<redacted>:<redacted>', 'https://cdn.realtor.ca/individual/TS<redacted>/highres/<redacted>.jpg', '', '', '', [<redacted>], [<redacted>]);
INSERT OR IGNORE INTO Property_Photo ('SequenceId', 'HighResPath', 'MedResPath', 'LowResPath', 'Description', 'LastUpdated', 'TypeId', 'PhotoGeneratedId') VALUES ('1', 'https://cdn.realtor.ca/listings/TS<redacted>/reb5/highres/4/<redacted>_1.jpg', 'https://cdn.realtor.ca/listings/TS<redacted>/reb5/medres/4/<redacted>_1.jpg', 'https://cdn.realtor.ca/listings/TS<redacted>/reb5/lowres/4/<redacted>_1.jpg', 'Exterior', '<redacted> <redacted>:<redacted>:<redacted> PM', '0', <redacted>);
INSERT OR IGNORE INTO Media ('MediaCategoryId', 'MediaCategoryURL', 'Description', 'Order', 'MediaGeneratedId') VALUES ('1', 'https://passerelle.centris.ca/redirect.aspx?CodeDest=PROPRIO&NoMLS=<redacted>&Lang=E', 'AlternateFeatureSheetWebsite', 1, <redacted>);
INSERT OR IGNORE INTO Listings ('Id', 'MlsNumber', 'PublicRemarks', 'Building_StoriesTotal', 'Property_Price', 'Property_Type', 'Property_Address_AddressText', 'Property_Address_Longitude', 'Property_Address_Latitude', 'Property_Address_PermitShowAddress', 'Property_TypeId', 'Property_FarmType', 'Property_ZoningType', 'Property_PriceUnformattedValue', 'Land_SizeTotal', 'Land_SizeFrontage', 'AlternateURL_DetailsLink', 'PostalCode', 'HistoricalDataIsCleared', 'ProvinceName', 'RelativeDetailsURL', 'StatusId', 'StandardStatusId', 'PhotoChangeDateUTC', 'Distance', 'RelativeURLEn', 'RelativeURLFr', 'InsertedDateUTC', 'TimeOnRealtor', 'UploadedBy', 'Individual', 'Property_Photo', 'Media') VALUES ('<redacted>', '<redacted>', "Terre sur une sortie de l'autoroute <redacted>, première emplacement sur la sortie du Ch. St-André direction du golf de St-Luc... Plus de 3,<redacted>,<redacted> pieds carré de site stratégique GST/QST must be added to the asking price (<redacted>)", '', '$5/sqft +  GST +  QST', 'Vacant Land', 'Ch. St-André|Saint-Jean-sur-Richelieu, Quebec <redacted>', '<redacted>.<redacted>', '<redacted>.<redacted>', True, '<redacted>', 'Other', 'Agricultural', '5', '<redacted>.<redacted> sqft', '<redacted> ft', 'https://passerelle.centris.ca/redirect.aspx?CodeDest=PROPRIO&NoMLS=<redacted>&Lang=E', '<redacted>', False, 'Quebec', '/real-estate/<redacted>/ch-st-andré-saint-jean-sur-richelieu-saint-luc', '1', '1', '<redacted> 3:<redacted>:<redacted> AM', '', '/real-estate/<redacted>/ch-st-andré-saint-jean-sur-richelieu-saint-luc', '/immobilier/<redacted>/ch-st-andré-saint-jean-sur-richelieu-saint-luc', '<redacted>', '', 5, [<redacted>, <redacted>], [<redacted>], [<redacted>]);
```

#### RealtorJSONtoSQLAnalyzer CLI
Create a new full db from existing raw scrapes
`python property_scraper_tools/RealtorJSONtoSQLAnalyzer.py --convert mls_raw_202* --output-database=montreal_full.sqlite`

Incrementally update a minimal db from existing raw scrapes
`python property_scraper_tools/RealtorJSONtoSQLAnalyzer.py --convert mls_raw_202* --output-database=montreal.sqlite --minimal --skip-existing-db-dates --update-output-db`



# TODO
- [ ] Native city support
- [ ] Split off `JSONtoSQLAnalyzer` to its own repo
- [ ] Make RealtorJSONtoSQLAnalyzer example work
- [ ] More examples about usage and ideally code that explains what you should do
- [ ] Fallback to TEXT when handling unsupported items such as lists or dicts

# Thanks
* https://github.com/harry-s-grewal/mls-real-estate-scraper-for-realtor.ca
* https://stackoverflow.com/questions/6027558/flatten-nested-dictionaries-compressing-keys
* https://stackoverflow.com/questions/1832714/18-digit-timestamp/1832746#1832746
* https://devblogs.microsoft.com/azure-sql/the-insert-if-not-exists-challenge-a-solution/
* 