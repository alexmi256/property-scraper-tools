""" Contains all queries to the Realtor.ca API and OpenStreetMap."""

import json
from typing import Optional

import requests


class RealtorAPI:
    def __init__(self, cookies_file: str = "cookies.json"):
        self.cookies_file = cookies_file
        self.session = requests.Session()
        cookies = self.load_cookies()
        requests.utils.add_dict_to_cookiejar(self.session.cookies, cookies)
        headers = {
            "Referer": "https://www.realtor.ca/",
            "Origin": "https://www.realtor.ca/",
            "Host": "api2.realtor.ca",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
        }
        self.session.headers.update(headers)

    def save_cookies(self):
        # FIXME: No idea if this actually works
        with open(self.cookies_file, "w") as f:
            json.dump(requests.utils.dict_from_cookiejar(self.session.cookies), f)

    def load_cookies(self) -> dict:
        with open(self.cookies_file, "r") as f:
            try:
                cookies_dict = json.load(f)
                return cookies_dict
            except json.decoder.JSONDecodeError:
                f.seek(0)
                cookies_string = f.readline()
                cookies_dict = {k: v for k, v in [x.strip().split("=", 1) for x in cookies_string.split(";")]}
                return cookies_dict

    def get_coordinates(self, city: str):
        """Gets the coordinate bounds of a city from OpenStreetMap."""

        url = "https://nominatim.openstreetmap.org/search?q=" + city + "&format=json"
        response = self.session.get(url=url, timeout=10)
        response.raise_for_status()
        data = response.json()
        for response in data:
            if response["class"] == "boundary" and response["type"] == "administrative":
                return response["boundingbox"]  # [latMin, latMax, lonMin, lonMax]
        return data

    # pylint: disable=too-many-arguments
    def get_property_list(
        self,
        lat_min,
        lat_max,
        long_min,
        long_max,
        price_min: int = 0,
        price_max: int = 50000000,
        records_per_page: int = 200,
        culture_id: int = 1,
        current_page: int = 1,
        application_id: int = 1,
        sort="6-D",
        bed_range: Optional[str] = None,
    ):
        """Queries the Realtor.ca API to get a list of properties."""

        url = "https://api2.realtor.ca/Listing.svc/PropertySearch_Post"
        form = {
            "LatitudeMin": lat_min,
            "LatitudeMax": lat_max,
            "LongitudeMin": long_min,
            "LongitudeMax": long_max,
            # 6-A is oldest, 6-D is newest
            "Sort": sort,
            "PriceMin": price_min,
            "PriceMax": price_max,
            "RecordsPerPage": records_per_page,
            "CultureId": culture_id,
            "Currency": "CAD",
            "PropertySearchTypeId": "0",
            "TransactionTypeId": "2",
            "PropertyTypeGroupID": "1",
            "CurrentPage": current_page,
            "ApplicationId": application_id,
            "Version": "7.0",
        }
        if bed_range:
            # This should be in the form of \d-\d where the second can be 0 for +
            form["BedRange"] = bed_range
        response = self.session.post(url=url, data=form, timeout=10)
        if response.status_code == 403:
            print("Error 403: Rate limited")
        elif response.status_code != 200:
            print("Error " + str(response.status_code))
        response.raise_for_status()
        return response.json()

    def get_property_details(self, property_id, mls_reference_number):
        """Queries the Realtor.ca API to get details of a property."""

        url = f"https://api2.realtor.ca/Listing.svc/PropertyDetails"
        params = {
            "ApplicationId": 1,
            "CultureId": 1,
            "PropertyID": property_id,
            "ReferenceNumber": mls_reference_number,
        }
        response = self.session.get(url=url, params=params, timeout=10)
        if response.status_code == 403:
            print("Error 403: Rate limited")
        elif response.status_code != 200:
            print("Error " + str(response.status_code))
        response.raise_for_status()
        return response.json()
