import json
from external.client import YandexWeatherAPI
from utils import get_url_by_city_name, CITIES


class DataFetchingTask:
    def __init__(self, city_name: str, cities_dict: dict) -> None:
        self.city_name = city_name
        self.cities_dict = cities_dict

    def get_url(self, city_name: str) -> dict:
        url_with_data = get_url_by_city_name(city_name)
        try:
            resp = YandexWeatherAPI.get_forecasting(url_with_data)
            return resp
        except Exception as e:
            print(f"Error fetching data for {city_name}: {e}")
            return None
        
    def create_json(self, cities_dict: dict) -> None:
        all_data = []

        for city in cities_dict:
            data_dict = self.get_url(city)
            if data_dict is not None:
                all_data.append(data_dict)

        with open('all_cities_data.json', 'w') as file:
            json.dump(all_data, file, indent=2) 


class DataCalculationTask:
    pass


class DataAggregationTask:
    pass


class DataAnalyzingTask:
    pass





