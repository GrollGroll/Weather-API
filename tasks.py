import json
from external.client import YandexWeatherAPI
from utils import get_url_by_city_name, CITIES

class DataFetchingTask:
    def __init__(self, city_name: str, cities_dict: dict) -> None:
        self.city_name = city_name
        self.cities_dict = cities_dict

    @staticmethod
    def get_url(city_name: str) -> dict:
        url_with_data = get_url_by_city_name(city_name)
        try:
            resp = YandexWeatherAPI.get_forecasting(url_with_data)
            return resp
        except Exception as e:
            print(f"Error fetching data for {city_name}: {e}")
            return None

    def create_json(self, filename: str = 'all_cities_data.json') -> None:
        with open(filename, 'w') as file:
            data_generator = (self.get_url(city) for city in self.cities_dict)
            filtered_data = filter(None, data_generator)
            json.dump(list(filtered_data), file, indent=2)


task = DataFetchingTask(city_name='Moscow', cities_dict=CITIES)
task.create_json()



class DataCalculationTask:
    pass


class DataAggregationTask:
    pass


class DataAnalyzingTask:
    pass





