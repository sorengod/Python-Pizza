import pandas as pd
import requests
from typing import Any, Dict, Union

ENDPOINTS = {
    'Spot_Prices' : 'https://api.energidataservice.dk/dataset/Elspotprices?start=2022-01-01&end=2023-01-01&filter={"PriceArea":["DK1", "DK2"]}&sort=HourDK asc',
    'Transmission' : 'https://api.energidataservice.dk/dataset/Transmissionlines?start=2022-01-01&end=2023-01-01&sort=HourDK asc', 
    'Production&Consumption_DK1' : 'https://api.energidataservice.dk/dataset/ProductionConsumptionSettlement?start=2022-01-01&end=2023-01-01&filter={"PriceArea":"DK1"}&sort=HourDK asc',
    'Production&Consumption_DK2' : 'https://api.energidataservice.dk/dataset/ProductionConsumptionSettlement?start=2022-01-01&end=2023-01-01&filter={"PriceArea":"DK2"}&sort=HourDK asc'
}

class Energy_Data:
    def __init__(self, url: str):
        """
        Initialize the Energy_Data object with the given URL.
        
        :param url: The API endpoint URL.
        """
        self.url = url
        self.columns_to_extract = ["HourDK", "GrossConsumptionMWh", "PowerToHeatMWh", "OffshoreWindLt100MW_MWh"]
    
    def get_data_from_api(self) -> Dict[str, Any]:
        """
        Fetch data from the API.

        :return: JSON response as a dictionary.
        """
        response = requests.get(url=self.url)
        result = response.json()
        return result

    def create_df_from_request(self) -> pd.DataFrame:
        """
        Create a DataFrame from the API response.

        :return: A pandas DataFrame created from the JSON response.
        """
        raw_json = self.get_data_from_api()
        df = pd.DataFrame.from_dict(raw_json)
        return df

    def extract_columns_from_ColumnDictionary(self) -> pd.DataFrame:
        """
        Extract specific columns from the DataFrame.

        :return: A pandas DataFrame with selected columns.
        """
        df = self.create_df_from_request()
        df = df['records'].apply(pd.Series)
        return df[self.columns_to_extract]

# Example usage
object = Energy_Data(f'{ENDPOINTS["Production&Consumption_DK2"]}')
df = object.extract_columns_from_ColumnDictionary()
print(df)
