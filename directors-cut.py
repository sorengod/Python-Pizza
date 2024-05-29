import pandas as pd
import requests
from typing import Any, Dict, Union
import sys
import os 
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt



ENDPOINTS = {
    'Spot_Prices' : 'https://api.energidataservice.dk/dataset/Elspotprices?start=2022-01-01&end=2023-01-01&filter={"PriceArea":["DK1", "DK2"]}&sort=HourDK asc',
    'Transmission' : 'https://api.energidataservice.dk/dataset/Transmissionlines?start=2022-01-01&end=2023-01-01&sort=HourDK asc', 
    'Production&Consumption_DK1' : 'https://api.energidataservice.dk/dataset/ProductionConsumptionSettlement?start=2022-01-01&end=2023-01-01&filter={"PriceArea":"DK1"}&sort=HourDK asc',
    'Production&Consumption_DK2' : 'https://api.energidataservice.dk/dataset/ProductionConsumptionSettlement?start=2022-01-01&end=2023-01-01&filter={"PriceArea":"DK2"}&sort=HourDK asc'
}


class Energy_Data:

    def __init__ (self, url):
        self.url = url
    
    def get_data_from_api(self):
        response = requests.get(
        url=self.url
        )
        assert response.status_code == 201
        result = response.json()
        
        return result

    def create_df_from_request(self):
        raw_json = self.get_data_from_api()
        df = pd.DataFrame.from_dict(raw_json)
        return df

    def extract_columns_from_ColumnDictionary(self):
        df = self.create_df_from_request()
        df = df['records'].apply(pd.Series)

        return df[["HourDK", "GrossConsumptionMWh", "PowerToHeatMWh", "OffshoreWindLt100MW_MWh"]]

try:
    energy_data_object = Energy_Data(f'{ENDPOINTS["Production&Consumption_DK2"]}')
    df = energy_data_object.extract_columns_from_ColumnDictionary()
    print(df)
except Exception as e:
    print(f"An error occurred during the instantiation of 'Energy Data' class: {e}")




