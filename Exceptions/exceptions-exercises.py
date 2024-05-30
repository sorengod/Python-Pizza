import pandas as pd
import requests
from typing import Any, Dict, Union


'''
Why Exceptions are Good:

Exceptions in Python are a powerful mechanism for handling errors and exceptional conditions in code. 
They provide a structured way to deal with unexpected situations, improve code robustness, and enhance user experience. 
Here are some reasons why exceptions are good:

1. Granular Error Handling: Exceptions allow developers to handle different types of errors individually, enabling more fine-grained control over error recovery and mitigation strategies.

2. Control Flow: Exception handling enables the alteration of program flow in response to errors, allowing code to gracefully handle exceptional conditions without terminating abruptly.

3. Error Context: Exceptions carry additional context about the error, including traceback information, which helps in diagnosing and debugging issues more effectively.

4. Modularity and Reusability: By encapsulating error-handling logic within exceptions, code becomes more modular and reusable, leading to cleaner code organization and easier maintenance.

5. Exceptional Conditions: Exceptions are designed to handle exceptional cases where normal execution cannot proceed, distinguishing them from expected errors that are part of the program's regular operation.

6. Robustness: Properly handled exceptions make code more robust and resilient to unexpected situations, preventing crashes and providing better user experience.

Exercises:

1. HTTPError Handling: Implement assert handling in the `get_data_from_api` method to catch HTTP errors (e.g., 404, 500, 201).
2. Endpoint error handling: wrap the instanziation of class Energy Data in a Try&Except. Make a small error in the ENDPOINT variable, and see the results.
3. Force an exception or an AssertionError and log the error to your log file
'''

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

object = Energy_Data(f'{ENDPOINTS["Production&Consumption_DK2"]}')
df = object.extract_columns_from_ColumnDictionary()

if __name__ == "__main__":
    # Code here runs only when main.py is executed directly
    print("This script is being run directly.")
    print(df)




