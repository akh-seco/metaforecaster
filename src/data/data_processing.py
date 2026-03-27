# Based on sebastian.rosengren@secotools.com/pipelines/income_read_productnumber

import pandas as pd
import numpy as np
import logging

class DataProcessor:
    def __init__(self, data: pd.DataFrame):
        self.data = data
    
    def rename_columns(self):
        try:
            if 'ProductNumber' in self.data.columns():
                self.data.rename(columns={'ProductNumber': 'ItemNumber'},inplace=True)
                logging.info("Column ProductNumber successfully renamed to ItemNumber")
            if 'SupplyingWarehouse' in self.data.columns():
                self.data.rename(columns={'SupplyingWarehouse': 'Warehouse'}, inplace=True)
                logging.info("Column SupplyingWarehouse successfully renamed to Warehouse")
            if 'CountryOfOrigin' in self.data.columns():
                self.data.rename(columns={'CountryOfOrigin': 'SalesMarket'}, inplace=True)
                logging.info("Column CountryOfOrigin successfully renamed to SalesMarket")
        except Exception as e:
            logging.error(
                f"Error trying to rename columns ProductNumber, SupplyingWarehouse, \
                CountryOfOrigin: {e}")
    
    def imputation_salesmarket_warehouse(self):
        try:
            if self.data['SalesMarket'].isnull().any():
                self.data['SalesMarket'].fillna('MISSING', inplace=True)
                logging.info("Missing values in SalesMarket column filled with 'MISSING'")
            if self.data['Warehouse'].isnull().any():
                self.data['Warehouse'].fillna('MISSING', inplace=True)
                logging.info("Missing values in Warehouse column filled with 'MISSING'")
        except Exception as e:
            logging.error(f"Error filling missing values in SalesMarket, Warehouse column: {e}")