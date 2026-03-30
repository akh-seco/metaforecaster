# Based on sebastian.rosengren@secotools.com/pipelines/income_read_productnumber

import pandas as pd
import numpy as np
import logging

class DataProcessor:
    def __init__(self, data: pd.DataFrame, new_name_cols: dict, missing_value_cols: list):
        self.data = data
        self.new_name_cols = new_name_cols
        self.missing_value_cols = missing_value_cols

    def rename_cols(self):
        for old_col, new_col in self.new_name_cols.items():
            try:
                if old_col in self.data.columns():
                    self.data.rename(columns={old_col: new_col}, inplace=True)
                    logging.info(f"Column {old_col} successfully renamed to {new_col}")
            except Exception as e:
                logging.error(f"Error trying to rename column {old_col}: {e}")
    
    def impute_cols(self):
        for col in self.missing_value_cols:
            try:
                if self.data[col].isnull().any():
                    self.data[col].fillna('MISSING', inplace=True)
                    logging.info(f"Missing values in {col} column filled with 'MISSING'")
            except Exception as e:
                logging.error(f"Error filling missing values in {col}: {e}")
    
    