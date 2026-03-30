# Based on sebastian.rosengren@secotools.com/pipelines/income_read_productnumber
# The data is supposed to come monthly. 
# We need to divide each month into the working days of that month.

import pandas as pd
import logging

class DataProcessor:
    def __init__(self, data: pd.DataFrame, new_name_cols: dict, missing_value_cols: list, work_days_per_month: pd.DataFrame):
        self.data = data
        self.new_name_cols = new_name_cols
        self.missing_value_cols = missing_value_cols
        self.work_days_per_month = work_days_per_month

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

    def impute_total_orders(self):
        try:
            if 'TotalOrders' in self.data.columns() and self.data['TotalOrders'].isnull().any():
                self.data['TotalOrders'].fillna(0, inplace=True)
                logging.info("Missing values in total orders column filled with 0")
        except Exception as e:
            logging.error(f"Error filling missing values in total orders column: {e}")

    def aggregate_month_total_orders(self):
        try:
            if 'TotalOrders' in self.data.columns():
                self.data = self.data.groupby(['itemNumber', 'Warehouse', 'SalesMarket'], as_index=False)['TotalOrders'].sum()
                logging.info("Total orders successfully aggregated by item number, warehouse, and sales market")
        except Exception as e:
            logging.error(f"Error aggregating total orders: {e}")

    def calculate_average_daily_total_orders(self):
        try:
            self.data = self.data.merge(self.work_days_per_month, on='Month', how='left')
            self.data['TotalOrders_scaled'] = self.data['TotalOrders'] / self.data['WorkDays']
            logging.info("Average daily total orders successfully calculated")
        except Exception as e:
            logging.error(f"Error calculating average daily total orders: {e}")
        

    
    