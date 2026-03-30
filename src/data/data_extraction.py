import pandas as pd
import logging

class DataExtractor:
    def __init__(self, file_path: str):
        self.file_path = file_path
    
    def extract_data(self):
        try:
            data = pd.read_csv(self.file_path)
            logging.info(f"Data successfully extracted from {self.file_path}")
            return data
        except Exception as e:
            logging.error(f"Error extracting data from {self.file_path}: {e}")

            
