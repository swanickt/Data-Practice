import glob # To call the correct function for data extraction
import pandas as pd # Can read .csv and .json with pandas
import xml.etree.ElementTree as ET # xml.etree ElementTree function can be used to parse data from an .xml file
from datetime import datetime # To correctly log the information from our ETL process

log_file = "log_file.txt"
target_file = "transformed_data.csv"