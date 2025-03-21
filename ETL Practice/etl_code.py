import glob # To call the correct function for data extraction

import pandas as pd # Can read .csv and .json with pandas
import xml.etree.ElementTree as ET # xml.etree ElementTree function can be used to parse data from an .xml file
from datetime import datetime # To correctly log the information from our ETL process

log_file = "log_file.txt"
target_file = "transformed_data.csv"

### Extraction Functions ###
def extract_from_csv(csv_file):
    dataframe = pd.read_csv(csv_file)
    return dataframe

def extract_from_json(json_file):
    dataframe = pd.read_json(json_file, lines=True)
    return dataframe

# To extract from XML file, we first need to parse data using the ElementTree function
# We can then extract relevant information and append it a pandas dataframe
# use pd.concat over pd.append which is deprecated
# pd.concat include the ignore_index=true argument which resets index to appropriate value when combining dataframes

def extract_from_xml(xml_file):
    df = pd.DataFrame(columns=["name", "height", "weight"])
    tree = ET.parse(xml_file)
    root = tree.getroot()
    for person in root:
        name = person.find("name").text
        height = float(person.find("height").text)
        weight = float(person.find("weight").text)
        df = pd.concat([df, pd.DataFrame([{"name":name, "height":height, "weight":weight}])], ignore_index=True)
    return df

# Need a function to identify which extraction function to call, based on data type

def extract():
    extracted = pd.DataFrame(columns=['name', 'height', 'weight'])  # create an empty data frame to hold extracted data

    # process all csv files, except the target file
    for csvfile in glob.glob("*.csv"):
        if csvfile != target_file:  # check if the file is not the target file
            extracted = pd.concat([extracted, pd.DataFrame(extract_from_csv(csvfile))], ignore_index=True)

            # process all json files
    for jsonfile in glob.glob("*.json"):
        extracted = pd.concat([extracted, pd.DataFrame(extract_from_json(jsonfile))], ignore_index=True)

        # process all xml files
    for xmlfile in glob.glob("*.xml"):
        extracted = pd.concat([extracted, pd.DataFrame(extract_from_xml(xmlfile))], ignore_index=True)

    return extracted

### Transform Function ###

# The height in the extracted data is in inches, and the weight is in pounds.
# We want the height to be in meters, and the weight to be in kilograms.

def transform(dataframe: pd.DataFrame) -> pd.DataFrame:
    """ Convert inches to meters and round off to two decimals
     1 inch is 0.0254 meters. Convert pounds to kilograms and round off to two decimals
     1 pound is 0.45359237 kilograms.
     """
    dataframe['height'] = round(dataframe.height * 0.0254, 2)
    dataframe['weight'] = round(dataframe.weight * 0.45359237,2)

    return dataframe

### Loading and Logging ###

# Want to load the data into a CSV file which could later be loaded into a database

def load_data(target_file, transformed_data: pd.DataFrame) -> None:
    transformed_data.to_csv(target_file)

# Implement a logging operation to record the progress of different operations

def log_progress(message: str) -> None:
    timestamp_format = '%Y-%h-%d-%H:%M:%S'  # Year-Monthname-Day-Hour-Minute-Second
    now = datetime.now()  # get current timestamp
    timestamp = now.strftime(timestamp_format)
    with open(log_file, "a") as f:
        f.write(timestamp + ',' + message + '\n')

if __name__ == '__main__':

    # Simple Test
    # Log the initialization of the ETL process
    log_progress("ETL Job Started")

    # Log the beginning of the Extraction process
    log_progress("Extract phase Started")
    extracted_data = extract()

    # Log the completion of the Extraction process
    log_progress("Extract phase Ended")

    # Log the beginning of the Transformation process
    log_progress("Transform phase Started")
    transformed_data = transform(extracted_data)
    print("Transformed Data")
    print(transformed_data)

    # Log the completion of the Transformation process
    log_progress("Transform phase Ended")

    # Log the beginning of the Loading process
    log_progress("Load phase Started")
    load_data(target_file, transformed_data)

    # Log the completion of the Loading process
    log_progress("Load phase Ended")

    # Log the completion of the ETL process
    log_progress("ETL Job Ended")