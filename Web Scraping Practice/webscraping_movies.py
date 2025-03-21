# Will scrape for the following website:
# https://web.archive.org/web/20230902185655/https://en.everybodywiki.com/100_Most_Highly-Ranked_Films
# We want to obtain the information: Average Rank, Film, and Year.
# We will save the extracted information to a CSV file top_50_films.csv
# Will save the same info to a database Movies.db under the table name Top_50

import pandas as pd
from bs4 import BeautifulSoup
import requests
import sqlite3 # To create the database instance

# SQLite3 is an in-process Python library that implements a self-contained, serverless, zero-configuration,
# transactional SQL database engine. It is a popular choice as an embedded database for local/client storage in application software.

url = 'https://web.archive.org/web/20230902185655/https://en.everybodywiki.com/100_Most_Highly-Ranked_Films'
db_name = 'Movies.db'
table_name = 'Top_50'
csv_path = 'top_50_films.csv'
df = pd.DataFrame(columns=["Average Rank","Film","Year"])
count = 0

# Will first load the entire web page as an HTML document into Python
# We can then parse the text in the HTML format using BeautifulSoup to enable extraction of relevant information

html_page = requests.get(url).text
data = BeautifulSoup(html_page, 'html.parser')

""" Next, open the webpage in a browser, locate the required table by scrolling down to it, right-click it
and click Inspect. This will open the HTML code for the page and take you directly to the point where the definition of
the table begins. If you hover your mouse on tbody, should see the table highlighted.

All rows under this table are mentioned as tr ("table row") objects. Clicking one shows that the data
in each row is also saved as tr objects.

We require the info under the first 3 headers of this stored data.

It is also important to note that this is the first table on the page. 
You must identify the required table when extracting information.
"""

# The rows of the table can be accessed using the find_all method
# tables gets the bodies of ALL the tables in the web page
# rows gets all the rows of the first table

tables = data.find_all('tbody')
rows = tables[0].find_all('tr')

# Iterate over rows to find required data
for row in rows:
    if count<50:
        col = row.find_all('td')
        if len(col)!=0:
            data_dict = {"Average Rank": col[0].contents[0],
                         "Film": col[1].contents[0],
                         "Year": col[2].contents[0]}
            df1 = pd.DataFrame(data_dict, index=[0])
            df = pd.concat([df,df1], ignore_index=True)
            count+=1
    else:
        break

"""- Iterate over the contents of the variable rows.
- Check for the loop counter to restrict to 50 entries.
- Extract all the td ("data cell") data objects in the row and save them to col.
- Check if the length of col is 0, that is, if there is no data in a current row. This is important since, many timesm there are merged rows that are not apparent in the web page appearance.
- Create a dictionary data_dict with the keys same as the columns of the dataframe created for recording the output earlier and corresponding values from the first three headers of data.
- Convert the dictionary to a dataframe and concatenate it with the existing one. This way, the data keeps getting appended to the dataframe with every iteration of the loop.
- Increment the loop counter.
- Once the counter hits 50, stop iterating over rows and break the loop.
"""

print(df)

# Save the dataframe to a CSV file
df.to_csv(csv_path)

"""
To store the required data in a database, you first need to initialize a connection to the database, 
save the dataframe as a table, and then close the connection.

False index makes sure that the dataframes indices aren't written as a column in the database table.
"""

conn = sqlite3.connect(db_name)
df.to_sql(table_name, conn, if_exists='replace', index=False)
conn.close()

# You can connect to SQLIte3 using the connect() function by passing the required database name as an argument.
# This makes the variable sql_connection an object of the SQL code engine. You can then use this to run the required queries on the database.
# use the to_sql() function to convert the pandas dataframe to an SQL table.