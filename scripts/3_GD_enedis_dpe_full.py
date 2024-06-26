
import pandas as pd
import requests
from io import BytesIO
import os

input_filepath = '../data/processed/num_dpe-v2-logements-existants_2022.csv'
url = 'https://data.ademe.fr/data-fair/api/v1/datasets/dpe-v2-logements-existants/master-data/bulk-searchs/identifiant-dpe'
#params = {'select': 'Date_réception_DPE'}
headers = {"Content-Type": 'text/csv'}

def dpe_api(input_filepath=input_filepath,
            output_filepath='output_dpe.csv',
            url=url,
            headers=headers):

    # Set the chunk size
    chunksize = 100000

    # Initialize an empty list to store the chunks
    chunks = []

    # Read the input file in chunks
    for chunk in pd.read_csv(input_filepath, chunksize=chunksize):
        # Convert the chunk to a CSV string
        csv_string = chunk.to_csv(index=False)

        # Send the POST request with the chunk as data
        while True:
            try:
                r = requests.post(url, data=csv_string, headers=headers)
                break
            except requests.exceptions.ChunkedEncodingError:
                continue

        # Read the response content as a CSV file and append it to the list of chunks
        chunks.append(pd.read_csv(BytesIO(r.content)))

    # Concatenate the chunks into a single DataFrame
    df = pd.concat(chunks)

    # Save the DataFrame to the output file
    df.to_csv(output_filepath, index=False)
dpe_api()


# version brut 
"""
def dpe_api(input_filepath=input_filepath,
            output_filepath='output_dpe.csv',
            url=url,
            headers=headers):

    with open(input_filepath, "rb") as csv_file:
        r = requests.post(url,
                          data=csv_file,
                          #params=params,
                          headers=headers
                         )
        # export csv file :
        pd.read_csv(BytesIO(r.content)).to_csv(output_filepath, index=False)
        # Display the dataframe (for test only)
        #return pd.read_csv(BytesIO(r.content))
dpe_api()
"""

# fonction pour couper un csv
"""
# Set the chunk size
chunksize = 500000

# Initialize a counter for the output file names
counter = 1

# Read the input file in chunks
for chunk in pd.read_csv(input_filepath, chunksize=chunksize):
    # Set the output file path
    output_filepath = f"output_{counter}.csv"

    # Write the chunk to a CSV file
    chunk.to_csv(output_filepath, index=False)

    # Increment the counter
    counter += 1
"""
