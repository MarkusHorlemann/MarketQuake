'''Corrects "Adjusted Close" column and adds the file name as the final column for each file in given market.'''

import sys, os
from google.cloud import storage

DATA = "gs://marketquake_data"

# Assign argument
market = sys.argv[1]

# Get GCS bucket
client = storage.Client()
bucket = client.get_bucket(DATA.replace('gs://', ''))

# List all files in the market folder
blobs = bucket.list_blobs(prefix=f"stock_market_data/{market}/")

# Iterate over each file in the folder
for blob in blobs:
    # Download the file's content
    content = blob.download_as_text()

    # Add file name at the end of each line
    lines = content.split('\n')
    header_line = lines[0].replace('Adjusted Close', 'AdjustedClose') + ',Name'
    modified_lines = [header_line] + [line + ',' + os.path.basename(blob.name)[:-4] for line in lines[1:]]

    # Join the modified lines into a single string
    modified_content = '\n'.join(modified_lines)

    # Upload the modified content back to the same file, overwriting it
    blob.upload_from_string(modified_content)

    print(f"File {DATA}/{blob.name} modified.")
