import pandas as pd
import os

# Load the Excel file
datafile_path = os.path.join('wiod_data/intermediate inputs', 'intermediate_inputs_2014.xlsx')
df_intermediate = pd.read_excel(datafile_path, header = None, engine='openpyxl')

# Extract destination industries and countries
destination_industries = df_intermediate.iloc[0, 2:].tolist()
destination_countries = df_intermediate.iloc[1, 2:].tolist()

# Extract origin industries and countries
origin_industries = df_intermediate.iloc[2:, 0].tolist()
origin_countries = df_intermediate.iloc[2:, 1].tolist()

# Prepare a list to collect data
data = []

# Iterate through the DataFrame, skipping the first two rows and columns
for i, (origin_country, origin_industry) in enumerate(zip(origin_countries, origin_industries)):
    for j, (destination_country, destination_industry) in enumerate(zip(destination_countries, destination_industries)):
        value = df_intermediate.iat[i+2, j+2]
        if pd.notnull(value):  # Only add rows where value is not NaN and we dont want ROW data since there is no capital or labor data available 
            data.append({
                'Origin Country': origin_country,
                'Origin Industry': origin_industry,
                'Country': destination_country,
                'Industry': destination_industry,
                'Value': value 
            })

# Convert the data list to a DataFrame
df_intermediate_final = pd.DataFrame(data)

df_intermediate_final.to_parquet("df_intermediate_wROW.parquet")

