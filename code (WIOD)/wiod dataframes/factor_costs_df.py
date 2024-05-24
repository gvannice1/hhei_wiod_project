import pandas as pd
import os

Output_at_basic_prices = 9

# Load the Excel file
datafile_path = os.path.join('wiod_data/factor costs', 'factor_costs_2014.xlsx')
df_costs = pd.read_excel(datafile_path, header = None, engine='openpyxl')

# Extract destination industries and countries
destination_industries = df_costs.iloc[0, 1:].tolist()
destination_countries = df_costs.iloc[1, 1:].tolist()

# Extract origin industries and countries
cost_category = df_costs.iloc[2:, 0].tolist()


# Prepare a list to collect data
cost_data = []

# Iterate through the DataFrame, skipping the first two rows and columns
for i, (cost_category) in enumerate(cost_category):
    for j, (destination_country, destination_industry) in enumerate(zip(destination_countries, destination_industries)):
        cost = df_costs.iat[i+2, j+1]
        if pd.notnull(cost):  # Only add rows where value is not NaN
            cost_data.append({
                'Cost Category': cost_category,               
                'Country': destination_country,
                'Industry': destination_industry,
                'Cost': cost
            })



# Convert the data list to a DataFrame
df_costs_final = pd.DataFrame(cost_data)

#df_costs_final.to_parquet('df_costs.parquet')
#Create Gross Output data
#initilize list to hold data
GO_data = []

for j, (destination_country, destination_industry) in enumerate(zip(destination_countries, destination_industries)):
        GO = df_costs.iat[Output_at_basic_prices, j+1]
        if pd.notnull(GO):  # Only add rows where value is not NaN
            GO_data.append({
                'Country': destination_country,
                'Industry': destination_industry,
                'Gross Output': GO
            })

#convert back to 
df_gross_output = pd.DataFrame(GO_data)

df_gross_output.to_parquet('df_gross_output.parquet')
