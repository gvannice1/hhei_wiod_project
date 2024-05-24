import pandas as pd
import numpy as np
import pickle 

total_int = 'Total intermediate consumption'
output = 'Output at basic prices'
labor_comp = 'Labour compensation at basic prices'
capital_comp = 'Capital compensation at basic prices'


df_costs = pd.read_parquet("wiod_data/df_costs.parquet")

unique_industries = df_costs['Industry'].unique()
df_costs['Industry'] = pd.Categorical(df_costs['Industry'], categories=unique_industries, ordered=True)


df_costs = df_costs[df_costs['Cost Category'] != output]
df_costs = df_costs[df_costs['Cost Category'] != labor_comp]
df_costs = df_costs[df_costs['Cost Category'] != capital_comp]
df_costs = df_costs[df_costs['Cost Category'] != total_int]


with open('wiod_data/final_demand_dict.pkl', 'rb') as file:
    final_demand_dict = pickle.load(file)

with open('wiod_data/intermediate_dict.pkl', 'rb') as file:
    intermediate_dict = pickle.load(file)

countries = list(intermediate_dict.keys())

sum_check = {}

for country in countries:

    #load in intermediate and final demand for given country
    intermediate_inputs = intermediate_dict[country]
    final_demand = final_demand_dict[country] 

    # Extract destination industries and countries
    destination_industries = intermediate_inputs.columns.tolist()

    # Extract origin industries and countries
    origin_industries = intermediate_inputs.index.tolist()

    # Prepare a list to collect data
    data = []

    # Iterate through the DataFrame, skipping the first two rows and columns
    for i, origin_industry in enumerate(origin_industries):
        for j, destination_industry in enumerate(destination_industries):
            value = intermediate_inputs.iat[i, j]
            if pd.notnull(value):  # Only add rows where value is not NaN and we dont want ROW data since there is no capital or labor data available 
                data.append({               
                    'Origin Industry': origin_industry,                
                    'Industry': destination_industry,
                    'Value': value 
                })

    # Convert the data list to a DataFrame
    intermediate_inputs = pd.DataFrame(data)

    #fix rows so they dont change when grouped
    unique_industries = intermediate_inputs['Origin Industry'].unique()
    intermediate_inputs['Origin Industry'] = pd.Categorical(intermediate_inputs['Origin Industry'], categories=unique_industries, ordered=True)
    intermediate_inputs['Industry'] = pd.Categorical(intermediate_inputs['Industry'], categories=unique_industries, ordered=True)



    #now lets get to sum checking 
    #start with finding total use
    total_use = intermediate_inputs.groupby(['Origin Industry'], observed = True)['Value'].sum().reset_index()

    total_use['Value'] = total_use['Value'] + final_demand['Value']


    #now find total supply
    total_supply = intermediate_inputs.groupby(['Industry'], observed = True)['Value'].sum().reset_index()

    #create value added dataframe
    df_value_added = df_costs[df_costs['Country'] == country]
    df_value_added = df_value_added.groupby(['Industry'], observed = True)['Cost'].sum().reset_index()
    print(df_value_added)
    #create row for dollars, append to value added
    df_dollars = pd.DataFrame({
            'Cost' : 0,
            'Industry' : ['Dollars']
        })
    df_value_added = pd.concat([df_value_added,df_dollars], axis = 0).reset_index(drop=True)

    total_supply['Value'] = total_supply['Value'] + df_value_added['Cost']

    #create total use and supply df
    total_supply_and_use = total_supply.rename(columns={'Value': 'Total Supply'})
    total_supply_and_use['Total Use'] = total_use['Value']

    total_supply_and_use['Difference'] = abs(total_supply_and_use['Total Use'] - total_supply_and_use['Total Supply'])

    sum_check[country] = total_supply_and_use


#for country in countries:
   # sum_check[country].to_excel(f'wiod by country/Sum check by country/sum_check_{country}.xlsx', sheet_name = 'Sheet 1', index = False)

