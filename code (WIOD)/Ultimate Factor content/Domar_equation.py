import pandas as pd
import numpy as np
import pickle 

total_int = 'Total intermediate consumption'
output = 'Output at basic prices'


df_costs = pd.read_parquet("wiod_data/df_costs.parquet")

unique_industries = df_costs['Industry'].unique()
df_costs['Industry'] = pd.Categorical(df_costs['Industry'], categories=unique_industries, ordered=True)



with open('wiod_data/final_demand_dict.pkl', 'rb') as file:
    final_demand_dict = pickle.load(file)

with open('wiod_data/intermediate_dict.pkl', 'rb') as file:
    intermediate_dict = pickle.load(file)

countries = list(intermediate_dict.keys())
countries.remove('ROW')


total_output = df_costs[df_costs['Cost Category'] == output].reset_index(drop = True)
total_int = df_costs[df_costs['Cost Category'] == total_int].reset_index(drop=True)
df_value_added = total_output.copy(deep = True).reset_index(drop = True)
df_value_added['Cost'] = total_output['Cost'] - total_int['Cost']


df_value_added = df_value_added.groupby(['Country'], observed = True)['Cost'].sum().reset_index()

df_total_expenditures = []

for country in countries:
     # Sum the 'Value' for the current country
     df_total_expenditures_temp = final_demand_dict[country]['Value'].sum()

      # Create a dictionary with the data and append it to the data_list
     df_total_expenditures.append({
         'Country': country,
          'Value': df_total_expenditures_temp
     })


df_total_expenditures = pd.DataFrame(df_total_expenditures)


df_GDP_check = df_total_expenditures.drop('Value',axis = 1).copy(deep = True)
df_GDP_check['GDP Expenditures'] = df_total_expenditures['Value']
df_GDP_check['GDP Value Added'] = df_value_added['Cost']
df_GDP_check['Difference'] = df_total_expenditures['Value'] - df_value_added['Cost']

#df_GDP_check.to_excel('GDP_Check_expenditures_minus_VA.xlsx', index = False)
#df_GDP_check.to_parquet('df_GDP_check.parquet')
#find final expenditure shares and domar weights
for country in countries:
    final_demand_df = final_demand_dict[country]
    final_demand_df = final_demand_df.rename(columns = {'Origin Industry': 'Industry'})

    GDP = df_GDP_check[df_GDP_check['Country'] == country]
    GDP = float(GDP.iloc[0,1])

    final_demand_shares = final_demand_df.copy(deep = True)
    final_demand_shares['Value'] = final_demand_df['Value'] / GDP


    #Now onto domar weights
    #first we need to create the intermediate df
    intermediate_inputs = intermediate_dict[country]
    destination_industries = intermediate_inputs.columns.tolist()
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
    intermediate_inputs_df = pd.DataFrame(data)

    #fix industrys
    unique_industries = intermediate_inputs_df['Industry'].unique()
    final_demand_df['Industry'] = pd.Categorical(final_demand_df['Industry'], categories=unique_industries, ordered=True)
    intermediate_inputs_df['Origin Industry'] = pd.Categorical(intermediate_inputs_df['Origin Industry'], categories=unique_industries, ordered=True)
    intermediate_inputs_df['Industry'] = pd.Categorical(intermediate_inputs_df['Industry'], categories=unique_industries, ordered=True)

    #find GO
    df_GO = final_demand_df.copy(deep = True).drop('Value', axis = 1)
    final_demand_GO =  final_demand_df.groupby(['Industry'], observed = True)['Value'].sum().reset_index() 
    intermediate_GO = intermediate_inputs_df.groupby(['Origin Industry'], observed = True)['Value'].sum().reset_index()
    df_GO['Gross Output'] = final_demand_GO['Value'] + intermediate_GO['Value']

    #take domar weights
    domar_weights = df_GO.copy(deep=True).drop('Gross Output', axis = 1)
    domar_weights['Domar Weights'] = df_GO['Gross Output'] / GDP

    #create input-output matrix 

    intermediate_shares_of_GO = pd.merge(intermediate_inputs_df, df_GO, on = 'Industry')
    intermediate_shares_of_GO['IO shares'] = intermediate_shares_of_GO['Value'] / intermediate_shares_of_GO['Gross Output']
    intermediate_shares_of_GO['IO shares'] = intermediate_shares_of_GO['IO shares'].fillna(0)
     
    #pivot to be n by n df
    intermediate_shares = intermediate_shares_of_GO.pivot(index='Origin Industry', columns='Industry', values='IO shares')

    intermediate_shares_numeric = intermediate_shares.select_dtypes(include=[np.number])
    intermediate_shares_matrix = intermediate_shares_numeric.values
    final_demand_shares_numeric = final_demand_shares.select_dtypes(include=[np.number])
    final_demand_vector = final_demand_shares_numeric.values

    #find domar
    I = np.identity(intermediate_shares_matrix.shape[0])    #initialize identity matrix

    leontief_inverse = np.linalg.inv(I - intermediate_shares_matrix.T)     #Take the leontieff inverse

    ultimate_content = np.matmul(final_demand_vector.T, leontief_inverse)
    
    domar_weights['Ultimate Content'] = ultimate_content.T

    domar_weights['Final Demand Shares'] = final_demand_shares['Value']

  #  with pd.ExcelWriter(f'wiod by country/Domar weights by country/Domar_WIOD_{country}.xlsx') as writer:
   #    domar_weights.to_excel(writer, sheet_name='Domar Weights',index = False)
   #    intermediate_shares.T.to_excel(writer, sheet_name='Intermediate Shares')
    
print(domar_weights)


