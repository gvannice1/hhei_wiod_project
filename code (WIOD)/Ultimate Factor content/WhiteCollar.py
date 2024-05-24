import pandas as pd
import numpy as np
import pickle
import os


labor_exp = 'Labour compensation at basic prices'


df_costs = pd.read_parquet("wiod_data/df_costs.parquet")
df_GDP_check = pd.read_parquet("wiod_data/df_GDP_check.parquet")
df_share_wc = pd.read_csv('wiod_data/ipums_share_wc.csv')

industries_matrix = pd.read_excel('wiod_data/industries_matrix.xlsx', header = None, engine='openpyxl')

unique_industries = df_costs['Industry'].unique()
df_costs['Industry'] = pd.Categorical(df_costs['Industry'], categories=unique_industries, ordered=True)


with open('wiod_data/final_demand_dict.pkl', 'rb') as file:
    final_demand_dict = pickle.load(file)

with open('wiod_data/intermediate_trans.pkl', 'rb') as file:
    intermediate_dict = pickle.load(file)

countries = list(intermediate_dict.keys())
countries.remove('ROW')


final_demand_USA = final_demand_dict['USA']
intermediate_USA = intermediate_dict['USA']

df_share_USA_wc = df_share_wc[df_share_wc['isocode'] == 'USA']

#Add wc/bc final expenditure to make final demand have same rows as the labor augmented intermediate matrix
df_white_collar_final_demand = pd.DataFrame({
    'Value' : 0,
    'Origin Industry' : ['White Collar Labor']
    })
df_blue_collar_final_demand = pd.DataFrame({
    'Value' : 0,
    'Origin Industry' : ['Blue Collar Labor']
    })

final_demand_USA = pd.concat([final_demand_USA, df_white_collar_final_demand], axis = 0).reset_index(drop = True)

final_demand_USA_labor = pd.concat([final_demand_USA, df_blue_collar_final_demand], axis = 0).reset_index(drop = True)


# Convert to numeric, coercing when necessary
industries_matrix = industries_matrix.apply(pd.to_numeric, errors='coerce')

industries_matrix = industries_matrix.to_numpy()[1:, 1:] 


#Get the share of labor that is wc/bc by wiod industry 
df_share_USA_wc = df_share_USA_wc['share_wc'].to_numpy()

wiod_share_wc = np.matmul(industries_matrix , df_share_USA_wc)

df_share_USA_bc = 1 - df_share_USA_wc

wiod_share_bc = np.matmul(industries_matrix , df_share_USA_bc)


#find the total labor expenditures then multiply by the share of that industry that is wc/bc
df_costs_USA = df_costs[df_costs['Country'] == 'USA']

df_labor_exp = df_costs_USA[df_costs_USA['Cost Category'] == labor_exp]

df_labor_exp_bc = df_labor_exp.copy(deep = True)
df_labor_exp_wc = df_labor_exp.copy(deep = True)

df_labor_exp_wc['Cost'] = df_labor_exp['Cost'] * wiod_share_wc
df_labor_exp_bc['Cost'] = df_labor_exp['Cost'] * wiod_share_bc


#add to intermediate 
df_labor_exp_wc = df_labor_exp_wc[['Industry', 'Cost']].reset_index(drop=True)

df_labor_exp_wc.rename(columns = {'Cost' : 'White Collar Labor'}, inplace=True)

labor_exp_pivot_wc = df_labor_exp_wc.pivot_table(
        index = 'Industry',
        values = 'White Collar Labor',
        aggfunc = 'sum'
     )

df_labor_exp_bc = df_labor_exp_bc[['Industry', 'Cost']].reset_index(drop=True)
df_labor_exp_bc.rename(columns = {'Cost' : 'Blue Collar Labor'}, inplace=True)

labor_exp_pivot_bc = df_labor_exp_bc.pivot_table(
        index = 'Industry',
        values = 'Blue Collar Labor',
        aggfunc = 'sum'
     )
     
intermediate_USA_labor = pd.concat([intermediate_USA, labor_exp_pivot_wc], axis = 1)
intermediate_USA_labor = pd.concat([intermediate_USA_labor, labor_exp_pivot_bc], axis = 1)

labor_exp_destination_wc = labor_exp_pivot_wc.copy(deep = True)
labor_exp_destination_bc = labor_exp_pivot_bc.copy(deep = True)
labor_exp_destination_wc['White Collar Labor'] = labor_exp_destination_wc['White Collar Labor'] * 0
labor_exp_destination_bc['Blue Collar Labor'] = labor_exp_destination_bc['Blue Collar Labor'] * 0

intermediate_USA_labor = pd.concat([intermediate_USA_labor, labor_exp_destination_wc.T], axis = 0)

intermediate_USA_labor = pd.concat([intermediate_USA_labor, labor_exp_destination_bc.T], axis = 0)

intermediate_USA_labor.iloc[56,57] = 0
intermediate_USA_labor.iloc[57,56] = 0
intermediate_USA_labor.iloc[57,57] = 0
intermediate_USA_labor.iloc[58,57] = 0
intermediate_USA_labor.iloc[57,58] = 0
intermediate_USA_labor.iloc[58,58] = 0
intermediate_USA_labor.iloc[58,56] = 0
intermediate_USA_labor.iloc[56,58] = 0


#intermediate_USA_labor.to_excel('labor_intermediate.xlsx', sheet_name='sheet 1', index = True)


destination_industries = intermediate_USA_labor.index.tolist()
origin_industries = intermediate_USA_labor.columns.tolist()

# Prepare a list to collect data
data = []

 # Iterate through the DataFrame, skipping the first two rows and columns
for j, origin_industry in enumerate(origin_industries):
     for i, destination_industry in enumerate(destination_industries):
         value = intermediate_USA_labor.iat[i, j]
         if pd.notnull(value):  # Only add rows where value is not NaN and we dont want ROW data since there is no capital or labor data available 
             data.append({               
                'Origin Industry': origin_industry,                
                'Industry': destination_industry,
                'Value': value 
            })
intermediate_inputs_df = pd.DataFrame(data)

#fix industrys
unique_industries = intermediate_inputs_df['Industry'].unique()
final_demand_USA_labor['Origin Industry'] = pd.Categorical(final_demand_USA_labor['Origin Industry'], categories=unique_industries, ordered=True)
intermediate_inputs_df['Origin Industry'] = pd.Categorical(intermediate_inputs_df['Origin Industry'], categories=unique_industries, ordered=True)
intermediate_inputs_df['Industry'] = pd.Categorical(intermediate_inputs_df['Industry'], categories=unique_industries, ordered=True)

#find GO
df_GO = final_demand_USA_labor.copy(deep = True).drop('Value', axis = 1)
df_GO = df_GO.rename(columns = {'Origin Industry': 'Industry'})
final_demand_GO =  final_demand_USA_labor.groupby(['Origin Industry'], observed = True)['Value'].sum().reset_index() 
intermediate_GO = intermediate_inputs_df.groupby(['Origin Industry'], observed = True)['Value'].sum().reset_index()
df_GO['Gross Output'] = final_demand_GO['Value'] + intermediate_GO['Value']

#create input-output matrix 
intermediate_shares_of_GO = pd.merge(intermediate_inputs_df, df_GO, on = 'Industry')
intermediate_shares_of_GO['IO shares'] = intermediate_shares_of_GO['Value'] / intermediate_shares_of_GO['Gross Output']
intermediate_shares_of_GO['IO shares'] = intermediate_shares_of_GO['IO shares'].fillna(0)

   #pivot to be n by n df
intermediate_shares = intermediate_shares_of_GO.pivot(columns='Origin Industry', index='Industry', values='IO shares')

intermediate_shares_numeric = intermediate_shares.select_dtypes(include=[np.number])
intermediate_shares_matrix = intermediate_shares_numeric.values

#find final demand shares vector
GDP = df_GDP_check[df_GDP_check['Country'] == 'USA']
GDP = float(GDP.iloc[0,1])

final_demand_shares = final_demand_USA_labor.copy(deep = True)
final_demand_shares['Value'] = final_demand_USA_labor['Value'] / GDP

final_demand_shares_numeric = final_demand_shares.select_dtypes(include=[np.number])
final_demand_vector = final_demand_shares_numeric.values


#get leontief
I = np.identity(intermediate_shares_matrix.shape[0])    #initialize identity matrix

leontief_inverse = np.linalg.inv(I - intermediate_shares_matrix)     #Take the leontieff inverse

ultimate_content = np.matmul(final_demand_vector.T, leontief_inverse)


ultimate_content_df = pd.DataFrame(ultimate_content.T)

ultimate_content_labeled = final_demand_shares.copy(deep = True).drop('Value', axis = 1)
ultimate_content_labeled['Ultimate content'] = ultimate_content_df


intermediate_shares_labor = pd.DataFrame(intermediate_shares_matrix)

#with pd.ExcelWriter(f'wiod by country/labor shares USA.xlsx') as writer:
  #  intermediate_USA_labor.to_excel(writer, sheet_name='Intermediate With Labor', index = True)
 #   intermediate_shares_labor.to_excel(writer, sheet_name='Intermediate Shares',index = True)
 #   ultimate_content_labeled.to_excel(writer, sheet_name='Utimate Content', index = False)


