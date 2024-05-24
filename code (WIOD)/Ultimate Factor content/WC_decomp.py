import pandas as pd
import numpy as np
import pickle
import os


labor_exp = 'Labour compensation at basic prices'
public_and_ag = ['Crop and animal production, hunting and related service activities','Public administration and defence; compulsory social security','Forestry and logging','Fishing and aquaculture',
                 'Education','Human health and social work activities']

gdp_capita_data = pd.read_excel('wiod_data/GDP per capita data.xlsx')

gdp_capita_data = gdp_capita_data.dropna(subset = ['GDP_per_capita'])

twn_df = pd.DataFrame({
    'Country': 'TWN',
    'GDP_per_capita': 22844},index = [0])
gdp_capita_data = pd.concat([gdp_capita_data,twn_df], axis = 0).reset_index(drop = True)

df_costs = pd.read_parquet("wiod_data/df_costs.parquet")
df_GDP_check = pd.read_parquet("wiod_data/df_GDP_check.parquet")
df_share_wc = pd.read_csv('wiod_data/ipums_share_wc.csv')

industries_matrix_15 = pd.read_excel('wiod_data/industries_matrix_15.xlsx', header = None, engine='openpyxl')
industries_matrix_14 = pd.read_excel('wiod_data/industries_matrix_14.xlsx', header = None, engine='openpyxl')


unique_industries = df_costs['Industry'].unique()
df_costs['Industry'] = pd.Categorical(df_costs['Industry'], categories=unique_industries, ordered=True)


with open('wiod_data/final_demand_dict.pkl', 'rb') as file:
    final_demand_dict = pickle.load(file)

with open('wiod_data/intermediate_trans.pkl', 'rb') as file:
    intermediate_dict = pickle.load(file)


#find all the countries that are in both ipums and wiod data
ipums_countries = df_share_wc['isocode'].unique()
wiod_countries = df_costs['Country'].unique()
wiod_countries = pd.DataFrame(wiod_countries).rename(columns = {0: 'Country'})
ipums_countries = pd.DataFrame(ipums_countries).rename(columns = {0: 'Country'})

wiod_countries = pd.merge(wiod_countries,gdp_capita_data, on = 'Country')
gdp_capita_data_wiod = wiod_countries.copy(deep = True)
ipums_countries = pd.merge(ipums_countries,gdp_capita_data, on = 'Country')

ipums_countries = ipums_countries[~ipums_countries['Country'].isin(['KHM', 'NLD','MLI'])]

# Find the intersection
def find_closest_gdp(country, gdp, comparison_df):
    comparison_df['Difference'] = np.abs(comparison_df['GDP_per_capita'] - gdp)
    closest_country = comparison_df.loc[comparison_df['Difference'].idxmin(), 'Country']
    return closest_country

# Applying the function to each row in df1
wiod_countries['Closest Country by GDP'] = wiod_countries.apply(lambda row: find_closest_gdp(row['Country'], row['GDP_per_capita'], ipums_countries.copy()), axis=1)

country_list = set(df_costs['Country'].unique())
country_list = sorted(country_list)

C = len(country_list)

final_demand_shares_dict = {}
domar_shares_dict = {}
intermediate_shares_dict = {}

for country in country_list: 

    final_demand_df = final_demand_dict[country]
    intermediate_df = intermediate_dict[country]

    wiod_country = wiod_countries[wiod_countries['Country'] == country]
    
    df_share_wc_country = df_share_wc[df_share_wc['isocode'] == wiod_country.iloc[0,2]]
    
    #Add wc/bc final expenditure to make final demand have same rows as the labor augmented intermediate matrix
    df_white_collar_final_demand = pd.DataFrame({
        'Value' : 0,
        'Origin Industry' : ['White Collar Labor']
        })
    df_blue_collar_final_demand = pd.DataFrame({
        'Value' : 0,
        'Origin Industry' : ['Blue Collar Labor']
        })

    final_demand_df = pd.concat([final_demand_df, df_white_collar_final_demand], axis = 0).reset_index(drop = True)

    final_demand_labor_df = pd.concat([final_demand_df, df_blue_collar_final_demand], axis = 0).reset_index(drop = True)

    if len(df_share_wc_country.index) == 15:
        industries_matrix = industries_matrix_15       
    else:
        industries_matrix = industries_matrix_14


    # Convert to numeric, coercing when necessary
    industries_matrix = industries_matrix.apply(pd.to_numeric, errors='coerce')

    industries_matrix = industries_matrix.to_numpy()[1:, 1:] 


    #Get the share of labor that is wc/bc by wiod industry 
    df_share_wc_country = df_share_wc_country['share_wc'].to_numpy()

    wiod_share_wc = np.matmul(industries_matrix , df_share_wc_country)

    wiod_share_bc =  1 - wiod_share_wc

    #find the total labor expenditures then multiply by the share of that industry that is wc/bc
    df_costs_country = df_costs[df_costs['Country'] == country]

    df_labor_exp = df_costs_country[df_costs_country['Cost Category'] == labor_exp]

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
        
    intermediate_labor_df = pd.concat([intermediate_df, labor_exp_pivot_wc], axis = 1)
    intermediate_labor_df = pd.concat([intermediate_labor_df, labor_exp_pivot_bc], axis = 1)

    labor_exp_destination_wc = labor_exp_pivot_wc.copy(deep = True)
    labor_exp_destination_bc = labor_exp_pivot_bc.copy(deep = True)
    labor_exp_destination_wc['White Collar Labor'] = labor_exp_destination_wc['White Collar Labor'] * 0
    labor_exp_destination_bc['Blue Collar Labor'] = labor_exp_destination_bc['Blue Collar Labor'] * 0

    intermediate_labor_df = pd.concat([intermediate_labor_df, labor_exp_destination_wc.T], axis = 0)

    intermediate_labor_df = pd.concat([intermediate_labor_df, labor_exp_destination_bc.T], axis = 0)

    intermediate_labor_df.iloc[56,57] = 0
    intermediate_labor_df.iloc[57,56] = 0
    intermediate_labor_df.iloc[57,57] = 0
    intermediate_labor_df.iloc[58,57] = 0
    intermediate_labor_df.iloc[57,58] = 0
    intermediate_labor_df.iloc[58,58] = 0
    intermediate_labor_df.iloc[58,56] = 0
    intermediate_labor_df.iloc[56,58] = 0


    destination_industries = intermediate_labor_df.index.tolist()
    origin_industries = intermediate_labor_df.columns.tolist()

    # Prepare a list to collect data
    data = []

    # Iterate through the DataFrame, skipping the first two rows and columns
    for j, origin_industry in enumerate(origin_industries):
        for i, destination_industry in enumerate(destination_industries):
            value = intermediate_labor_df.iat[i, j]
            if pd.notnull(value):  # Only add rows where value is not NaN and we dont want ROW data since there is no capital or labor data available 
                data.append({               
                    'Origin Industry': origin_industry,                
                    'Industry': destination_industry,
                    'Value': value 
                })
    intermediate_inputs_df = pd.DataFrame(data)

    #fix industrys
    unique_industries = intermediate_inputs_df['Industry'].unique()
    final_demand_labor_df['Origin Industry'] = pd.Categorical(final_demand_labor_df['Origin Industry'], categories=unique_industries, ordered=True)
    intermediate_inputs_df['Origin Industry'] = pd.Categorical(intermediate_inputs_df['Origin Industry'], categories=unique_industries, ordered=True)
    intermediate_inputs_df['Industry'] = pd.Categorical(intermediate_inputs_df['Industry'], categories=unique_industries, ordered=True)

    #find GO
    df_GO = final_demand_labor_df.copy(deep = True).drop('Value', axis = 1)
    df_GO = df_GO.rename(columns = {'Origin Industry': 'Industry'})
    final_demand_GO =  final_demand_labor_df.groupby(['Origin Industry'], observed = True)['Value'].sum().reset_index() 
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
    GDP = df_GDP_check[df_GDP_check['Country'] == country]
    GDP = float(GDP.iloc[0,1])

    final_demand_shares = final_demand_labor_df.copy(deep = True)
    final_demand_shares['Value'] = final_demand_labor_df['Value'] / GDP

    final_demand_shares_numeric = final_demand_shares.select_dtypes(include=[np.number])
    final_demand_vector = final_demand_shares_numeric.values


    #get leontief
    I = np.identity(intermediate_shares_matrix.shape[0])    #initialize identity matrix

    leontief_inverse = np.linalg.inv(I - intermediate_shares_matrix)     #Take the leontieff inverse

    ultimate_content = np.matmul(final_demand_vector.T, leontief_inverse)

    ultimate_content_df = pd.DataFrame(ultimate_content.T)

    ultimate_content_labeled = final_demand_shares.copy(deep = True).drop('Value', axis = 1)
    ultimate_content_labeled['Ultimate content'] = ultimate_content_df

    domar_shares_dict[country] = ultimate_content_labeled
    final_demand_shares_dict[country] = final_demand_shares
    intermediate_shares_dict[country] = intermediate_shares_of_GO

    # with pd.ExcelWriter(f'wiod by country/total labor shares by country/total labor shares {country}.xlsx') as writer:
  #    intermediate_shares.to_excel(writer, sheet_name='Intermediate Shares',index = True)
  #    final_demand_shares.to_excel(writer, sheet_name='Final Demand Shares',index = False)
  #    ultimate_content_labeled.to_excel(writer, sheet_name='Domar Weights', index = False)

first = True
for country in country_list:
    temp_final_demand_shares = final_demand_shares_dict[country].copy(deep = True)
    temp_intermediate_shares = intermediate_shares_dict[country].copy(deep = True)
    
    if first:
        avg_final_demand_shares = temp_final_demand_shares
        avg_intermediate_shares = temp_intermediate_shares
        first = False
    else:
        avg_final_demand_shares['Value'] = avg_final_demand_shares['Value'] + temp_final_demand_shares['Value']
        avg_intermediate_shares['IO shares'] = avg_intermediate_shares['IO shares'] + temp_intermediate_shares['IO shares']

avg_final_demand_shares['Value'] = avg_final_demand_shares['Value'] / C
avg_intermediate_shares['IO shares'] = avg_intermediate_shares['IO shares'] / C 


 #pivot to be n by n df
avg_intermediate_shares_df = avg_intermediate_shares.pivot(columns='Origin Industry', index='Industry', values='IO shares')

avg_final_demand_shares_numeric = avg_final_demand_shares.select_dtypes(include=[np.number])
avg_final_demand_vector = avg_final_demand_shares_numeric.values

avg_intermediate_shares_numeric = avg_intermediate_shares_df.select_dtypes(include=[np.number])
avg_intermediate_shares_matrix = avg_intermediate_shares_numeric.values

#non ag now
avg_intermediate_shares_non_ag = avg_intermediate_shares[~avg_intermediate_shares['Industry'].isin(public_and_ag)]

avg_intermediate_shares_non_ag_wc = avg_intermediate_shares_non_ag[avg_intermediate_shares_non_ag['Origin Industry'] == 'White Collar Labor']
avg_intermediate_shares_non_ag_bc = avg_intermediate_shares_non_ag[avg_intermediate_shares_non_ag['Origin Industry'] == 'Blue Collar Labor']

avg_intermediate_shares_non_ag_wc = avg_intermediate_shares_non_ag_wc.pivot(columns='Origin Industry', index='Industry', values='IO shares')
avg_intermediate_shares_non_ag_bc = avg_intermediate_shares_non_ag_bc.pivot(columns='Origin Industry', index='Industry', values='IO shares')

avg_intermediate_shares_numeric = avg_intermediate_shares_non_ag_wc.select_dtypes(include=[np.number])
fixed_intermediate_shares_non_ag_wc = avg_intermediate_shares_numeric.values
avg_intermediate_shares_numeric = avg_intermediate_shares_non_ag_bc.select_dtypes(include=[np.number])
fixed_intermediate_shares_non_ag_bc = avg_intermediate_shares_numeric.values




first = True
first_non_ag = True
for country in country_list:

    final_demand_shares_df = final_demand_shares_dict[country].copy(deep = True)
    intermediate_shares = intermediate_shares_dict[country].copy(deep = True)

    intermediate_shares_df = intermediate_shares.pivot(columns='Origin Industry', index='Industry', values='IO shares')

    intermediate_shares_numeric = intermediate_shares_df.select_dtypes(include=[np.number])
    intermediate_shares_matrix = intermediate_shares_numeric.values
 
    final_demand_shares_numeric = final_demand_shares_df.select_dtypes(include=[np.number])
    final_demand_vector = final_demand_shares_numeric.values
 
    I = np.identity(intermediate_shares_matrix.shape[0])    

    leontief_inverse = np.linalg.inv(I - intermediate_shares_matrix)    

    fixed_demand_ultimate_content = np.matmul(avg_final_demand_vector.T, leontief_inverse)

    fixed_leontief_inverse = np.linalg.inv(I - avg_intermediate_shares_matrix)

    fixed_intermediate_ultimate_content = np.matmul(final_demand_vector.T, fixed_leontief_inverse)

    actual_ultimate_content = np.matmul(final_demand_vector.T, leontief_inverse)
    
    actual_wc_domar = actual_ultimate_content[0,57]
    actual_bc_domar = actual_ultimate_content[0,58]
    
    actual_wc_share = actual_wc_domar / (actual_wc_domar + actual_bc_domar)


    fixed_demand_wc_domar = fixed_demand_ultimate_content[0,57]
    fixed_demand_bc_domar = fixed_demand_ultimate_content[0,58]
    
    fixed_demand_wc_share = fixed_demand_wc_domar / (fixed_demand_bc_domar + fixed_demand_wc_domar)
 
    fixed_intermediate_wc_domar = fixed_intermediate_ultimate_content[0,57]
    fixed_intermediate_bc_domar = fixed_intermediate_ultimate_content[0,58]
    
    fixed_intermediate_wc_share = fixed_intermediate_wc_domar / (fixed_intermediate_bc_domar + fixed_intermediate_wc_domar)

    if first:
        counterfactual_wc_shares_df = pd.DataFrame({
            'Country' : country,
            'Actual WC Share' : actual_wc_share,
            'Fixed Demand WC Share' : fixed_demand_wc_share,
            'Fixed Intermediate WC Share' : fixed_intermediate_wc_share
             },index=[0])
        first = False
    else:
        temp_counterfactual_wc_shares_df = pd.DataFrame({
            'Country' : country,
            'Actual WC Share' : actual_wc_share,
            'Fixed Demand WC Share' : fixed_demand_wc_share,
            'Fixed Intermediate WC Share' : fixed_intermediate_wc_share
             },index=[0])
        counterfactual_wc_shares_df = pd.concat([counterfactual_wc_shares_df, temp_counterfactual_wc_shares_df], axis = 0).reset_index(drop = True)


    #now do private non-ag
    intermediate_shares_non_ag = intermediate_shares[~intermediate_shares['Industry'].isin(public_and_ag)]
    intermediate_shares_non_ag_wc = intermediate_shares_non_ag[intermediate_shares_non_ag['Origin Industry'] == 'White Collar Labor']
    intermediate_shares_non_ag_bc = intermediate_shares_non_ag[intermediate_shares_non_ag['Origin Industry'] == 'Blue Collar Labor']

    intermediate_shares_non_ag_wc_df = intermediate_shares_non_ag_wc.pivot(columns='Origin Industry', index='Industry', values='IO shares')
    intermediate_shares_non_ag_bc_df = intermediate_shares_non_ag_bc.pivot(columns='Origin Industry', index='Industry', values='IO shares')
    
    intermediate_shares_numeric = intermediate_shares_non_ag_wc_df.select_dtypes(include=[np.number])
    intermediate_shares_non_ag_wc = intermediate_shares_numeric.values
    intermediate_shares_numeric = intermediate_shares_non_ag_bc_df.select_dtypes(include=[np.number])
    intermediate_shares_non_ag_bc = intermediate_shares_numeric.values

    fixed_intermediate_ultimate_content_non_ag = final_demand_labor_df.copy(deep = True)
    fixed_demand_ultimate_content_non_ag = final_demand_labor_df.copy(deep = True)
    actual_ultimate_content_non_ag = final_demand_labor_df.copy(deep = True)

    fixed_intermediate_ultimate_content_non_ag['Value'] = fixed_intermediate_ultimate_content.T
    fixed_demand_ultimate_content_non_ag['Value'] = fixed_demand_ultimate_content.T
    actual_ultimate_content_non_ag['Value'] = actual_ultimate_content.T

    fixed_intermediate_ultimate_content_non_ag = fixed_intermediate_ultimate_content_non_ag[~fixed_intermediate_ultimate_content_non_ag['Origin Industry'].isin(public_and_ag)]
    fixed_demand_ultimate_content_non_ag = fixed_demand_ultimate_content_non_ag[~fixed_demand_ultimate_content_non_ag['Origin Industry'].isin(public_and_ag)]
    actual_ultimate_content_non_ag = actual_ultimate_content_non_ag[~actual_ultimate_content_non_ag['Origin Industry'].isin(public_and_ag)]
    
    domar_shares_numeric = fixed_intermediate_ultimate_content_non_ag.select_dtypes(include=[np.number])
    fixed_intermediate_ultimate_content_non_ag = domar_shares_numeric.values
    
    domar_shares_numeric = fixed_demand_ultimate_content_non_ag.select_dtypes(include=[np.number])
    fixed_demand_ultimate_content_non_ag = domar_shares_numeric.values
    
    domar_shares_numeric = actual_ultimate_content_non_ag.select_dtypes(include=[np.number])
    actual_ultimate_content_non_ag = domar_shares_numeric.values

    fixed_intermediate_wc_domar_non_ag = np.matmul(fixed_intermediate_ultimate_content_non_ag.T, fixed_intermediate_shares_non_ag_wc)
    fixed_intermediate_bc_domar_non_ag = np.matmul(fixed_intermediate_ultimate_content_non_ag.T, fixed_intermediate_shares_non_ag_bc)

    fixed_demand_wc_domar_non_ag = np.matmul(fixed_demand_ultimate_content_non_ag.T, intermediate_shares_non_ag_wc)
    fixed_demand_bc_domar_non_ag = np.matmul(fixed_demand_ultimate_content_non_ag.T, intermediate_shares_non_ag_bc)

    actual_wc_domar_non_ag = np.matmul(actual_ultimate_content_non_ag.T, intermediate_shares_non_ag_wc)
    actual_bc_domar_non_ag = np.matmul(actual_ultimate_content_non_ag.T, intermediate_shares_non_ag_bc)

    fixed_intermediate_wc_share_non_ag = fixed_intermediate_wc_domar_non_ag / (fixed_intermediate_bc_domar_non_ag + fixed_intermediate_wc_domar_non_ag)
    fixed_demand_wc_share_non_ag = fixed_demand_wc_domar_non_ag / (fixed_demand_bc_domar_non_ag + fixed_demand_wc_domar_non_ag)
    actual_wc_share_non_ag = actual_wc_domar_non_ag / (actual_bc_domar_non_ag + actual_wc_domar_non_ag)

    fixed_intermediate_wc_share_non_ag = fixed_intermediate_wc_share_non_ag[0,0]
    fixed_demand_wc_share_non_ag = fixed_demand_wc_share_non_ag[0,0]
    actual_wc_share_non_ag = actual_wc_share_non_ag[0,0]
    
    if first_non_ag:
        counterfactual_wc_shares_non_ag_df = pd.DataFrame({
            'Country' : country,
            'Actual WC Share' : actual_wc_share_non_ag,
            'Fixed Demand WC Share' : fixed_demand_wc_share_non_ag,
            'Fixed Intermediate WC Share' : fixed_intermediate_wc_share_non_ag
             },index=[0])
        first_non_ag = False
    else:
        temp_counterfactual_wc_shares_df = pd.DataFrame({
            'Country' : country,
            'Actual WC Share' : actual_wc_share_non_ag,
            'Fixed Demand WC Share' : fixed_demand_wc_share_non_ag,
            'Fixed Intermediate WC Share' : fixed_intermediate_wc_share_non_ag
             },index=[0])
        counterfactual_wc_shares_non_ag_df = pd.concat([counterfactual_wc_shares_non_ag_df, temp_counterfactual_wc_shares_df], axis = 0).reset_index(drop = True)



counterfactual_wc_shares_df = pd.merge(counterfactual_wc_shares_df, gdp_capita_data_wiod, on = 'Country')

counterfactual_wc_shares_non_ag_df = pd.merge(counterfactual_wc_shares_non_ag_df, gdp_capita_data_wiod, on = 'Country')

print(counterfactual_wc_shares_non_ag_df)

print(counterfactual_wc_shares_df)

counterfactual_wc_shares_df.to_excel('Counterfactual data/Counterfactual WC Shares.xlsx', sheet_name = 'Sheet 1', index = False)

counterfactual_wc_shares_non_ag_df.to_excel('Counterfactual data/Counterfactual WC Shares non ag private.xlsx', sheet_name = 'Sheet 1', index = False)



