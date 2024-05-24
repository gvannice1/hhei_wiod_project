import pandas as pd
import numpy as np
import pickle

DEFAULT = 0
COSTS_LABOR = 'Labour compensation at basic prices'
COSTS_CAPITAL = 'Capital compensation at basic prices'



#load in data frames using parquet (see other files for code construsting these df's)
df_intermediate = pd.read_parquet("wiod_data/df_intermediate.parquet")

df_costs        = pd.read_parquet("wiod_data/df_costs.parquet")

df_gross_output = pd.read_parquet("wiod_data/df_gross_output.parquet")

# Find the unique industries and sort them in the order they appear in 'Industry'
unique_industries = df_intermediate['Industry'].unique()

# Make sure that the order of the industries are preserved when we pivot the data
df_intermediate['Industry'] = pd.Categorical(df_intermediate['Industry'], categories=unique_industries, ordered=True)
df_intermediate['Origin Industry'] = pd.Categorical(df_intermediate['Origin Industry'], categories=unique_industries, ordered=True)


domestic_df = df_intermediate[df_intermediate['Origin Country'] == df_intermediate['Country']]   #we only want domestic trade 

domestic_df = domestic_df.drop('Origin Country', axis = 1)     #drop origin country column as it is now redundant

domestic_with_GO = pd.merge(domestic_df, df_gross_output)    #merge in gross output column

domestic_with_GO['Input Share'] = domestic_with_GO['Value'] / domestic_with_GO['Gross Output']   #find input as a share of gross ouput

domestic_with_GO['Input Share'].fillna(0, inplace=True)       #set Nan values to zero

input_shares_df = domestic_with_GO.drop('Value', axis = 1).drop('Gross Output', axis = 1)    #drop Value and GO as they wont be needed in the final matrix



countries = df_gross_output['Country'].unique()   #find unique countries

input_shares_matrixes = {} #create a dictionary for each countries matrix

for country in countries:

     temp_input_shares = input_shares_df[input_shares_df['Country'] == country]
     temp_input_shares = temp_input_shares.pivot(columns='Origin Industry', index='Industry', values='Input Share')
    
    # Reindex both rows and columns to match the order of unique_industries
     temp_input_shares = temp_input_shares.reindex(index=unique_industries, columns=unique_industries, fill_value=0)

    # Store the numpy array in the dictionary
     input_shares_matrixes[country] = temp_input_shares.to_numpy()



#Lets move on to imports 

imports_df = df_intermediate[df_intermediate['Origin Country'] != df_intermediate['Country']]

imports_df = imports_df.groupby(['Country', 'Industry'], observed=True)['Value'].sum().reset_index()

imports_df = imports_df.rename(columns={'Value': 'Imports'})


#Lets create a labor inputs df 

labor_df = df_costs[df_costs['Cost Category'] == COSTS_LABOR]

labor_df = labor_df.drop('Cost Category', axis = 1)

labor_df = labor_df.rename(columns={'Cost': 'Labor Input'})


#Lets create a capital inputs df

capital_df = df_costs[df_costs['Cost Category'] == COSTS_CAPITAL]

capital_df = capital_df.drop('Cost Category', axis = 1)

capital_df = capital_df.rename(columns={'Cost': 'Capital Input'})




#merge it into one 

direct_inputs_df = pd.merge(labor_df, capital_df)

direct_inputs_df = pd.merge(direct_inputs_df, imports_df)




#Now we create an empty direct shares matrix with the columns we want
direct_shares_df = pd.DataFrame(columns=['Country','Labor Share', 'Capital Share', 'Import Share'])

#Lets populate it
direct_shares_df['Country'] = df_gross_output['Country']

direct_shares_df['Labor Share']  = direct_inputs_df['Labor Input'] / df_gross_output['Gross Output']

direct_shares_df['Capital Share']  = direct_inputs_df['Capital Input'] / df_gross_output['Gross Output']

direct_shares_df['Import Share']  = direct_inputs_df['Imports'] / df_gross_output['Gross Output']

direct_shares_df.fillna(0, inplace=True)   #account for the potential division by zero's, fill them with 0

#intialize direct matrix dictionary
direct_matrixes = {}

#create a direct matrix for each country and store it in a dictionary by country name
for country in countries:

     direct_temp = direct_shares_df[direct_shares_df['Country'] == country]   

     direct_temp = direct_temp.drop('Country', axis = 1)

     direct_matrixes[country] = direct_temp.to_numpy()
  


#normalize so data sums to 1
for country in countries:
     row_sums = input_shares_matrixes[country].sum(axis=1) + direct_matrixes[country].sum(axis=1)  #sum over all columns for both matrixes
     row_sums[row_sums == 0] = 1  #set rows that sum to zero = 1, in order to not divide by zero

     #divide each value by what each row sums to, data should sum to one now
     sigma_normalized =  input_shares_matrixes[country]/ row_sums[:, np.newaxis]
     direct_normalized = direct_matrixes[country] / row_sums[:, np.newaxis]
      
     #add data back into dicionary, replacing old matrixes
     input_shares_matrixes[country] = sigma_normalized
     direct_matrixes[country] = direct_normalized







#intitialize dictionary for ultimate factor content matrixes
ultimate_factor_matrixes = {}

#take leontiff inverse for each country and multiply it by the direct matrix, this will be our ultimate factor content
for country in countries:

     I = np.identity(input_shares_matrixes[country].shape[0])    #initialize identity matrix

     leontief_inverse = np.linalg.inv(I - input_shares_matrixes[country])     #Take the leontieff inverse

     ultimate_factor_matrixes[country] = np.matmul(leontief_inverse, direct_matrixes[country])   #multiply it by the direct matrixes for the country and store in dictionary

     #save each to excel file
     df_ultimate_factor_content = pd.DataFrame(ultimate_factor_matrixes[country])
     df_ultimate_factor_content.columns = ['Labor Share', 'Capital Share', 'Import Share']
     df_ultimate_factor_content.to_excel(f'ultimate_factor_countent_{country}.xlsx', sheet_name = 'sheet 1', index = False)


#save so it can be used elsewhere
#with open('ultimate_factor_matrixes.pkl', 'wb') as file:
#    pickle.dump(ultimate_factor_matrixes, file)




#create excel sheets to see data
#direct_df = pd.DataFrame(direct_matrixes['POL'])
#direct_df.to_excel('direct_POL.xlsx', sheet_name='sheet 1', index=False)

#input_shares_POL = pd.DataFrame(input_shares_matrixes['POL'])
#input_shares_POL.to_excel('input_shares_POL.xlsx', sheet_name='sheet 1', index=False)

#ultimate_df = pd.DataFrame(ultimate_factor_matrixes['AUS'])
#ultimate_df.to_excel('ultimate_content_AUS.xlsx', sheet_name='sheet 1', index=False)


