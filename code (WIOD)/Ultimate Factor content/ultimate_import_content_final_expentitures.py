import pandas as pd
import numpy as np
import pickle

#take relevant final expenditure categories 
CONSUMPTION_EXPENDITURES = 'Final consumption expenditure by households'
GOVERNMENT_EXPENDITURES = 'Final consumption expenditure by government'
GROSS_CAPITAL_FORMATION = 'Gross fixed capital formation'

#create a list to loop through expenditure categories
final_expenditure_categories = [CONSUMPTION_EXPENDITURES, GOVERNMENT_EXPENDITURES, GROSS_CAPITAL_FORMATION]

#load in dataframe
df_final_use = pd.read_parquet("wiod_data/df_final_use.parquet")

#load in ultimate factor matrix for indirect imports
with open('wiod_data/ultimate_factor_matrixes.pkl', 'rb') as file:
    ultimate_factor_matrixes = pickle.load(file)

#save unique countries to loop through
countries = df_final_use['Country'].unique()

#save df of all countries and industries for creating new dataframes
df_countries_list = df_final_use.groupby(['Country', 'Origin Industry'], observed= True)['Value'].sum().reset_index()
df_countries_list = df_countries_list.drop('Value', axis = 1)

#filter out data not needed 
df_final_use = df_final_use[df_final_use['Final Use'].isin([GROSS_CAPITAL_FORMATION, 
                                                                      GOVERNMENT_EXPENDITURES, CONSUMPTION_EXPENDITURES])]
#df_final_use = df_final_use[df_final_use['Origin Industry'] != 'Construction']


#find total use for each country, category
df_total_expenditures = df_final_use.groupby(['Country','Final Use'], observed=True)['Value'].sum().reset_index()
df_total_expenditures = df_total_expenditures.rename(columns={'Value': 'Total Expenditures'})


#Direct import shares
#take all datapoints refering to imports
df_direct_imports = df_final_use[df_final_use['Country'] != df_final_use['Origin Country']]


#sum over origin industries/countries
df_direct_imports = df_direct_imports.groupby(['Country','Final Use'], observed=True)['Value'].sum().reset_index()
df_direct_imports = df_direct_imports.rename(columns={'Value': 'Total Direct Imports'})

#merge so that we have imports and total expenditures in the same df
df_direct_imports = pd.merge(df_direct_imports, df_total_expenditures)

#take imports as a share of total expenditure for each column
df_direct_imports['Direct Import Share'] = df_direct_imports['Total Direct Imports'] / df_direct_imports['Total Expenditures']



#create new data fram to store all the import shares as their own columns
df_direct_import_shares = pd.DataFrame(columns=['Country', CONSUMPTION_EXPENDITURES, GOVERNMENT_EXPENDITURES, GROSS_CAPITAL_FORMATION])

df_direct_import_shares['Country'] = df_final_use['Country'].unique()


for use in final_expenditure_categories:  

   df_direct_import_shares[use] = df_direct_imports[df_direct_imports['Final Use'] == use]['Direct Import Share'].reset_index(drop=True)







#Find the Indirect import share

#find domestic expenditures and divide by total expeditures
df_final_use_dom = df_final_use[df_final_use['Country'] == df_final_use['Origin Country']]

df_final_use_dom = df_final_use_dom.drop('Origin Country', axis = 1)

df_final_use_dom = pd.merge(df_final_use_dom, df_total_expenditures)

df_final_use_dom['Import Share'] = df_final_use_dom['Value'] / df_final_use_dom['Total Expenditures']





#Now stack all of the matrixes on top of eachother so that they can be multiplied by domestic expenditure shares
ultimate_matrix = []

for country in countries:
     if len(ultimate_matrix) > 0:
        # Append the current matrix to the bottom of the result_matrix
        ultimate_matrix = np.vstack((ultimate_matrix, ultimate_factor_matrixes[country]))
     else:
        # For the first matrix, just set it as the result_matrix
        ultimate_matrix = ultimate_factor_matrixes[country]

#convert matrix to dataframe so we can use multiply it by our other dataframes
ultimate_factor_matrix = pd.DataFrame(ultimate_matrix)

#name the coulumns of the matrix so we can choose to multiply by 'import share' later
ultimate_column_names = ['Labor Share', 'Capital Share', 'Import Share']
ultimate_factor_matrix.columns = ultimate_column_names



df_indirect_import_shares = pd.DataFrame(columns=['Country',CONSUMPTION_EXPENDITURES, GOVERNMENT_EXPENDITURES, GROSS_CAPITAL_FORMATION])

#fill columns with data
df_indirect_import_shares['Country'] = df_countries_list['Country']

#mutiply each expenditure share by its repspected ultimiate import share
for use in final_expenditure_categories:

   df_indirect_import_shares[use] = df_final_use_dom[df_final_use_dom['Final Use'] == use]['Import Share'].reset_index(drop = True) * ultimate_factor_matrix['Import Share'] 

   
#sum over all industries for each country
df_indirect_import_shares = df_indirect_import_shares.groupby('Country').sum().reset_index()





#create dataframe for total import shares
df_final_expentiture_import_shares = pd.DataFrame(columns=['Country',CONSUMPTION_EXPENDITURES, GOVERNMENT_EXPENDITURES, GROSS_CAPITAL_FORMATION])
df_final_expentiture_import_shares['Country'] = df_final_use['Country'].unique()

#sum indirect and direct import shares 
for use in final_expenditure_categories:

   df_final_expentiture_import_shares[use] = df_indirect_import_shares[use] + df_direct_import_shares[use]


#df_final_expentiture_import_shares.to_excel('Import_content_of_final_expenditure_categories.xlsx', index = False )

