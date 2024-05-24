import pandas as pd
import numpy as np
import pickle



df_final_use = pd.read_parquet("wiod_data/df_final_use_wROW.parquet")
df_intermediate = pd.read_parquet("wiod_data/df_intermediate_wROW.parquet")

#load in ultimate factor matrix for indirect imports
with open('wiod_data/ultimate_factor_matrixes.pkl', 'rb') as file:
    ultimate_factor_matrixes = pickle.load(file)

#We dont want ROW as an origin country because we dont have ultimate content data for it
df_final_use = df_final_use[df_final_use['Origin Country'] != 'ROW']
df_intermediate = df_intermediate[df_intermediate['Origin Country'] != 'ROW']
#save unique countries to loop through
countries = df_final_use['Origin Country'].unique()

#save unique industries and fix origin industry category
unique_industries = df_intermediate['Origin Industry'].unique()
df_intermediate['Origin Industry'] = pd.Categorical(df_intermediate['Origin Industry'], categories=unique_industries, ordered=True)
df_final_use['Origin Industry'] = pd.Categorical(df_final_use['Origin Industry'], categories=unique_industries, ordered=True)

#want only exports
df_exports_intermediate = df_intermediate[df_intermediate['Origin Country'] != df_intermediate['Country']]
df_exports_expenditures = df_final_use[df_final_use['Country'] != df_final_use['Origin Country']]


#sum over all dest countries and uses/dest industustries
df_exports_summed_intermediate = df_exports_intermediate.groupby(['Origin Country','Origin Industry'], observed=True)['Value'].sum().reset_index()
df_exports_summed_expenditures = df_exports_expenditures.groupby(['Origin Country','Origin Industry'], observed=True)['Value'].sum().reset_index()

df_exports_by_industry = df_exports_summed_intermediate
df_exports_by_industry['Value'] = df_exports_summed_intermediate['Value'] + df_exports_summed_expenditures['Value']


#create total exports dataframe
df_exports_total_intermediate = df_exports_intermediate.groupby(['Origin Country'], observed=True)['Value'].sum().reset_index()
df_exports_total_expenditures = df_exports_expenditures.groupby(['Origin Country'], observed=True)['Value'].sum().reset_index()

df_total_exports = df_exports_total_expenditures
df_total_exports['Value'] = df_exports_total_expenditures['Value'] + df_exports_total_intermediate['Value']
df_total_exports.rename(columns={'Value':'Total Exports'},inplace=True)

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


#create import content column
df_exports_by_industry = pd.merge(df_exports_by_industry,df_total_exports)

#fill with values 
df_exports_by_industry['Import Content'] = df_exports_by_industry['Value'] /  df_exports_by_industry['Total Exports'] * ultimate_factor_matrix['Import Share']

#drop data that is not import content
df_import_content_by_industry = df_exports_by_industry.drop('Value',axis = 1)
df_import_content_by_industry = df_import_content_by_industry.drop('Total Exports',axis = 1)

#sum over origin industries to get a value for each country
df_import_content_of_exports = df_exports_by_industry.groupby(['Origin Country'], observed = True)['Import Content'].sum().reset_index()

#df_import_content_of_exports.to_excel('import_content_of_exports.xlsx', sheet_name='sheet 1', index=False)




