import pandas as pd
import numpy as np

GROSS_CAPITAL_FORMATION = 'Gross fixed capital formation'

df_final_use = pd.read_parquet("wiod_data/df_final_use_wROW.parquet")
df_intermediate = pd.read_parquet("wiod_data/df_intermediate_wROW.parquet")


# Find the unique industries and sort them in the order they appear in 'Industry'
unique_industries = df_intermediate['Industry'].unique()

# Make sure that the order of the industries are preserved when we pivot the data
df_intermediate['Industry'] = pd.Categorical(df_intermediate['Industry'], categories=unique_industries, ordered=True)
df_intermediate['Origin Industry'] = pd.Categorical(df_intermediate['Origin Industry'], categories=unique_industries, ordered=True)


#Create GFCF data frame and sum over dest country since we will estimate that later
df_GFCF = df_final_use[df_final_use['Final Use'] == GROSS_CAPITAL_FORMATION]

df_GFCF = df_GFCF.groupby(['Origin Industry', 'Country'], observed=True)['Value'].sum().reset_index()

df_GFCF = df_GFCF.rename(columns={'Value' : 'Gross Fixed Capital Formation'})



#generate random bilateral shares (share of GFCF in orig country, industry used by dest country)
#would be to hard to calculate this week

bilateral_GFCF_shares = []


for _ in range(2464):
    random_numbers = np.random.random(55)
    breakpoints = np.sort(np.concatenate(([0, 1], random_numbers)))
    vector = np.diff(breakpoints)
    bilateral_GFCF_shares.append(vector)

bilateral_GFCF_shares = np.array(bilateral_GFCF_shares).reshape(-1, 1)
bilateral_GFCF_shares = pd.DataFrame(bilateral_GFCF_shares)


#Calculate GFCF from certain industries to 

df_bilateral_GFCF_shares = df_intermediate.groupby(['Country','Origin Industry','Industry'], observed=True)['Value'].sum().reset_index()

df_bilateral_GFCF_shares['Value'] = bilateral_GFCF_shares[0]

df_bilateral_GFCF = pd.merge(df_bilateral_GFCF_shares,df_GFCF)

df_bilateral_GFCF['Gross Fixed Capital Formation'] = df_bilateral_GFCF['Gross Fixed Capital Formation'] * df_bilateral_GFCF['Value']

df_bilateral_GFCF = df_bilateral_GFCF.drop('Value', axis = 1)






 
#find total exports from given sector to a given country
total_exports = df_intermediate.groupby(['Origin Country','Origin Industry'], observed=True)['Value'].sum().reset_index()

total_exports = total_exports.rename(columns={'Value' : 'Total Exports'})
#take trade from a given country and sector to a given country as a portion total trade from a given sector to country

import_shares = df_intermediate.groupby(['Country','Origin Country','Origin Industry'], observed=True)['Value'].sum().reset_index()

import_shares = pd.merge(import_shares,total_exports)

import_shares['Value'] = import_shares['Value'] / import_shares['Total Exports']

import_shares = import_shares.drop('Total Exports', axis = 1)



#multiply this coefficient by GFCF dataframe

final_bilateral_GFCF = pd.merge(import_shares, df_bilateral_GFCF)

final_bilateral_GFCF['Gross Fixed Capital Formation'] = final_bilateral_GFCF['Gross Fixed Capital Formation'] * final_bilateral_GFCF['Value']

final_bilateral_GFCF = final_bilateral_GFCF.drop('Value', axis = 1)







#take domestic US capital formation as an example 
final_bilateral_GFCF_US = final_bilateral_GFCF[final_bilateral_GFCF['Origin Country'] == 'USA']
final_bilateral_GFCF_US = final_bilateral_GFCF_US[final_bilateral_GFCF_US['Country'] == 'USA']

final_bilateral_GFCF_US.to_excel('final_bilateral_GFCF_US.xlsx', sheet_name='sheet 1', index=False)


final_bilateral_GFCF_US['Origin Industry'] = pd.Categorical(final_bilateral_GFCF_US['Origin Industry'], categories=unique_industries, ordered=True)
final_bilateral_GFCF_US = final_bilateral_GFCF_US.groupby(['Origin Industry'], observed=True)['Gross Fixed Capital Formation'].sum().reset_index()



final_bilateral_GFCF_US.to_excel('final_bilateral_GFCF_US_agg.xlsx', sheet_name='sheet 1', index=False)


