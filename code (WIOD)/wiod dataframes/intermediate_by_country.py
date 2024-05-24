import pandas as pd
import numpy as np
import pickle 


df_intermediate = pd.read_parquet("wiod_data/df_intermediate_wROW.parquet")
df_final_demand = pd.read_parquet("wiod_data/df_final_use_wROW.parquet")

countries = df_intermediate['Origin Country'].unique()

unique_industries = df_intermediate['Origin Industry'].unique()
df_intermediate['Origin Industry'] = pd.Categorical(df_intermediate['Origin Industry'], categories=unique_industries, ordered=True)
df_intermediate['Industry'] = pd.Categorical(df_intermediate['Industry'], categories=unique_industries, ordered=True)
df_final_demand['Origin Industry'] = pd.Categorical(df_final_demand['Origin Industry'], categories=unique_industries, ordered=True)


intermediate_dict = {}


for country in countries:
     
     intermediate_temp = df_intermediate[df_intermediate['Origin Country'] == country]

     intermediate_temp = intermediate_temp[intermediate_temp['Country'] == country]

     intermediate_temp = intermediate_temp.drop('Origin Country', axis = 1)

     intermediate_temp = intermediate_temp.drop('Country', axis = 1)

     intermediate_pivot = intermediate_temp.pivot_table(
         index   = 'Origin Industry',
         columns = 'Industry',
         values  = 'Value',
         aggfunc = 'sum'
         )
    
     intermediate_imports = df_intermediate[df_intermediate['Origin Country'] != country]
     intermediate_imports = intermediate_imports[intermediate_imports['Country'] == country]
     intermediate_imports_summed = intermediate_imports.groupby(['Industry'], observed=True)['Value'].sum().reset_index()

     final_imports = df_final_demand[df_final_demand['Country'] == country]
     final_imports = final_imports[final_imports['Origin Country'] != country]   
     final_imports_summed = final_imports.groupby(['Origin Industry'], observed=True)['Value'].sum().reset_index()

     total_imports_summed = intermediate_imports_summed
     total_imports_summed['Value'] = intermediate_imports_summed['Value'] + final_imports_summed['Value']


         
     intermediate_exports = df_intermediate[df_intermediate['Country'] != country]
     intermediate_exports = intermediate_exports[intermediate_exports['Origin Country'] == country]
     intermediate_exports_summed = intermediate_exports.groupby(['Origin Industry'], observed=True)['Value'].sum().reset_index()


     final_exports = df_final_demand[df_final_demand['Country'] != country]
     final_exports = final_exports[final_exports['Origin Country'] == country]   
     final_exports_summed = final_exports.groupby(['Origin Industry'], observed=True)['Value'].sum().reset_index()

     total_exports_summed = intermediate_exports_summed
     total_exports_summed['Value'] = intermediate_exports_summed['Value'] + final_exports_summed['Value']


     exports_pivot = total_exports_summed.pivot_table(
          index = 'Origin Industry',
          values = 'Value',
          aggfunc = 'sum'
     )

     imports_pivot = total_imports_summed.pivot_table(
          columns = 'Industry',
          values = 'Value',
          aggfunc = 'sum'
     )
     
     intermediate_with_imports = pd.concat([intermediate_pivot, imports_pivot], axis = 0)

     intermediate_final = pd.concat([intermediate_with_imports, exports_pivot], axis = 1)

     intermediate_final.iloc[-1,-1] = 0

     intermediate_final.rename(columns = {'Value' : 'Dollars'}, inplace=True)
     intermediate_final.rename(index = {'Value' : 'Dollars'},inplace=True)


     intermediate_dict[country] = intermediate_final.T


with open('intermediate_trans.pkl', "wb") as file:
     pickle.dump(intermediate_dict, file)

#for country in countries:

   #intermediate_dict[country].to_excel(f'{country}_intermediate.xlsx',  sheet_name='sheet 1', index = True)


