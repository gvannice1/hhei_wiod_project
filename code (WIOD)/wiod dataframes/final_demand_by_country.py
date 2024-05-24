import pandas as pd
import numpy as np 
import pickle


df_final_demand = pd.read_parquet("wiod_data/df_final_use_wROW.parquet")
df_intermediate = pd.read_parquet("wiod_data/df_intermediate_wROW.parquet")


countries = df_final_demand['Origin Country'].unique()
unique_industries = df_final_demand['Origin Industry'].unique()
df_final_demand['Origin Industry'] = pd.Categorical(df_final_demand['Origin Industry'], categories=unique_industries, ordered=True)


final_demand = df_final_demand.groupby(['Country','Origin Industry'], observed= True)['Value'].sum().reset_index()


final_demand_dict ={}

for country in countries:

    #create FD from non-dollar industries
    final_demand_vector = final_demand[final_demand['Country'] == country]
    final_demand_vector =final_demand_vector.drop('Country', axis = 1).reset_index().drop('index', axis = 1)

    #find total exports
    intermediate_exports = df_intermediate[df_intermediate['Origin Country'] == country]
    intermediate_exports = intermediate_exports[intermediate_exports['Country'] != country]

    intermediate_exports = intermediate_exports['Value'].sum()

    final_exports = df_final_demand[df_final_demand['Origin Country'] == country]
    final_exports = final_exports[final_exports['Country'] != country]

    final_exports = final_exports['Value'].sum()

    #find total imports
    intermediate_imports = df_intermediate[df_intermediate['Country'] == country]
    intermediate_imports = intermediate_imports[intermediate_imports['Origin Country'] != country]

    intermediate_imports = intermediate_imports['Value'].sum()

    final_imports = df_final_demand[df_final_demand['Country'] == country]
    final_imports = final_imports[final_imports['Origin Country'] != country]

    final_imports = final_imports['Value'].sum()

    #find net exports
    total_exports = final_exports + intermediate_exports

    total_imports = final_imports + intermediate_imports

    net_exports = total_exports - total_imports

    #put NE into df format to concate FD vector
    df_net_exports = pd.DataFrame({
        'Value' : [net_exports],
        'Origin Industry' : ['Dollars']
    })

    final_demand_dict[country] = pd.concat([final_demand_vector, df_net_exports], axis = 0).reset_index(drop = True)



with open('final_demand_dict.pkl', 'wb') as file:
    pickle.dump(final_demand_dict, file)

#for country in countries:

    #final_demand_dict[country].to_excel(f'{country}_final_demand.xlsx', sheet_name = 'sheet 1', index = False)





    