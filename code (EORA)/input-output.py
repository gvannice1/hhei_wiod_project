import pandas as pd
import numpy as np



file_path = 'IO_All_2015/IO_AFG_2015_BasicPrice.txt'
# Read the file
IO_df = pd.read_csv(file_path, sep='\t')

#remove first row and header (redundant)
IO_df = IO_df.iloc[:, 1:]
IO_df.columns = range(IO_df.shape[1])

#initiate rows where intermediate values occur
start_row =    3
start_column = 3

intermediate_IO_df = IO_df.iloc[start_row:start_row + 26, start_column: start_column + 26]

intermediate_IO    = intermediate_IO_df.apply(pd.to_numeric, errors = 'coerce').to_numpy()



#isolate final demand
start_col_final = 29

final_IO_df = IO_df.iloc[start_row:start_row + 26, start_col_final:start_col_final + 6]

#sum across all rows of final demand

numerical_df = final_IO_df.apply(pd.to_numeric, errors='coerce')

final_vector  = numerical_df.sum(axis=1)

final_vector = final_vector.to_numpy()

#isolate and sum exports and imports, use X - M to calculate trade surplus
#imports first
start_import_row = 35

imports_df = IO_df.iloc[start_import_row: start_import_row + 190, start_column:start_column + 26 + 6] # add six for imports to final demand

imports_df = imports_df.apply(pd.to_numeric, errors='coerce')

imports_sum  = imports_df.sum().sum()

#now exports
start_export_column = 35

exports_df = IO_df.iloc[start_row: start_row + 26, start_export_column:start_export_column + 190]
print(exports_df)
exports_df = exports_df.apply(pd.to_numeric, errors='coerce')

exports_sum = exports_df.sum().sum()

#calculate net exports
net_exports =  exports_sum - imports_sum

#add to final demand vecor
final_vector = np.append(final_vector,net_exports)



#create import and export vecors by summing across origins, append to intermediate vector
imports_df_no_final = IO_df.iloc[start_import_row: start_import_row + 190, start_column:start_column + 26]
imports_df_no_final = imports_df_no_final.apply(pd.to_numeric, errors='coerce')

imports_vector = imports_df_no_final.sum(axis = 0)

#convert to a row to append
imports_vector = pd.DataFrame([imports_vector.values], columns=intermediate_IO_df.columns)

#concat with intermediate
intermediate_imports_IO_df = pd.concat([intermediate_IO_df, imports_vector], axis = 0).reset_index(drop=True)

#already have exports df 
exports_vector = exports_df.sum(axis = 1)
#add zero 

#concat exports and make (27,27) equal to zero
exports_vector.index = range(exports_vector.shape[0])

input_output_df = pd.concat([intermediate_imports_IO_df, exports_vector.to_frame()], axis = 1).reset_index(drop=True)

input_output_df.iloc[-1,-1] = 0




#input_output_df.to_excel("input_output_EORA.xlsx", index=False, engine='openpyxl')


#final_demand_df = pd.DataFrame(final_vector)
#final_demand_df.to_excel("final_demand.xlsx", index=False, engine='openpyxl')

