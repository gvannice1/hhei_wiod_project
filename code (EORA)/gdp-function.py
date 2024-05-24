import pandas as pd
import numpy as np
import os

#initiate rows where intermediate values occur
start_row =    3
start_column = 3

start_VA = 29

financial_int = 20


fin_share_of_GDP_dict = {}


file_path1 = 'wikipedia-iso-country-codes.csv'

country_codes = pd.read_csv(file_path1, delimiter=',')

country_codes = country_codes['Alpha-3 code']


for country in country_codes:


    file_path = f'IO_All_2015/IO_{country}_2015_BasicPrice.txt'

    #check file exists
    if os.path.exists(file_path):
        # File exists, so you can process it
        # Your file processing logic here

        # Read the file
        IO_df = pd.read_csv(file_path, sep='\t', low_memory=False)

        #remove first row and header (redundant)
        IO_df = IO_df.iloc[:, 1:]
        IO_df.columns = range(IO_df.shape[1])


        #find total output
        final_and_IO_df = IO_df.iloc[start_row:start_row + 26, start_column:start_column + 26 + 6 + 190]

        final_and_IO = final_and_IO_df.apply(pd.to_numeric, errors='coerce').to_numpy()


        #find VA to calculate gdp
        VA_df = IO_df.iloc[start_VA:start_VA + 6, start_column: start_column + 26]

        VA_df = VA_df.apply(pd.to_numeric, errors = 'coerce').to_numpy()

        GDP = VA_df.sum().sum()


        #sum across all rows of final demand and intermediate
        gross_output  = final_and_IO.sum(axis=1)

        financial_GO = gross_output[financial_int]

        fin_share_value = financial_GO / GDP

        if 0 <= fin_share_value <= 1 and not np.isnan(fin_share_value):
        # Add the country-value pair to the dictionary
            fin_share_of_GDP_dict[country] = fin_share_value


    else:
        print('File doesnt exist')


result_df = pd.DataFrame(list(fin_share_of_GDP_dict.items()), columns=['CountryCode', 'FinancialShareOfGDP'])


result_df.to_excel("Share_of_GDP_EORA.xlsx", index=False, engine='openpyxl')