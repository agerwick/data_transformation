import pandas as pd
from func.shared import structure_dataframe

def merge_by_column(data, input_fields, output_fields):

    # input_data is a dictionary with key: data_source ("input1", ...) and value: another dictionary with key: data_entry (sheet_name from spreadsheet or "csv" for CSV files) and value: a dataframe with data from that sheet or CSV file
    # example:
    # {
    #     "input_1": {
    #         "sheet1": <pandas dataframe>
    #         "sheet2": <pandas dataframe>
    #     }
    # }
    # if there's only one file, then the list will only contain one dictionary
    # if the file is a csv file, then the sheet_name will be "csv".
    # Here's an example where 2 csv files are loaded:
    # {
    #     "input_1": {
    #         "csv": <pandas dataframe>
    #     },
    #     "input_2": {
    #         "csv": <pandas dataframe>
    #     }
    # ]
    # The order in the numbering of data sources is the same as the order in which the files were specified in the transform file or command line argument.

    print("Merging data from all data sources and all data entries (sheets in spreadsheets)...")
    # loop through the input data, file by file and merge them
    merged_data = pd.DataFrame()
    #input_fields_per_file = []
    for data_source, data_entry_dict in data.items():
        # get the dataframe from the dictionary
        for data_entry, tmp_data in data_entry_dict.items():
            #input_fields_per_file.append(list(tmp_data.columns))
            print(f"Adding data from source '{data_source}' entry '{data_entry}' (cols: {len(tmp_data.columns)}, rows: {len(tmp_data)})")
            # merge the dataframe with the output data
            merged_data = merged_data.combine_first(tmp_data)

    metadata = {} 
    return structure_dataframe(merged_data, data), metadata
