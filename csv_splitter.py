import pandas as pd
import sys

def process_csv(input_file, only, exclude, split_on, selected_columns=None):
    # Load CSV into a dataframe
    df = pd.read_csv(input_file)
    
    # Filter records based on "only" dictionary
    for column, values in only.items():
        df = df[df[column].isin(values)]
    
    # Exclude records based on "exclude" dictionary
    for column, values in exclude.items():
        df = df[~df[column].isin(values)]
    
    # Generate output files based on "split_on" list
    grouped = df.groupby(split_on)
    
    # Export each group to a separate file
    for group, data in grouped:
        filename = f"{input_file.split('.')[0]}"
        for column, value in zip(split_on, group):
            filename += f"_{value}"
        filename += ".csv"
        
        if selected_columns:
            data = data[selected_columns]

        # add Time column before all other columns
        # if continuous_time_column:
        #     data = data[["Time"] + data.columns]

        # loop through all rows and calculate time
        # this is not complete, as we don't need it right now
        # tmp_time = []
        # time = 0
        # time_from_previous_row = data[continuous_time_column][0]
        # for index, row in data.iterrows():
        #     time_from_current_row = row[continuous_time_column]
        #     time_difference = time_from_current_row - time_from_previous_row
        #     if time_difference == 0:
        #         # reset time difference
        #         previous_time_difference = time_difference
        #     else:
        #         if previous_time_difference != time_difference:
        #             # gap between records changed
        #             print(f"Time difference changed from {previous_time_difference} to {time_difference} at row {index}")
        #         time_difference = previous_time_difference

        #     time += time_difference
        #     tmp_time.append(time)
        #     time_from_previous_row = time_from_current_row

        # fill time column with continuous time
        # if continuous_time_column:
        #     data["Time"] = tmp_time
        
        data.to_csv(filename, index=False)

# Example usage
input_file = sys.argv[1]  # CSV input file from command line argument
only = eval(sys.argv[2])  # Evaluate the "only" dictionary from input parameter
exclude = eval(sys.argv[3])  # Evaluate the "exclude" dictionary from input parameter
split_on = eval(sys.argv[4])  # Evaluate the "split_on" list from input parameter
selected_columns = eval(sys.argv[5])  # Evaluate the "selected_columns" list if provided
if selected_columns == "_" or selected_columns == "[]":
    selected_columns = None
# continuous_time_column = sys.argv[6]  # Evaluate the "continuous_time_column" string if provided
# if continuous_time_column == "_":
#     continuous_time_column = None

process_csv(input_file, only, exclude, split_on, selected_columns)

# example usage:
# python3 csv_splitter.py input.csv "{'column1': ['value1', 'value2'], 'column2': ['value3']}" "{'column1': ['value4']}" "['column3', 'column4']" "_" "_"
# use empty dictionary or list if you don't want to filter or split on a column
# example 2 (only save certain columns):
# python csv_splitter.py input.csv "{'column1': ['value1', 'value2'], 'column2': ['value3']}" "{'column1': ['value4']}" "['column3', 'column4']" "['column1', 'column3']" "_"
# example 3 (continuous time column):
# python csv_splitter.py input.csv "{'column1': ['value1', 'value2'], 'column2': ['value3']}" "{'column1': ['value4']}" "['column3', 'column4']" "['column1', 'column3']" "_" "column5"