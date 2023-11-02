import pandas as pd
import sys

def process_csv(input_file, only, exclude, split_on):
    # Load CSV into a dataframe
    df = pd.read_csv(input_file)
    
    # Filter records based on "only" dictionary
    for column, values in only.items():
        df = df[df[column].isin(values)]
    
    # Exclude records based on "exclude" dictionary
    for column, values in exclude.items():
        df = df[~df[column].isin(values)]
    
    # Generate output files based on "split_on" list
    # Group the dataframe by the split_on columns
    grouped = df.groupby(split_on)

    # Export each group to a separate file
    for group, data in grouped:
        filename = f"{input_file.split('.')[0]}"

        for column, value in zip(split_on, group):
            filename += f"_{value}"

        filename += ".csv"
        data.to_csv(filename, index=False)

    # for i, row in df.iterrows():
    #     filename = f"{input_file.split('.')[0]}"
    #     for column in split_on:
    #         filename += f"_{row[column]}"
    #     filename += ".csv"
    #     row.to_frame().transpose().to_csv(filename, index=False)

input_file = sys.argv[1]  # CSV input file from command line argument
only = eval(sys.argv[2])  # Evaluate the "only" dictionary from input parameter
exclude = eval(sys.argv[3])  # Evaluate the "exclude" dictionary from input parameter
split_on = eval(sys.argv[4])  # Evaluate the "split_on" list from input parameter

# example usage:
# python3 csv_splitter.py input.csv "{'column1': ['value1', 'value2'], 'column2': ['value3']}" "{'column1': ['value4']}" "['column3', 'column4']"
# use empty dictionary or list if you don't want to filter or split on a column

process_csv(input_file, only, exclude, split_on)