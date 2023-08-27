import pandas as pd

def split_name(input_data, input_fields, output_fields):
    if len(input_fields) != 1:
        raise Exception(f"split_name() requires exactly one input field: name -- however, this was given: {input_fields}")
    if len(output_fields) != 2:
        raise Exception(f"split_name() requires exactly two output fields: first_name and last_name -- however, this was given: {output_fields}")
    output_data = pd.DataFrame()
    first_names = []
    last_names = []
    for index, row in input_data.iterrows():
        # if name is "first_name last_name"
        if ' ' in row[input_fields[0]]:
            first_name, last_name = row[input_fields[0]].split()
        # if name is "last_name, first_name"
        elif ',' in row[input_fields[0]]:
            last_name, first_name = row[input_fields[0]].split(',')
        # if name is just "first_name"
        else:
            first_name = row[input_fields[0]]
            last_name = ''
        first_names.append(first_name.strip())
        last_names.append(last_name.strip())
    output_data[output_fields[0]] = first_names
    output_data[output_fields[1]] = last_names
    return output_data

def combine_name_to_first_last(input_data, input_fields, output_fields):
    if len(input_fields) != 2:
        raise Exception(f"combine_name() requires exactly two input fields: first_name and last_name -- however, this was given: {input_fields}")
    if len(output_fields) != 1:
        raise Exception(f"combine_name() requires exactly one output field: name -- however, this was given: {output_fields}")
    output_data = pd.DataFrame()
    names = []
    for index, row in input_data.iterrows():
        full_name = f"{row[input_fields[0]]} {row[input_fields[1]]}"
        names.append(full_name)
    output_data[output_fields[0]] = names
    return output_data
