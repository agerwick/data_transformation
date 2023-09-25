import pandas as pd
from func.shared import check_data_source_and_entry, verify_data, split_data_and_metadata, structure_dataframe

def extract_single_sheet(input_data, input_section, output_section):
    # ensure consistency of input data
    existing_data, existing_metadata = split_data_and_metadata(verify_data(input_data))
    # input_data is only used here and at the end, when merging with new_data.
    # all other operations are done on existing_data, which keys are data_source names

    # check the validify of the input section of the transform file
    input_section = check_data_source_and_entry(existing_data, input_section)
    # if the above didn't fail (which would have exited the program), then the input section is valid, and these are safe to use:
    data_source = input_section['data_source']
    data_entry = input_section['data_entry']
    input_fields = input_section['fields']

    # get the specified sheet from the spreadsheet file
    new_data = existing_data.get(data_source).get(data_entry)

    # check if columns are specified in input fields
    if input_fields:
        # check if specified columns exist in the sheet
        missing_columns = [col for col in input_fields if col not in new_data.columns]
        if len(missing_columns) > 0:
            print(f"The following columns were specified in the transformations section of the transform file:")
            print(f"  {input_fields}")
            print(f"But the following columns do not exist in the sheet {data_source}:")
            print(f"  {missing_columns}")
            print("The sheet has the following columns:")
            print(f"  {list(new_data.columns)}")
            print("Nothing will be returned.")
            new_data = pd.DataFrame() # return nothing
        else:
            # add the selected columns to the output data
            new_data = new_data[input_fields]
    else:
        # return all columns from the sheet
        pass

    metadata = {} # temporary, until metadata is in a separate key in data
    new_metadata = {}
    return structure_dataframe(new_data, new_metadata, {}), metadata
    # by replacing input_data with {}, we are returning only the output data.
