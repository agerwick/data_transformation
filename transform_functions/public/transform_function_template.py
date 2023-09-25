import pandas as pd
from func.shared import verify_data, check_data_source_and_entry, split_data_and_metadata, structure_dataframe

def transform_function_template(input_data, input_section, output_section):
    # ensure consistency of input data
    existing_data, existing_metadata = split_data_and_metadata(verify_data(input_data))

    # check the validify of the input section of the transform file
    input_section = check_data_source_and_entry(existing_data, input_section)
    # if the above didn't fail (which would have exited the program), then the input section is valid, and these are safe to use:
    data_source = input_section['data_source']
    data_entry = input_section['data_entry']
    input_fields = input_section['fields']

    # get the specified csv file or sheet from the spreadsheet file
    existing_df = existing_data.get(data_source).get(data_entry)

    # make some new data
    new_data = pd.DataFrame()
    new_data['new_column'] = ['a', 'b', 'c']
    new_metadata = {}

    # merge with existing data
    metadata={} # will be removed later as metadata is now in a separate key in data

    # simple method, which merges new_data dataframe with existing_data:
    return_data = structure_dataframe(new_data, new_metadata, input_data)
    # this defaults to setting datasouce to the name of the transform function, and data_entry to 'data'

    # more complex method, where you can specify data_source and data_entry:
    # return_data = structure_dataframe(new_data, new_metadata, existing_data, data_source='temp_data', data_entry='1')

    return return_data, metadata
