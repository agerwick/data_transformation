import argparse
import importlib
import sys

from func.shared import get_filenames, print_data_summary, check_data_source_and_entry, split_data_and_metadata, process_metadata, data_is_empty
from func.input import get_input_data

def main():
    parser = argparse.ArgumentParser(description='Data Transformation')
    parser.add_argument('--input', help='Input CSV or spreadsheet file(s)', required=False)
    args = parser.parse_args()
    input_filenames = [f.strip() for f in getattr(args, "input", False).split(',')] # list of input filenames
    input_filenames_dict = {f"input_{idx+1}": filename for idx, filename in enumerate(input_filenames)} # dict of "input_n": filename so we can easily look up the filename for a given input_n
    # read input files

    input_data = get_input_data(
        getattr(args, "input", False), #comma separated string of input files from args
        None, # transform_file.get('input'),
        quiet=False
    )

    # split data and metadata
    data, metadata = split_data_and_metadata(input_data)

    schemas = {}
    for data_source, data_entries in data.items():
        for data_entry, df in data_entries.items():
            cols = tuple(set(df.columns)) # set to sort and remove dupes, tuple to make hashable
            col_info = [data_source, data_entry, list(df.columns)]
            if cols not in schemas:
                schemas[cols] = [col_info] # list of lists
            else:
                schemas[cols].append(col_info)
                

    for idx, (schema, sources_and_entries) in enumerate(schemas.items(), start=1):
        data_source = sources_and_entries[0][0]
        data_entry = sources_and_entries[0][1]
        columns = sources_and_entries[0][2]
        number_of_data_entries = len(sources_and_entries)
        print(f"Schema {idx} ({data_source}/{data_entry}): {number_of_data_entries} data entries")
        for col in columns:
            print(f"  {col}")            
        print("  Data sources and entries:")
        # for data_source, data_entry in sources_and_entries:
        #     print(f"  {data_source} {data_entry}")
        for sec in sources_and_entries:
            data_source = sec[0]
            data_entry = sec[1]
            columns = sec[2]
            print(f"    {data_source}/{data_entry}")
        print()

    print("----------------------------------------------------------------------------")
    for idx, (schema, sources_and_entries) in enumerate(schemas.items(), start=1):
        print(f"Schema {idx}: {number_of_data_entries} sheets with the same schema")
        previous_data_source = None
        for sec in sources_and_entries:
            data_source = sec[0]
            data_entry = sec[1]
            columns = sec[2]
            filename = input_filenames_dict[data_source]
            if data_source != previous_data_source:
                print(f"  {data_source}: {filename}")
            print(f"    {data_entry}")
            previous_data_source = data_source
        print()

if __name__ == "__main__":
    main()

