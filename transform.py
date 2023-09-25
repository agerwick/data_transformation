import pandas as pd
import json
import argparse
import importlib
import sys

from func.shared import get_filenames, print_data_summary, check_data_source_and_entry, split_data_and_metadata
from func.input import get_input_data

def main():
    parser = argparse.ArgumentParser(description='Data Transformation')
    parser.add_argument('--input', help='Input CSV file(s), if not defined in transform file', required=False)
    parser.add_argument('--output', help='Output CSV file(s), if not defined in transform file', required=False)
    parser.add_argument('--transform', help='Transform file in JSON format', required=True)
    parser.add_argument('--quiet', '-q', help='Suppress output', action='store_true')
    args = parser.parse_args()

    with open(args.transform, 'r') as transform_file_wrapper:
        transform_file = json.load(transform_file_wrapper)

    # import modules needed for transformations
    try:
        modules_and_functions = transform_file['import']
    except KeyError:
        modules_and_functions = []
    
    transform_functions = {}
    for module_and_function in modules_and_functions:
        module_name = module_and_function['module']
        function_names = module_and_function['functions']
        
        try:
            module = importlib.import_module(module_name)
            for function_name in function_names:
                function = getattr(module, function_name)
                transform_functions[function_name] = function
        except ImportError:
            print(f"Failed to import functions {function_names} in module {module_name}")
            print(f"Modules and functions to be imported are defined the import section of {args.transform}")
            print(f"The function names specified here refer to functions defined in {module_name.replace('.','/')}.py.")
            print(f"(the path starts in the same directory as transform.py)")
            sys.exit(1)

    # read input files
    data = get_input_data(
        getattr(args, "input", False), #comma separated string of input files from args
        transform_file.get('input'),
        quiet=args.quiet
    )

    # transform data
    # output_data = pd.DataFrame()
    transformations = transform_file.get('transformations')

    # # if input data is a dataframe, this means we're dealing with a single sheet or csv file, that could be combined with others (if multiple input files are specified)
    # if isinstance(input_data, pd.DataFrame):
    #     # add an index column to the input data so that it can be combined with the transformed data
    #     input_data['__index__'] = input_data.index
    # # copy input data to output data
    
    # assign input data to output data so that we can keep passing output_data to the next transformation function
    # output_data = input_data

    # go through each transformation defined in the transform file (json) and apply the result to the output data
    for index, transformation in enumerate(transformations, start=1):
        # Take input and output fields from transform file and use them as arguments to the transformation function
        # This makes it possible to define all field names for both input and output files in the transform file
        input_section = transformation.get('input') or []
        output_section = transformation.get('output') or []

        if transformation['function'] in transform_functions:
            transform_function = transform_functions[transformation['function']]
        else:
            available_functions = '\n'.join(transform_functions.keys())
            print(f"ERROR:\n"\
            f"Transform function '{transformation['function']}' is not defined.\n"\
            f"You can define it by adding it in the import section of '{args.transform}'.\n"\
            f"These functions are then used in the transformations section.\n"\
            f"Currently, the following functions have been defined:\n{available_functions}\n")
            sys.exit(1)

        # apply transform function to data to procude updated data
        print(f"Executing transform function #{index} ({transformation['function']}):")
        data, metadata = transform_function(data, input_section, output_section)

        # check for metadata returned by transform function
        if metadata:
            if not args.quiet:
                print(f"Metadata returned by transform function #{index} ({transformation['function']}): {metadata}")

        if not args.quiet:
            print(f"\nOutput fields produced by transform function #{index} ({transformation['function']}):")
            print_data_summary(data)

    if len(transformations) == 0:
        # if no transformations are specified, this script could still be used to extract data from a CSV file and write it to another CSV file, so just write a helpful note.
        if not args.quiet:
            print("No transformations specified in transform file. Output data is the same as input data.")
            print()
    else:
        if not args.quiet:
            print()

    # get ouput file names
    output_filenames = get_filenames(
        getattr(args, "output", False),
        transform_file.get('output'),
        'output', # this is used for error messages to make it clear what kind of files we are talking about
        fail_if_not_defined_in_transform_file=True,
        quiet=args.quiet
    )

    # if a dataframe is returned, the output data is the result of an old-style transformation.
    # the transformation function should have returned a dictionary with the output data and metadata
    if type(data) == pd.DataFrame:
        print("ERROR:\nThe transform function returned a dataframe. This means that it is an old-style transform function, and it needs to be updated to using the structure_dataframe() function.")
        sys.exit(1)
    elif type(data) != dict:
        print("ERROR:\nThe transform function did not return a dictionary. This could mean the transform function is faulty. It may need to be updated to using the structure_dataframe() function.")
        sys.exit(1)
    elif 'data' not in data or 'metadata' not in data:
        print("ERROR:\nThe transform function did not return a dictionary with 'data' and 'metadata' keys. This could mean the transform function is faulty. It may need to be updated to using the structure_dataframe() function.")
        sys.exit(1)

    # if there's no data, don't write anything
    if not data:
        print("ERROR:\nNo data in output -- nothing will be written to output file(s)")
        sys.exit(1)

    output_section = transform_file.get('output')
    # sample output section:
    # [
    #     {
    #         "filename": "data/sample/output/names_and_addresses_split.csv",
    #         "fields": ["name", "first_name", "last_name", "address", "address_street", "address_house_number", "address_suffix", "postal_code", "city"]
    #     }
    # write output file(s)
    for index, output_node in enumerate(output_section, start=0):
        output_data, output_metadata = split_data_and_metadata(data)
        output_node = check_data_source_and_entry(output_data, output_node, section=f"output")

        data_source = output_node['data_source']
        data_entry = output_node['data_entry']
        output_fields = output_node['fields']
        # data_source is the name of the data source in the output data
        # for an input file, this is "input1", "input2", etc. unless data_source_name is specified in the transform file
        # for a transformation, this is the name of the transform function

        # data_entry is the name of the data entry in the output data
        # for CSV files it is "csv" by default
        # for spreadsheet files it is the name of the sheet
        # for transformations it is whatever name the transformation function gives it, typically just "data"

        # if any of the output fields are "*", find all fields not specified in output_fields and add them to the output
        if '*' in output_fields:
            # make a list of fields to add to the output
            fields_to_add = []
            for field in output_data.get(data_source).get(data_entry).columns:
                if field not in output_fields:
                    fields_to_add.append(field)
            # replace '*' with fields_to_add, in the position where '*' was 
            index_of_asterisk = output_fields.index('*')
            output_fields = output_fields[:index_of_asterisk] + fields_to_add + output_fields[index_of_asterisk + 1:]
            # delete any subsequent '*' in the list
            while '*' in output_fields:
                index_of_asterisk = output_fields.index('*')
                output_fields = output_fields[:index_of_asterisk] + output_fields[index_of_asterisk + 1:]
                if not args.quiet:
                    print(f"Warning: multiple instances of '*' found in output fields for output file #{index + 1}. Only the first instance will be used.")
        output_filename = output_filenames[index]
        data_from_output_data_entry = output_data.get(data_source).get(data_entry)
        if not args.quiet:
            print(f"Writing '{data_source}'/'{data_entry}' to output file #{index + 1} -- {len(data_from_output_data_entry)} rows\n  Columns: {list(output_fields)}")
        data_from_output_data_entry.to_csv(output_filename, columns=output_fields, index=False)

    # Generate graphs, if any are defined in the transform file
    graphs = transform_file.get('graphs')
    if graphs:
        # get ouput file names
        graph_filenames = get_filenames(
            getattr(args, "graph", False),
            transform_file['graph'],
            'graph', # this is used for error messages to make it clear what kind of files we are talking about
            fail_if_not_defined_in_transform_file=True,
            update_transform_file=True, # if the graph filenames from command line args are not defined in the transform file, add them to the transform file - this makes it easier to pick up the filenames from within the generate_graphs() function.
            quiet=args.quiet
        )

        from func.graph import generate_graphs
        generate_graphs(data, graphs, quiet=args.quiet)

if __name__ == "__main__":
    main()

