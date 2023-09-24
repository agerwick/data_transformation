import pandas as pd
import json
import argparse
import importlib
import sys

from func.shared import get_filenames
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

    input_data = get_input_data(args, transform_file)

    # transform data
    output_data = pd.DataFrame()
    try:
        transformations = transform_file['transformations']
    except KeyError:
        transformations = []

    # if input data is a dataframe, this means we're dealing with a single sheet or csv file, that could be combined with others (if multiple input files are specified)
    if isinstance(input_data, pd.DataFrame):
        # add an index column to the input data so that it can be combined with the transformed data
        input_data['__index__'] = input_data.index
    # copy input data to output data
    output_data = input_data # maybe this should be output_data = input_data.copy() ? (to avoid modifying input_data) -- but do we really need the input data again?

    # go through each transformation defined in the transform file (json) and apply the result to the output data
    for index, transformation in enumerate(transformations, start=1):
        # Take input and output fields from transform file and use them as arguments to the transformation function
        # This makes it possible to define all field names for both input and output files in the transform file
        input_fields = transformation.get('input') or []
        output_fields = transformation.get('output') or []

        if transformation['function'] in transform_functions:
            transform_function = transform_functions[transformation['function']]
        else:
            available_functions = '\n'.join(transform_functions.keys())
            function_not_found_exception_message = \
            f"Transformation function '{transformation['function']}' is not defined.\n"\
            f"You can define it by adding it in the import section of '{args.transform}'.\n"\
            f"These functions are then used in the transformations section.\n"\
            f"Currently, the following functions have been defined:\n{available_functions}\n"
            raise Exception(function_not_found_exception_message)

        # apply transform function to input data to procude output data
        transformed_data, metadata = transform_function(output_data, input_fields, output_fields)

        # check for metadata returned by transform function
        if metadata:
            if not args.quiet:
                print(f"Metadata returned by transform function #{index} ({transformation['function']}): {metadata}")
            if metadata.get('clear_input_data'):
                # delete all existing data in the output table before writing the new data returned from the transform function
                output_data = pd.DataFrame()

        if not args.quiet:
            print(f"Output fields produced by transform function #{index} ({transformation['function']}): {list(transformed_data.columns)}")
            print(f"Number of rows in output data: {len(transformed_data)}")

        if isinstance(input_data, pd.DataFrame): # one sheet only (not a dictionary)
            if not transformed_data.empty:
                # add an index column to the transformed data so that it can be combined with the input and output data
                transformed_data['__index__'] = transformed_data.index

                # add transformed data to output data
                # output_data already has an index column, as it was added to input_data in the previous iteration, from which output_data was created
                # merging output and transformed data will overwrite fields with duplicate names in output (for example, if name from input is split into first_name and last_name in output, then combined after some transformation back into "name", the original input name will be overwritten)
                output_data = transformed_data.combine_first(output_data)

                # dropping the index column from the transformed data is not necessary, as it will never be used again
                # transformed_data = transformed_data.drop(columns=['__index__'])
        elif isinstance(input_data, dict): # multiple sheets (a dictionary)
            output_data = transformed_data # we can do this because loading multiple sheets is not supported

    # if input data is a dataframe, this means we're dealing with a single sheet or csv file, that could be combined with others (if multiple input files are specified)
    if isinstance(input_data, pd.DataFrame): # one sheet only (not a dictionary)
        # drop the index column from output data
        if '__index__' in output_data.columns:
            output_data = output_data.drop(columns=['__index__'])
        # dropping the index column from input data is not necessary, as it will never be used again
        # input_data = input_data.drop(columns=['__index__'])

    if len(transformations) == 0:
        # if no transformations are specified, this script could still be used to extract data from a CSV file and write it to another CSV file, so just write a helpful note.
        if not args.quiet:
            print("No transformations specified in transform file. Output data is the same as input data.")
            print()
    else:
        if not args.quiet:
            print()

    # get ouput file names
    output_filenames = get_filenames(args, transform_file, 'output', fail_if_not_defined_in_transform_file=True)

    # if there's no data in the output, don't write anything
    if output_data.empty:
        if not args.quiet:
            print("No data in output -- nothing will be written to output file(s)")
        sys.exit(0)

    # write output file(s)
    for index, output in enumerate(transform_file['output_files'], start=0):
        output_fields = output['fields']
        # if any of the output fields are "*", find all fields not specified in output_fields and add them to the output
        if '*' in output_fields:
            # make a list of fields to add to the output
            fields_to_add = []
            for field in output_data.columns:
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
        if not args.quiet:
            print(f"Writing to output file #{index + 1} {list(output_fields)}")
        output_data.to_csv(output_filename, columns=output_fields, index=False)

    # check for graphs to generate
    graphs = transform_file.get('graphs')
    if graphs:
        from func.graph import generate_graphs
        generate_graphs(graphs, output_data)

if __name__ == "__main__":
    main()

