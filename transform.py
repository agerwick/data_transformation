import pandas as pd
import json
import argparse
import importlib
import sys
from tabulate import tabulate

def main():
    parser = argparse.ArgumentParser(description='Data Transformation')
    parser.add_argument('--input', help='Input CSV file(s), if not defined in transform file', required=False)
    parser.add_argument('--output', help='Output CSV file(s), if not defined in transform file', required=False)
    parser.add_argument('--transform', help='Transformation file in JSON format', required=True)
    parser.add_argument('--quiet', '-q', help='Suppress output', action='store_true')
    args = parser.parse_args()

    with open(args.transform, 'r') as transform_file:
        transformation_file = json.load(transform_file)

    # define generic help text for input and output filename issues
    generic_filename_help = \
    f"\nThe Xput filename(s) can be defined as a command line argument --Xput (comma separated list if more than one) or in Xput_files section in the transformation file '{args.transform}' (as a filename attribute under each Xput node).\n" + \
    f"File names specified on the command line takes precedence over file names defined in the transformation file.\n" + \
    f"A single underscore can be used as a placeholder for a filename on the command line if the filename is defined in the transformation file and you don't want to override it with a command line argument.\n\n" + \
    f"The following Xput files were specified:\n"

    # get output file names from either command line arguments or from the transform file (the nodes under output)
    output_filenames = []
    output_filenames_for_error_message = []
    output_filename_help = ''
    raise_exception = False
    try:
        transformation_file['output_files']
    except KeyError:
        transformation_file['output_files'] = []
        output_filename_help += \
        "No nodes defined in the output_files section of the transformation file.\n"\
        "This section should contain a node for each output file.\n"
        raise_exception = True
    
    for index, output in enumerate(transformation_file['output_files'], start=0):
        try:
            output_filename_from_transform_file = output['filename']
        except KeyError:
            output_filename_from_transform_file = None
        
        try:
            output_filename_from_command_line = args.output.split(',')[index]
            if output_filename_from_command_line == '_':
                # replace placeholder with None - filename from transform file will be used instead
                output_filename_from_command_line = None
        except IndexError:
            if index == 0 and args.output: # 1st argument and one arg is given
                output_filename_from_command_line = args.output
            else:
                output_filename_from_command_line = None
        
        # assemple list of output file names for error message
        output_filenames_for_error_message.append([index + 1, output_filename_from_transform_file, output_filename_from_command_line])
        
        if output_filename_from_transform_file or output_filename_from_command_line:
            output_filenames.append(output_filename_from_command_line or output_filename_from_transform_file)
        else:
            output_filename_help += f"Output file name not defined for output #{index + 1}.\n"
            raise_exception = True

    output_filename_help += generic_filename_help.replace('Xput', "output")
    tabulate(output_filenames_for_error_message, headers=["#", "Command line argument", "Transformation file"], tablefmt="psql", showindex=False)

    if raise_exception:
        raise Exception(output_filename_help)

    if not output_filenames:
        raise Exception("No output file names defined.\n{output_filename_help}")

    # import modules needed for transformations
    try:
        modules_and_functions = transformation_file['import']
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

    # read input
    input_data = pd.read_csv(args.input)

    # transform data
    output_data = pd.DataFrame()
    try:
        transformations = transformation_file['transformations']
    except KeyError:
        transformations = []

    # go through each transformation defined in the transform file (json) and apply the result to the output data
    for transformation in transformations:
        # Take input and output fields from transformation file and use them as arguments to the transformation function
        # This makes it possible to define all field names for both input and output files in the transformation file
        input_fields = transformation['input']
        output_fields = transformation['output']

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
        transformed_data = transform_function(input_data, input_fields, output_fields)

        # add transformed data to output
        if output_data.empty:
            output_data = transformed_data
        else:
            output_data = output_data.combine_first(transformed_data)

    if output_data.empty:
        # if no transformations are specified, this script could still be used to extract data from a CSV file and write it to another CSV file
        output_data = input_data
    else:
        # Add index column to both DataFrames
        input_data['__index__'] = input_data.index
        output_data['__index__'] = output_data.index
        
        # merge output and input data, overwriting input fields with duplicate names in output (for example, if name from input is split into first_name and last_name in output, then combines after some transformation back into "name")
        output_data = output_data.combine_first(input_data)
        
        # drop the index column
        output_data = output_data.drop(columns=['__index__'])

    # write output file(s)
    for index, output in enumerate(transformation_file['output_files'], start=0):
        output_fields = output['fields']
        output_filename = output_filenames[index]
        if not args.quiet:
            print(f"Writing output to '{output_filename}' {output_fields}")
        output_data.to_csv(output_filename, columns=output_fields, index=False)

if __name__ == "__main__":
    main()

