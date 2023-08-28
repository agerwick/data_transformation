import pandas as pd
import json
import argparse
import importlib
import sys
from tabulate import tabulate

def get_filenames(args, transform_file, xput, fail_if_not_defined_in_transform_file=True):
    # get in/output file names from either command line arguments or from the transform file (the nodes under output)
    filenames = []
    filenames_for_error_message = []
    filename_help = ''
    raise_exception = False
    try:
        transform_file[f'{xput}_files']
    except KeyError:
        transform_file[f'{xput}_files'] = []
        if fail_if_not_defined_in_transform_file:
            filename_help += \
            f"No nodes defined in the {xput}_files section of the transformation file.\n"\
            f"This section should contain a node for each {xput} file.\n"
            raise_exception = True
    
    # make sure that the number of filenames defined in the transform file matches or exceeds the number of filenames given on the command line
    if getattr(args, xput, False):
        number_of_files_from_args = len(getattr(args, xput).split(','))
        number_of_files_from_transform_file = len(transform_file[f'{xput}_files'])
        if number_of_files_from_transform_file < number_of_files_from_args:
            if fail_if_not_defined_in_transform_file:
                filename_help += \
                f"Number of {xput} files defined in the transform file ({number_of_files_from_transform_file}) is less than the number of {xput} files defined on the command line ({number_of_files_from_args}).\n"
                raise_exception = True
            else:
                # add empty nodes to the transform file to match the number of filenames given on the command line
                for i in range(number_of_files_from_args - number_of_files_from_transform_file):
                    transform_file[f'{xput}_files'].append({})

    for index, xput_node in enumerate(transform_file[f'{xput}_files'], start=0):
        try:
            filename_from_transform_file = xput_node['filename']
        except KeyError:
            filename_from_transform_file = None
        
        try:
            filename_from_command_line = getattr(args, xput, False).split(',')[index]
            if filename_from_command_line == '_':
                # replace placeholder with None - filename from transform file will be used instead
                filename_from_command_line = None
        except Exception:
            # IndexError     - no filename for this index given on command line
            # AttributeError - in/output_files not defined in args
            if index == 0 and getattr(args, xput, False): # 1st argument and one arg is given
                filename_from_command_line = getattr(args, xput)
            else:
                filename_from_command_line = None
        
        # assemble list of file names for error message
        filenames_for_error_message.append([index + 1, filename_from_command_line, filename_from_transform_file])
        
        if filename_from_transform_file or filename_from_command_line:
            filenames.append(filename_from_command_line or filename_from_transform_file)
        else:
            filename_help += f"File name not defined for {xput} #{index + 1}.\n"
            raise_exception = True

    filename_help += \
    f"\nThe {xput} filename(s) can be defined as a command line argument --{xput} (comma separated list if more than one) or in {xput}_files section in the transformation file '{args.transform}' (as a filename attribute under each {xput} node).\n" + \
    f"File names specified on the command line takes precedence over file names defined in the transformation file.\n" + \
    f"A single underscore can be used as a placeholder for a filename on the command line if the filename is defined in the transformation file and you don't want to override it with a command line argument.\n\n" + \
    f"The following {xput} files were specified:\n"

    tabulate(filenames_for_error_message, headers=["#", "Command line argument", "Transform file"], tablefmt="psql", showindex=False)

    if raise_exception:
        raise Exception(filename_help)

    if not filenames:
        raise Exception(f"No {xput} file names defined.\n{filename_help}")
    
    if not args.quiet:
        for index, filename_from_command_line, filename_from_transform_file in filenames_for_error_message:
            if filename_from_command_line:
                print(f"{xput.capitalize()} file #{index}: {filename_from_command_line} (from command line)")
            elif filename_from_transform_file:
                print(f"{xput.capitalize()} file #{index}: {filename_from_transform_file} (from transform file)")

    return filenames


def get_node_attributes(transform_file, node_name, attribute_name, default=None):
    try:
        nodes = transform_file[node_name]
    except KeyError:
        nodes = []
    
    attributes = []
    for node in nodes:
        try:
            attribute = node[attribute_name]
        except KeyError:
            attribute = default
        attributes.append(attribute)
    return attributes

def main():
    parser = argparse.ArgumentParser(description='Data Transformation')
    parser.add_argument('--input', help='Input CSV file(s), if not defined in transform file', required=False)
    parser.add_argument('--output', help='Output CSV file(s), if not defined in transform file', required=False)
    parser.add_argument('--transform', help='Transform file in JSON format', required=True)
    parser.add_argument('--quiet', '-q', help='Suppress output', action='store_true')
    args = parser.parse_args()

    with open(args.transform, 'r') as transform_file_wrapper:
        transform_file = json.load(transform_file_wrapper)

    # get input file names
    input_filenames = get_filenames(args, transform_file, 'input', fail_if_not_defined_in_transform_file=False)

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

    # get prefixes for input file fields (if any)
    field_prefixes = get_node_attributes(transform_file, 'input_files', 'field_prefix', default=None)
    field_suffixes = get_node_attributes(transform_file, 'input_files', 'field_suffix', default=None)
    rename_fields = get_node_attributes(transform_file, 'input_files', 'rename_fields', default=None)

    # read input
    input_fields_per_file = []
    input_data = pd.DataFrame()
    for index, input_filename in enumerate(input_filenames, start=0):
        tmp_data = pd.read_csv(input_filename)
        # add prefix to field names if defined in transform file
        if field_prefixes[index]:
            tmp_data = tmp_data.add_prefix(field_prefixes[index]+'_')
        if field_suffixes[index]:
            tmp_data = tmp_data.add_suffix('_'+field_suffixes[index])
        if rename_fields[index]:
            tmp_data = tmp_data.rename(columns=rename_fields[index])
        input_data = input_data.combine_first(tmp_data)
        input_fields_per_file.append(list(tmp_data.columns))

    # print field names of input data
    if not args.quiet:
        for index, input_fields in enumerate(input_fields_per_file, start=1):
            print(f"Input fields from file #{index}: {', '.join(input_fields)}")
        if len(input_fields_per_file) > 1:
            print(f"All input fields: {', '.join(input_data.columns)}")
        print()

    # transform data
    output_data = pd.DataFrame()
    try:
        transformations = transform_file['transformations']
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
        if not args.quiet:
            print("No transformations specified in transform file. Output data is the same as input data.")
            print()
    else:
        # print field names of output data
        if not args.quiet:
            print("Output fields produced by transform functions:")
            print(", ".join(output_data.columns))
            print()

        # Add index column to both DataFrames
        input_data['__index__'] = input_data.index
        output_data['__index__'] = output_data.index
        
        # merge output and input data, overwriting input fields with duplicate names in output (for example, if name from input is split into first_name and last_name in output, then combines after some transformation back into "name")
        output_data = output_data.combine_first(input_data)
        
        # drop the index column
        output_data = output_data.drop(columns=['__index__'])

    # get ouput file names
    output_filenames = get_filenames(args, transform_file, 'output', fail_if_not_defined_in_transform_file=True)

    # write output file(s)
    for index, output in enumerate(transform_file['output_files'], start=0):
        output_fields = output['fields']
        output_filename = output_filenames[index]
        if not args.quiet:
            print(f"Writing to output file #{index + 1} {output_fields}")
        output_data.to_csv(output_filename, columns=output_fields, index=False)

if __name__ == "__main__":
    main()

