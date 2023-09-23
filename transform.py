import pandas as pd
import json
import argparse
import importlib
import sys
from tabulate import tabulate

def get_filenames(args, transform_file, xput, fail_if_not_defined_in_transform_file=True):
    """
    Get List of Input/Output File Names from Command Line or Transform File.

    This function retrieves a list of input or output file names based on the specified
    command line argument or from the corresponding section in the transform file.
    The function ensures that the number of filenames defined in the transform file matches
    or exceeds the number of filenames given on the command line. If not, it adds empty nodes
    to the transform file to match the number of filenames provided on the command line.

    Args:
        args (Namespace): The parsed command line arguments from argparse.
        transform_file (dict): The loaded transform configuration from the JSON file.
        xput (str): The type of file (e.g., 'input' or 'output').
        fail_if_not_defined_in_transform_file (bool, optional): Determines whether to raise an
          exception if the specified node is not defined in the transform file.
          Defaults to True. For output files, this should be set to True, otherwise we won't
          know what fields to write in the output file.

        Note: The command line argument for input files can be a comma separated list of file names or just a single name if there is only one input file. Command line arguments are optional if the file names are defined in the transform file. If the command line argument is defines, it takes precedence over the transform file. If the placeholder '_' is used as a filename on the command line, the filename from the transform file will be used instead.

    Returns:
        list: A list of input/output file names.

    Raises:
        Exception: If there are missing or mismatched filenames based on the provided arguments
                   and the transform file.

    Example:
        transform_file:
            "input_files": [
                {"filename": "input_file_1.csv"},
                {"filename": "input_file_2.csv"}
            ]

        args:
            input: input_file_3.csv,input_file_4.csv

        Code:
            from transform import get_filenames
            from argparse import Namespace
            transform_file = {
                "input_files": [
                    {"filename": "input_file_1.csv"},
                    {"filename": "input_file_2.csv"}
                ]
            }
            args = Namespace(input="_,input_file_4.csv", transform="transform_file.json", quiet=False)
            filenames = get_filenames(args, transform_file, "input")
            filenames

        Prints:
            Input file #1: input_file_1.csv (from transform file)
            Input file #2: input_file_4.csv (from command line)
        (unless --quiet is set on the command line)

        Returns:
            ['input_file_1.csv', 'input_file_4.csv']
    """
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
            f"No nodes defined in the {xput}_files section of the transform file.\n"\
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
    f"\nThe {xput} filename(s) can be defined as a command line argument --{xput} (comma separated list if more than one) or in {xput}_files section in the transform file '{args.transform}' (as a filename attribute under each {xput} node).\n" + \
    f"File names specified on the command line takes precedence over file names defined in the transform file.\n" + \
    f"A single underscore can be used as a placeholder for a filename on the command line if the filename is defined in the transform file and you don't want to override it with a command line argument.\n\n" + \
    f"The following {xput} files were specified:\n"

    tabulate(filenames_for_error_message, headers=["#", "Command line argument", "Transform file"], tablefmt="psql", showindex=False)

    if raise_exception:
        print(filename_help)
        sys.exit(1)

    if not filenames:
        print(f"No {xput} file names defined.\n{filename_help}")
        sys.exit(1)
    
    if not getattr(args, "quiet", False): # unless args.quiet
        for index, filename_from_command_line, filename_from_transform_file in filenames_for_error_message:
            if filename_from_command_line:
                print(f"{xput.capitalize()} file #{index}: {filename_from_command_line} (from command line)")
            elif filename_from_transform_file:
                print(f"{xput.capitalize()} file #{index}: {filename_from_transform_file} (from transform file)")

    return filenames


def get_node_attributes(transform_file, node_name, attribute_name, default=None):
    """
    Get List of Node Attributes from Transform File.

    This function retrieves a list of specified attributes associated with a particular
    node type from the loaded transform configuration. If the specified attribute is not
    found for a particular node, the default value is used. It provides a convenient way
    to extract attributes from nodes in the transform file.

    Args:
        transform_file (dict): The loaded transform configuration from the JSON file.
        node_name (str): The name of the node type from which to extract attributes.
        attribute_name (str): The name of the attribute to extract from each node.
        default (Any, optional): The default value to use if the specified attribute is
                                 not found in a node. Defaults to None.

    Returns:
        list: A list of attributes associated with the specified node type.

    Example:
        transform_file:
            "transformations": [
                {"input": ["name"], "function": "split_name", "output": ["first_name", "last_name"]},
                {"input": ["address"], "function": "split_address", "output": ["address_street", "address_house_number", "address_suffix", "postal_code", "city"]}
            ]

        Code:
            from transform import get_node_attributes
            transform_file = {
                "transformations": [
                    {"input": ["name"], "function": "split_name", "output": ["first_name", "last_name"]},
                    {"input": ["address"], "function": "split_address", "output": ["address_street", "address_house_number", "address_suffix", "postal_code", "city"]}
                ]
            }
            node_name = "transformations"
            attribute_name = "function"
            attributes = get_node_attributes(transform_file, node_name, attribute_name)
            attributes

        Returns:
            ['split_name', 'split_address']
    """
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

def generate_graphs(graphs, output_data):
    """
    Generate Graphs from Output Data.

    This function generates graphs from the output data based on the specified
    graph definitions in the transform file.

    Args:
        graphs (list): A list of graph definitions from the transform file.
        output_data (DataFrame): The output data from the transformations.
    """
    import matplotlib.pyplot as plt

    for graph_id, graph in enumerate(graphs):
        # NOTE: Adding breakpoints within the plotting (between any of the plt. assignments) will cause the graph not to be plotted correctly and give the following error:
        # Backend TkAgg is interactive backend. Turning interactive mode on.

        graph_id += 1 # start at 1 instead of 0
        # check graph definition
        for graph_definition_key in ['filename', 'title', 'series']:
            graph_definition_value = graph.get(graph_definition_key)
            if not graph_definition_value:
                print(f"{graph_definition_key} not defined for graph #{graph_id} -- skipping graph generation")
                continue

        print()
        print(f"Generating graph #{graph_id} ({graph['title']}))")
        # NOTE: rcParams must be set before creating the figure, otherwise it will have no effect
        plt.rcParams['figure.figsize'] = graph.get('size') or [16, 8]
        plt.title(
            graph['title'], 
            fontsize = graph.get('title_fontsize') or 'large',
            loc = graph.get('title_loc') or 'center',
            fontweight = graph.get('title_fontweight') or 'bold'
        )

        # check if X-axis and Y-axis are defined, or provide default values
        for graph_definition_key in ['X-axis', 'Y-axis']:
            axis_definition = graph.get(graph_definition_key)
            if not axis_definition:
                # create an empty one
                graph[graph_definition_key] = {}
            axis_title = axis_definition.get('title')
            # set axis titles if defined
            if axis_title:
                if graph_definition_key == 'X-axis':
                    plt.xlabel(axis_title)
                else:
                    plt.ylabel(axis_title)

        series_labels = []
        # loop through series and plot the lines
        for line_id, line in enumerate(graph['series']):
            # get x and y columns

            x_column_name = line.get('x').get('column')
            if not x_column_name:
                print(f"X-axis column not defined for series #{line_id}")
            elif not x_column_name in output_data.columns:
                print(f"X-axis column '{x_column_name}' not found in output data")
                print(f"Available columns: {list(output_data.columns)}")
                x_column_name = None
            else:
                # get series data
                x_column_data = output_data[x_column_name]

            y_column_name = line.get('y').get('column')
            if not y_column_name:
                print(f"Y-axis column not defined for series #{line_id}")
            elif not y_column_name in output_data.columns:
                print(f"Y-axis column '{y_column_name}' not found in output data")
                print(f"Available columns: {list(output_data.columns)}")
                y_column_name = None
            else:
                # get series data
                y_column_data = output_data[y_column_name]

            if not x_column_name or not y_column_name:
                print(f"Skipping series #{line_id}")
                continue

            # get series title
            line_label = line.get('label')
            if not line_label:
                line_label = y_column_name # use y column name as series title if not defined

            series_labels.append(line_label)

            # plot series
            plt.plot(x_column_data, y_column_data, label=line_label)

        show_legend = False
        # show a legend on the plot
        if graph.get('show_legend'):
            if graph['show_legend'] in ['True', 'true', '1', 'yes', 'Yes', 'YES', 'y', 'Y']:
                show_legend = True

        if show_legend:
            plt.legend(series_labels)

        # save graph to file
        print(f"Saving graph #{graph_id} ({graph['title']}) to file '{graph['filename']}'")
        plt.savefig(graph['filename'])


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

    # get prefix/suffix/rename info for input file fields (if any)
    field_prefixes = get_node_attributes(transform_file, 'input_files', 'field_prefix', default=None)
    field_suffixes = get_node_attributes(transform_file, 'input_files', 'field_suffix', default=None)
    rename_fields = get_node_attributes(transform_file, 'input_files', 'rename_fields', default=None)

    # get spreadsheet sheet names (if any)
    sheet_names = get_node_attributes(transform_file, 'input_files', 'sheets', default=None)

    # read input
    input_fields_per_file = []
    input_data = pd.DataFrame()
    for index, input_filename in enumerate(input_filenames, start=0):
        # determine what type of file we are reading
        if input_filename.endswith('.csv'):
            tmp_data = pd.read_csv(input_filename)
        # check if input file is a spreadsheet ('.ods', '.xlsx', '.xls')
        elif input_filename.endswith('.ods') or input_filename.endswith('.xlsx') or input_filename.endswith('.xls'):
            # Quit if multiple input_filenames are specified and at least one of them is a spreasheet, as loading multiple spreadsheets at once is not supported
            if len(input_filenames) > 1:
                # explain why we are quitting, then quit gracefully
                print("Multiple input files were specified, and at least one of them is a spreadsheet.")
                print("Loading multiple spreadsheets at once is not supported.")
                print("An alternative could be to extract the fields you need from each spreadsheet and generate a CSV file, then load and transform these CSV files.")
                sys.exit(1)
            if not args.quiet:
                print(f"Reading input file #{index + 1} (spreadsheet)")
                sys.stdout.flush() # flush stdout so that the print statement above is printed immediately
            spreadsheet = pd.ExcelFile(input_filename)
            if len(spreadsheet.sheet_names) == 1:
                # if there's only one sheet in the spreadsheet, just read it as if it was a CSV file
                if not args.quiet:
                    print(f"Spreadsheet has only one sheet ({spreadsheet.sheet_names[0]}) -- loading the dataframe directly into the input data")
                    sys.stdout.flush()
                tmp_data = pd.read_excel(input_filename)
            else:
                # loop through all sheets in the spreadsheet and read them into a dictionary with sheet names as keys
                if not args.quiet:
                    print(f"Spreadsheet has multiple sheets:")
                    for sheet_name in pd.ExcelFile(input_filename).sheet_names:
                        print(f"  {sheet_name}")
                    print("Loading each sheet into a separate dataframe and combining them as a dictionary with sheet names as keys.")
                    print("Fields can be referred to in the transform file by prefixing them with the sheet name and | (pipe), for example: Sheet1|Column1.")
                    if sheet_names[index]:
                        print(f"Sheets specified in the input section of the transform file: {sheet_names[index]}")
                    else:
                        print(f"No sheets specified in input section of the transform file -- using all sheets in spreadsheet.")
                        print("If you want to use only some of the sheets, add a 'sheets' node to the input_files section of the transform file.")
                    sys.stdout.flush()
                tmp_data = {}
                # loop through all the specified sheets, or if none are specified, all sheets in the spreadsheet
                for sheet_name in sheet_names[index] or spreadsheet.sheet_names:
                    if not args.quiet:
                        print(f"Reading sheet {sheet_name}")
                        sys.stdout.flush()
                    if not sheet_name in spreadsheet.sheet_names:
                        print(f"Sheet '{sheet_name}' not found in spreadsheet - exiting...")
                        sys.exit(1)
                    tmp_data[sheet_name] = pd.read_excel(input_filename, sheet_name=sheet_name)

        # add prefix to field names if defined in transform file
        # but only if tmp_data has a single sheet only
        if isinstance(tmp_data, pd.DataFrame): # on sheet only (not a dictionary)
            if field_prefixes[index]:
                tmp_data = tmp_data.add_prefix(field_prefixes[index]+'_')
            if field_suffixes[index]:
                tmp_data = tmp_data.add_suffix('_'+field_suffixes[index])
            if rename_fields[index]:
                tmp_data = tmp_data.rename(columns=rename_fields[index])

            input_data = input_data.combine_first(tmp_data)
            input_fields_per_file.append(list(tmp_data.columns))
        elif isinstance(tmp_data, dict): # multiple sheets (a dictionary)
            input_data = tmp_data # we can do this because loading multiple sheets is not supported

    # print field names of input data
    if not args.quiet:
        if isinstance(input_data, pd.DataFrame): # one sheet only (not a dictionary)
            for index, input_fields in enumerate(input_fields_per_file, start=1):
                print(f"Input fields from file #{index}: {list(input_fields)}")
            if len(input_fields_per_file) > 1:
                print(f"All input fields: {', '.join(input_data.columns)}")
        elif isinstance(input_data, dict): # multiple sheets (a dictionary)
            for sheet_name, sheet_data in input_data.items():
                print(f"Input fields from sheet '{sheet_name}': {list(sheet_data.columns)}")
        else:
            print(f"System error: Unsupported type of input data: {type(input_data)} -- exiting")
            sys.exit(1)
        print()

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
    try:
        graphs = transform_file['graphs']
        generate_graphs(graphs, output_data)
    except KeyError:
        pass

if __name__ == "__main__":
    main()

