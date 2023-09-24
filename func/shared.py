from tabulate import tabulate
import pandas as pd

def get_filenames(
        files_from_args, 
        files_from_transform_file, 
        file_type,
        fail_if_not_defined_in_transform_file=True, 
        quiet=False
    ):
    """
    Get List of Input/Output File Names from Command Line or Transform File.

    This function retrieves a list of input or output file names based on the specified
    command line argument or from the corresponding section in the transform file.
    The function ensures that the number of filenames defined in the transform file matches
    or exceeds the number of filenames given on the command line. If not, it adds empty nodes
    to the transform file to match the number of filenames provided on the command line.

    Args:
        files_from_args (str): A string with a comma separated list of file names from args.
        files_from_transform_file (dict): the input or output section from the transform file.
        file_type (str): The type of files (e.g., 'input' or 'output'). (only used for error messages to refer to the command line argument or section in the transform file)
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
        files_from_transform_file:
            [
                {"filename": "input_file_1.csv"},
                {"filename": "input_file_2.csv"}
            ]

        args:
            input: input_file_3.csv,input_file_4.csv

        Code:
            from func/shared import get_filenames
            files_from_transform_file = [
                {"filename": "input_file_1.csv"},
                {"filename": "input_file_2.csv"}
            ]
            files_from_args = ["input_file_4.csv"]
            filenames = get_filenames(files_from_args, files_from_transform_file, "input")
            filenames

        Prints:
            Input file #1: input_file_1.csv (from transform file)
            Input file #2: input_file_4.csv (from command line)
        (unless quiet=True)

        Returns:
            ['input_file_1.csv', 'input_file_4.csv']
    """
    # get in/output file names from either command line arguments or from the transform file (the nodes under output)
    filenames = []
    filenames_for_error_message = []
    filename_help = ''
    print_help = False

    # if no files are passed in the section_of_transform_file, set it to an empty list and optionally raise an exception
    if not files_from_transform_file:
        files_from_transform_file = []
        if fail_if_not_defined_in_transform_file:
            filename_help += \
            f"No nodes defined in the {file_type} section of the transform file.\n"\
            f"This section should contain a node for each {file_type} file.\n"
            print_help = True

    files_from_args_list = files_from_args.split(',') if files_from_args else []
    
    # make sure that the number of filenames defined in the transform file matches or exceeds the number of filenames given on the command line
    if files_from_args:
        number_of_files_from_args = len(files_from_args_list)
        number_of_files_from_transform_file = len(files_from_transform_file)
        if number_of_files_from_transform_file < number_of_files_from_args:
            if fail_if_not_defined_in_transform_file:
                filename_help += \
                f"ERROR:\nNumber of {file_type} files defined in the transform file ({number_of_files_from_transform_file}) is less than the number of {file_type} files defined on the command line ({number_of_files_from_args}).\n"
                print_help = True
            else:
                # add empty nodes to the transform file to match the number of filenames given on the command line
                for i in range(number_of_files_from_args - number_of_files_from_transform_file):
                    files_from_transform_file.append({})

    for index, file_from_transform_file in enumerate(files_from_transform_file, start=0):
        filename_from_transform_file = file_from_transform_file.get('filename')
        if index < len(files_from_args_list):
            filename_from_command_line = files_from_args_list[index]
            # replace empty string, space(s) or placeholder _ with None - filename from transform file will be used instead
            if filename_from_command_line.strip(' ') in ['', '_']:
                filename_from_command_line = None
        else:
            filename_from_command_line = None
        
        # assemble list of file names for error message
        filenames_for_error_message.append([index + 1, filename_from_command_line, filename_from_transform_file])
        
        if filename_from_transform_file or filename_from_command_line:
            filenames.append(filename_from_command_line or filename_from_transform_file)
        else:
            filename_help += f"File name not defined for {file_type} #{index + 1}.\n"
            print_help = True

    filename_help += \
    f"\nThe {file_type} filename(s) can be defined as a command line argument --{file_type} (comma separated list if more than one) or in the {file_type} section in the transform file (as a filename attribute under each {file_type} node).\n" + \
    f"File names specified on the command line takes precedence over file names defined in the transform file.\n" + \
    f"A single underscore (or nothing) can be used as a placeholder for a filename on the command line if the filename is defined in the transform file and you don't want to override it with a command line argument, but you want to override another file.\n" + \
    f"example: --{file_type}=_,_,3rdfile.csv\n" + \
    f"(here the 1st and 2nd filename will be from the transform file and the 3rd from the command line)\n\n" + \
    f"The following {file_type} files were specified:\n"

    # format table for error message
    tabulate(filenames_for_error_message, headers=["#", "Command line argument", "Transform file"], tablefmt="psql", showindex=False)

    if not filenames:
        print(f"ERROR:\nNo {file_type} file names defined.")
        print_help = True
    
    if print_help:
        print(filename_help)

    if not quiet:
        for index, filename_from_command_line, filename_from_transform_file in filenames_for_error_message:
            if filename_from_command_line:
                print(f"{file_type.capitalize()} file #{index}: {filename_from_command_line} (from command line)")
            elif filename_from_transform_file:
                print(f"{file_type.capitalize()} file #{index}: {filename_from_transform_file} (from transform file)")

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

def replace_placeholders(user_string, variable_substitutions):
    # example usage:
    # user_string = "The temperature is {temperature} degrees Celsius"
    # variable_substitutions = {"temperature": 25}
    # print(__replace_placeholders(user_string, variable_substitutions))
    # output: The temperature is 25 degrees Celsius

    # If the user_string is not a string, return it as is
    # this is useful if a routine generating the string returns None, for example
    if type(user_string) != str:
        return user_string

    # Iterate through each replacement in the dictionary
    for key, value in variable_substitutions.items():
        # Define the placeholder to search for (e.g., {temperature})
        placeholder = f"{{{key}}}"
        # Replace the placeholder with the corresponding value
        user_string = user_string.replace(placeholder, str(value))
    return user_string

def print_data_summary(data):
    # data is a dictionary of dictionaries, key: data_source, value: dict with key:  dataframe with data from that sheet (sheet name is "csv" if the file is a csv file)
    # example:
    # {
    #     "input_1": {
    #       "csv": <pandas dataframe>
    #     },
    #     "input_2": {
    #       "csv": <pandas dataframe>
    #     },
    #     "input_3": {
    #       "sheet1": <pandas dataframe>, 
    #       "sheet2": <pandas dataframe>
    #     }
    # }

    # loop through all data entries and print the sheet names, column names and number of rows
    for data_source, data_entry_dict in data.items():
        for data_entry, dataframe in data_entry_dict.items():
            # number of columns in dataframe
            print(f"Data source:'{data_source}', Data entry:'{data_entry}' (cols: {len(dataframe.columns)}, rows: {len(dataframe)})")
            print(f"  Columns: {list(dataframe.columns)}")
    print()

def check_data_source_and_entry(data, input_fields, section="transformation"):
    # data is a dictionary with key: data_source ("input1", ...) and value: another dictionary with key: data_entry (sheet_name from spreadsheet or "csv" for CSV files) and value: a dataframe with data from that sheet or CSV file
    # example:
    # {
    #     "input_1": {
    #         "sheet1": <pandas dataframe>
    #         "sheet2": <pandas dataframe>
    #     }
    # }
    # if there's only one file, then the list will only contain one dictionary
    # if the file is a csv file, then the sheet_name will be "csv".
    # Here's an example where 2 csv files are loaded:
    # {
    #     "input_1": {
    #         "csv": <pandas dataframe>
    #     },
    #     "input_2": {
    #         "csv": <pandas dataframe>
    #     }
    # ]
    # The order in the numbering of data sources is the same as the order in which the files were specified in the transform file or command line argument.

    # input_fields is either:
    # - a list of fields to be used by the transform function calling this routine
    #   (note that this only works if there's only one data source and one data entry in the data)
    # example: ["first_name", "last_name"]
    # - a dictionary with these keys:
    #   - "data_source": the name of the data source (e.g., "input_1")
    #   - "data_entry": the name of the data entry (e.g., "sheet1" or "csv")
    #   - "fields": a list of fields to be used by the transform function calling this routine
    # example: {
    #     "data_source": "input_1",
    #     "data_entry": "sheet1",
    #     "fields": ["first_name", "last_name"]
    # }

    # if input_fields is a list, convert it to a dictionary
    if type(input_fields) == list:
        input_fields = {
            "data_source": "input_1",
            "data_entry": "csv",
            "fields": input_fields
        }
    elif type(input_fields) != dict:
        print(f"ERROR:\nThe {section} section must be a list or a dictionary. It is {type(input_fields)}. Here is a valid example:")
        print(f"""
        "{section}": {
            "data_source": "input_1",
            "data_entry": "csv",
            "fields": ["first_name", "last_name"]
        }
        """)
        print("Optionally, if you only have one data source and one data entry, you can use a list instead of a dictionary:")
        print("""
        "input": ["first_name", "last_name"]
        """)
        return None
    
    # get data source and data entry from input_fields, if they exist
    data_source = input_fields.get("data_source")
    data_entry = input_fields.get("data_entry")

    # if data_source is not specified, check the data - if there's only one data source, use that. If not, print error and return None
    if not data_source:
        if len(data) == 1:
            data_source = list(data.keys())[0]
            input_fields["data_source"] = data_source
            print(f"Data source not specified for {section}, but there's only one data source in the data.")
            print("Using data source '{data_source}'")
        else:
            print(f"ERROR:\nData source not specified for {section} and there's more than one data source in the data.")
            print(f"Available data sources: {list(data.keys())}")
            print(f"Specify the data source in the {section} section of the transform file.")
            return None
    # if data_source is specified, check if it exists in the data - if not, print error and return None
    else:
        if not data_source in data:
            print(f"ERROR:\nData source '{data_source}' not found in data.")
            print(f"Available data sources: {list(data.keys())}")
            return None

    # if data_entry is not specified, check the data - if there's only one data entry, use that. If not, print error and return None
    if not data_entry:
        if len(data[data_source]) == 1:
            data_entry = list(data[data_source].keys())[0]
            input_fields["data_entry"] = data_entry
            print(f"Data entry not specified for {section}, but there's only one data entry in data source '{data_source}'.")
            print(f"Using data entry '{data_entry}'")
        else:
            print(f"ERROR:\nData entry not specified for {section} and there's more than one data entry in data source '{data_source}'.")
            print(f"Available data entries: {list(data[data_source].keys())}")
            print(f"Specify the data entry in the {section} section of the transform file.")
            return None
    # if data_entry is specified, check if it exists in the data - if not, print error and return None
    else:
        if not data_entry in data[data_source]:
            print(f"ERROR:\nData entry '{data_entry}' not found in data source '{data_source}'.")
            print(f"Available data entries: {list(data[data_source].keys())}")
            return None
    
    return input_fields


def structure_dataframe(new_data, existing_data={}, data_source=None, data_entry='data'):
    # add the structure for the output data to make it compatible with the input data
    import inspect
    calling_function = inspect.stack()[1].function # grab name of the calling function

    # if the data is not a dataframe, raise an exception as this is a developer error
    if type(new_data) != pd.DataFrame:
        raise Exception(f"structure_dataframe() requires a pandas dataframe as input. However, this was given: {type(new_data)} (called from {calling_function})")

    structured_data = {
        data_source or calling_function: {
            data_entry: new_data
        }
    }
    
    # add new data to existing data (or an empty dict if not supplied)
    existing_data.update(structured_data)
    return existing_data

