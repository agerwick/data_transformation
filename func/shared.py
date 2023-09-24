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

    # format table for error message
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
