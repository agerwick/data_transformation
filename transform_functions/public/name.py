import pandas as pd

def split_name(input_data, input_fields, output_fields):
    """
    Split Full Names into First Name and Last Name Columns.

    This function takes input data with full names and splits them into separate
    first name and last name columns. The input and output fields are specified
    to match the field names of the input and output DataFrame.

    Args:
        input_data (pd.DataFrame): The input DataFrame containing the data to be split.
        input_fields (list): A list containing the name of the input field to be split.
        output_fields (list): A list containing the names of the output fields
                             where the first name and last name will be stored.

    Returns:
        pd.DataFrame: A DataFrame with two new columns for first name and last name.

    Raises:
        Exception: If the length of input_fields is not exactly 1 or the length of
                   output_fields is not exactly 2.

    Example:
        input_data:
            name
            John Doe
            Jane H. Smith
            Johnson, Robert A.

        input_fields:
            ['name']

        output_fields:
            ['first_name', 'last_name']

        Code:
            import pandas as pd
            from transform_functions.public.name import split_name
            input_data = pd.DataFrame({ 'name': ['John Doe', 'Jane H. Smith', 'Johnson, Robert A.'] })
            input_fields = ['name']
            output_fields = ['first_name', 'last_name']
            split_name(input_data, input_fields, output_fields)

        Returns:
            output_data:
                first_name     last_name
                John           Doe
                Jane           Smith
                Robert         Johnson
    """
    if len(input_fields) != 1:
        raise Exception(f"split_name() requires exactly one input field: name -- however, this was given: {input_fields}")
    if len(output_fields) != 2:
        raise Exception(f"split_name() requires exactly two output fields: first_name and last_name -- however, this was given: {output_fields}")
    output_data = pd.DataFrame()
    first_names = []
    last_names = []
    for index, row in input_data.iterrows():
        # if name is "last_name, first_name"
        if ',' in row[input_fields[0]]:
            last_name, first_name = [part.strip() for part in row[input_fields[0]].split(',', 1)]
        # if name is "first_name last_name"
        elif ' ' in row[input_fields[0]]:
            first_name, last_name = [part.strip() for part in row[input_fields[0]].rsplit(' ', 1)]
        # if name is just "first_name"
        else:
            first_name = row[input_fields[0]]
            last_name = ''
        first_names.append(first_name.strip())
        last_names.append(last_name.strip())
    output_data[output_fields[0]] = first_names
    output_data[output_fields[1]] = last_names
    return output_data

def combine_name_to_first_last(input_data, input_fields, output_fields):
    """
    Combine First Name and Last Name Fields into a Single 'Name' Field.

    This function combines first name and last name fields from the input DataFrame
    into a single 'name' field. The input and output field names are specified to match
    the field names of the input and output DataFrame.

    Args:
        input_data (pd.DataFrame): The input DataFrame containing the first name and last name columns.
        input_fields (list): A list containing the names of the input fields: [first_name_field, last_name_field].
        output_fields (list): A list containing the name of the output field: [combined_name_field].

    Returns:
        pd.DataFrame: A DataFrame with a combined 'name' field.

    Raises:
        Exception: If the length of input_fields is not exactly 2 or the length of
                   output_fields is not exactly 1.

    Example:
        input_data:
            | first_name | last_name |
            |------------|-----------|
            | John       | Doe       |
            | Jane       | Smith     |

        input_fields:
            ['first_name', 'last_name']

        output_fields:
            ['name']

        Code:
            import pandas as pd
            from transform_functions.public.name import combine_name_to_first_last
            input_data = pd.DataFrame({
                'first_name': ['John', 'Jane'],
                'last_name': ['Doe', 'Smith']
            })
            input_fields = ['first_name', 'last_name']
            output_fields = ['name']
            combine_name_to_first_last(input_data, input_fields, output_fields)

        Returns:
            | name         |
            |--------------|
            | John Doe     |
            | Jane Smith   |
    """
    if len(input_fields) != 2:
        raise Exception(f"combine_name() requires exactly two input fields: first_name and last_name -- however, this was given: {input_fields}")
    if len(output_fields) != 1:
        raise Exception(f"combine_name() requires exactly one output field: name -- however, this was given: {output_fields}")
    output_data = pd.DataFrame()
    names = []
    for index, row in input_data.iterrows():
        full_name = f"{row[input_fields[0]]} {row[input_fields[1]]}"
        names.append(full_name)
    output_data[output_fields[0]] = names
    return output_data
