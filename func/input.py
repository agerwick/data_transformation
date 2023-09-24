import pandas as pd
from func.shared import get_filenames, get_node_attributes

def get_input_data(args, transform_file):
    """ 
    Read input data from files specified in the transform file.
    The input data is returned as a pandas dataframe.
    """

    # get input file names
    input_filenames = get_filenames(args, transform_file, 'input', fail_if_not_defined_in_transform_file=False)

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

    return input_data
