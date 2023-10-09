import sys
import os
import json
import pandas as pd
from func.shared import get_filenames, print_data_summary, resource_name_match

def get_input_data(input_files_from_args, transform_file_input_section, quiet=False):
    """ 
    Read input data from files specified in the transform file.
    The input data is returned as a pandas dataframe.
    """
    transform_file_input_section = transform_file_input_section or []

    # get input file names
    input_filenames = get_filenames(
        input_files_from_args,          # comma separated string of input files from args
        transform_file_input_section,   # input section of the transform file
        'input',                        # this is used for error messages to make it clear what kind of files we are talking about
        fail_if_not_defined_in_transform_file=False,
        quiet=quiet
    )

    if not input_filenames:
        print("No input files specified in transform file or in command line arguments -- exiting...")
        sys.exit(1)

    # get prefix/suffix/rename info for input file fields (if any)
    field_prefixes = [input_file.get("field_prefix") for input_file in transform_file_input_section]
    field_suffixes = [input_file.get("field_suffix") for input_file in transform_file_input_section]
    rename_fields = [input_file.get("rename_fields") for input_file in transform_file_input_section]
    file_name_column = [input_file.get('file_name_column') for input_file in transform_file_input_section]
    dir_name_column = [input_file.get('dir_name_column') for input_file in transform_file_input_section]
    dir_and_file_name_column = [input_file.get('dir_and_file_name_column') for input_file in transform_file_input_section]
    dir_levels_to_include = [input_file.get('dir_levels_to_include') for input_file in transform_file_input_section]
    # TODO: add support for file and dir name columns on a per data_entry (sheet) basis (currently it's only supported on a per input file basis, so it will add on all sheets. This is not a problem for CSV files, but it is for spreadsheets with multiple sheets)

    # get spreadsheet sheet names (if any)
    transform_file_sheet_names = [input_file.get("sheets") for input_file in transform_file_input_section]

    # all of the above return lists in the same order as the input files so it can be easily accessed with the index, and if a value is not defined for a specific input file, the corresponding list element is None

    # read input
    input_data = {}
    input_file_metadata = {}
    # input_data is a dictionary with data_source ("input_1", "input_2"), etc as keys and the data_entry as values. 
    # The data_entry is a dictionary with sheet names as keys ("csv" for CSV files) and dataframes as values.
    for index, input_filename in enumerate(input_filenames, start=0):
        data_source = f"input_{index + 1}"

        # check if file exists, output error message and exit if it doesn't
        try:
            with open(input_filename) as f:
                pass
        except IOError:
            print(f"ERROR:\nInput file '{input_filename}' not found - exiting...")
            sys.exit(1)

        # get file name and directory name for use in dataframes if specified
        file_name_for_data = os.path.basename(input_filename)
        dir_name_for_data = os.path.dirname(input_filename)
        if dir_levels_to_include and dir_levels_to_include[index]:
            dir_name_for_data = os.path.join(*dir_name_for_data.split(os.sep)[-dir_levels_to_include[index]:])
        dir_and_file_name_for_data = os.path.join(dir_name_for_data, file_name_for_data)

        # load input file metadata (if any)
        # look for a filename with the same filename but with additional extension ".meta.json" and load the metadata from that file:
        # example: if input file is "data.csv", look for "data.meta.json"
        # if the metadata file doesn't exist, the metadata will be an empty dictionary
        input_file_metadata_filenames = [
            input_filename + ".meta.json", # per file metadata
            os.path.join(os.path.dirname(input_filename), "/../metadata.json") # metadata for all files in directories below the parent of this file's directory (if this file is a/b/c/data.csv, look for a/metadata.json)
        ]
        for input_file_metadata_filename in input_file_metadata_filenames:
            input_file_metadata_tmp = {} # reset so that we don't accidentally use metadata from a previous file
            if os.path.isfile(input_file_metadata_filename):
                try:
                    with open(input_file_metadata_filename, 'r') as f:
                        input_file_metadata_tmp = json.load(f)
                except:
                    print(f"\nERROR:\nInput file metadata '{input_file_metadata_filename}' could not be loaded:\n{sys.exc_info()[1]}\n-- continuing...\n")
            if input_file_metadata_tmp:
                print(f"Loading metadata from '{input_file_metadata_filename}'")
                print(f"Metadata: {input_file_metadata_tmp}")
                # add the data source as a key to the metadata dictionary
                input_file_metadata[data_source] = input_file_metadata_tmp

        # determine what type of file we are reading
        if input_filename.endswith('.csv'):
            # to make loading a csv compatible with loading a multi sheet spreadsheet, we load it into a dictionary with a single key 'csv'
            tmp_data = {"csv": pd.read_csv(input_filename)}
            # TODO: support custom data_entry names for CSV files to replace "csv"
        # check if input file is a spreadsheet ('.ods', '.xlsx', '.xls')
        elif input_filename.endswith('.ods') or input_filename.endswith('.xlsx') or input_filename.endswith('.xls'):
            if not quiet:
                print(f"Reading input file #{index + 1} (spreadsheet)")
                sys.stdout.flush() # flush stdout so that the print statement above is printed immediately
            spreadsheet = pd.ExcelFile(input_filename)
            # loop through all sheets in the spreadsheet and read them into a dictionary with sheet names as keys
            if not quiet:
                print(f"The spreadsheet has these sheets:")
                for sheet_name in spreadsheet.sheet_names:
                    print(f"  {sheet_name}")
                print("Loading each sheet into a separate dataframe and combining them as a dictionary with sheet names as keys.")
                if transform_file_sheet_names[index]:
                    print(f"Sheets specified in the input section of the transform file: {transform_file_sheet_names[index]}")
                else:
                    print(f"No sheets specified in input section of the transform file.")
                    if len(spreadsheet.sheet_names) == 1:
                        print(f"But there's only one sheet in the spreadsheet, so it wouldn't make a difference.")
                    else:
                        print(f"Loading all sheets in spreadsheet.")
                        print("If you want to use only some of the sheets, add a 'sheets' node to the input section of the transform file and specify sheet names there.")
                sys.stdout.flush()
                tmp_data = {}
                # loop through all the specified sheets, or if none are specified, all sheets in the spreadsheet
                for sheet_name_tmp in transform_file_sheet_names[index] or spreadsheet.sheet_names:
                    if type(sheet_name_tmp) is dict:
                        # example: "input": [
                        #   { "sheets": [{"Sheet_{*}_data": "Data"}] }
                        # ]
                        sheet_name = list(sheet_name_tmp.keys())[0] # "Sheet_{*}_data"
                        rename_to_sheet_name = sheet_name_tmp[sheet_name] # "Data"
                    elif type(sheet_name_tmp) is str:
                        # example: "input": [
                        #   { "sheets": ["Sheet_1_data"] }
                        # ]
                        sheet_name = sheet_name_tmp
                        rename_to_sheet_name = False
                    else:
                        print(f"ERROR:\nSheet name in transform file must be a string or a dictionary - exiting...")
                        sys.exit(1)

                    if not quiet:
                        print(f"Reading sheet '{sheet_name}")
                        sys.stdout.flush()
                    # we must assign the output from resource_name_match, as this will be the matched sheet name in case the given sheet_name is a template ("Sheet_{*}")
                    actual_sheet_name = resource_name_match(sheet_name, spreadsheet.sheet_names, "sheet name") # "Sheet_1_data", the name of the sheet in the spreadsheet, not the template
                    if not actual_sheet_name:
                        print(f"ERROR:\nSheet '{sheet_name}' not found in spreadsheet - exiting...")
                        sys.exit(1)
                    tmp_data[rename_to_sheet_name or actual_sheet_name] = pd.read_excel(input_filename, sheet_name=actual_sheet_name)
                    # if sheet-name from input[].sheets list contains a dictionary, the key is the sheet name in the spreadsheet (or a template with placeholder), and the value is the name we want to use for the sheet when passing to the transform function
        else:
            print(f"ERROR:\nUnsupported input file type: {input_filename} - exiting...")
            sys.exit(1)

        if file_name_column and file_name_column[index]:
            for data_entry in tmp_data.keys():
                tmp_data[data_entry].insert(0, file_name_column[index], file_name_for_data)
        if dir_name_column and dir_name_column[index]:
            for data_entry in tmp_data.keys():
                tmp_data[data_entry].insert(0, dir_name_column[index], dir_name_for_data)
        if dir_and_file_name_column and dir_and_file_name_column[index]:
            for data_entry in tmp_data.keys():
                tmp_data[data_entry].insert(0, dir_and_file_name_column[index], dir_and_file_name_for_data)

        # field renaming (prefixing, suffixing, renaming) which is mostly used for CSV files
        field_prefix = field_suffix = rename_field = None
        if len(field_prefixes) > index:
            field_prefix = field_prefixes[index]
        if len(field_suffixes) > index:
            field_suffix = field_suffixes[index]
        if len(rename_fields) > index:
            rename_field = rename_fields[index]
        
        if field_prefix or field_suffix or rename_field:
            # add entries so that the lists are the same length as the number of input files
            # loop through all sheets in the spreadsheet and do renaming, prefixing, suffixing:
            for sheet_name, sheet_data in tmp_data.items():
                # add prefix to field names if defined in transform file and the field name exists
                if field_prefix and sheet_data.columns.isin([field_prefix]).any():
                    print(f"Adding prefix '{field_prefix}_' to field names in data entry '{sheet_name}'")
                    sheet_data = sheet_data.add_prefix(field_prefix+'_')
                if field_suffix and sheet_data.columns.isin([field_suffix]).any():
                    print(f"Adding suffix '_{field_suffix}' to field names in data entry '{sheet_name}'")
                    sheet_data = sheet_data.add_suffix('_'+field_suffix)
                if rename_field and sheet_data.columns.isin(list(rename_field.keys())).any():
                    print(f"Renaming fields {list(rename_field.keys())} in data entry '{sheet_name}' to {list(rename_field.values())}:")
                    sheet_data = sheet_data.rename(columns=rename_field)
                tmp_data[sheet_name] = sheet_data
                # TODO: this renaming is now done across all sheets, but it should be done per sheet, if we had a way to specify which sheet the renaming applies to.
                # this could for example be by specifying the renaming under the sheet name in the json transform file, like this:
                # "input": [
                #     {
                #         "sheets": [
                #                     {
                #                       "name": "Record Layer",
                #                       "rename_fields": {
                #                         "old_field_name": "new_field_name"
                #                       }
                #                     }
                #                   ]
                #     }
                # ]
                # we would need to detect if the sheet name is a string or a dictionary, and if it's a dictionary, we would need to loop through the sheets and rename the fields for each sheet
                # however, as this is not needed yet, we'll leave it for now
                # renaming fields has only been useful for single sheet spreadsheets or CSV files, so it's not a big problem that it's not implemented for multi sheet spreadsheets yet

        # add the dictionary with data entries and dataframes to the input_data dictionary with data source ("input_1, ...") as keys
        input_data[data_source] = tmp_data

    # end of: for index, input_filename in enumerate(input_filenames, start=0):

    # check metadata for actions that should be performed on the input data
    input_data = process_input_metadata(input_data, input_file_metadata)

    # create a dict with data_source as key and file name as value
    filenames_for_metadata = {}
    for index, input_filename in enumerate(input_filenames, start=1):
        filenames_for_metadata[f"input_{index}"] = input_filename

    structured_data = {
        "data": input_data,
        "metadata": {"input_filenames": filenames_for_metadata}
    }

    # print field names of input data
    if not quiet:
        print()
        print_data_summary(structured_data)

    return structured_data

def process_input_metadata(input_data, input_file_metadata):
    """
    example input_file_metadata:
    
    As specified in filename.meta.json: 
    {
        "csv": {
            "find_records": {
                "criteria": {
                    "Status": "Inactive",
                    "Preserve": "False"
                },
                "action": {
                    "update_records": {
                        "Reason to Delete": "Inactive Records"
                    }
                }
            },
            "find_records": {
                "criteria": {
                    "record_id": "123"
                },
                "action": {
                    "delete_records": "True"
                }
            }
        }
    }

    this is what this function gets (after loading the json file and adding the input file name as a key):
    {
        "input_1": {
            "csv": {
                "add_column": {
                    "Reason to Delete": ""
                },
                "find_records": {
                    "criteria": {
                        "Status": "Inactive",
                        "Preserve": "False"
                    },
                    "action": {
                        "update_records": {
                            "Reason to Delete": "Inactive Records"
                        }
                    }
                },
                "find_records": {
                    "criteria": {
                        "record_id": "123"
                    },
                    "action": {
                        "delete_records": "True"
                    }
                }
            }
        }
    }
    """
    # this will
    # - add a new column called "Reason to Delete" to all records and make the default value an empty string
    # - find all records status is inactive and preserve is false
    #   - update the Reason to Delete field to "Inactive Records"
    # - find all records where record_id is 123
    #   - delete them
    # for now, we can only do exact matches, but we could add support for regex, <, >, etc. later
    if input_file_metadata:
        for data_source, data_entries in input_file_metadata.items(): # "input1", {"csv": ...}
            for data_entry, tmp_metadata in data_entries.items(): # "csv", {"find_records": ...}
                for metadata_entry, metadata_info in tmp_metadata.items(): # "find_records", {"criteria": ..., "action": ...}
                    if metadata_entry == "add_column":
                        for field_name, field_value in metadata_info.items():
                            input_data[data_source][data_entry][field_name] = field_value
                            print(f"process_input_metadata: added column '{field_name}'='{field_value}' to '{data_source}'/'{data_entry}'")
                    elif metadata_entry == "find_records":
                        message_printed = False
                        criteria = metadata_info.get("criteria") # {"record_id": "123"}
                        action_dict = metadata_info.get("action") # {"delete_records": "True"}
                        # if all field names and field values match, delete the record
                        for field_name, field_value in criteria.items(): # "record_id", "123"
                            matching_records = input_data[data_source][data_entry][field_name].isin([field_value])
                            # TODO: Support multiple criteria
                            if matching_records.any():
                                if not message_printed: # only print this once per data entry
                                    print(f"process_input_metadata: found records matching {criteria} in data entry '{data_entry}' in data source '{data_source}' -- action: {action_dict}")
                                    message_printed = True
                                for action, action_info in action_dict.items():
                                    if action == "update_records":
                                        # update all records that match the criteria
                                        for field_name, field_value in action_info.items():
                                            input_data[data_source][data_entry].loc[matching_records, field_name] = field_value
                                            print(f"process_input_metadata: updated field '{field_name}'='{field_value}' for records matching {criteria} in '{data_source}'/'{data_entry}'")
                                    elif action == "delete_records":
                                        # delete all records that match the criteria
                                        print(f"process_input_metadata: deleting records matching {criteria} in '{data_source}'/'{data_entry}'")
                                        input_data[data_source][data_entry] = input_data[data_source][data_entry][~matching_records] # keep non-matching records
    return input_data
