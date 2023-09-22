import pandas as pd

def extract_single_sheet(input_data, input_fields, output_fields):
    # input data is a dictionary with the name of the sheet as key and the dataframe as value
    # input fields look like this:
    """
    {
        "sheet": "Sheet1",
        "columns": ["Date", "Order Number", "Order Value"]
    }
    """
    df = pd.DataFrame() # default return value

    # check if sheet name is specified in input fields
    if "sheet" not in input_fields:
        print("Sheet name not specified -- nothing will be returned")
        print("You can define a sheet name in the transform file like this:")
        print("""
    "transformations": [
        {
            "function": "extract_single_sheet",
            "input": {
                "sheet": "Orders",
                "columns": ["date", "order number", "order amount"]
            },
            "output": []
        }
    ],
""")
    else:
        sheet_name = input_fields["sheet"]
        # check if specified sheet name exists in the input data
        if sheet_name not in input_data:
            print(f"The sheet name specified in the transformations section of the transform file ({sheet_name}) does not exist in input data")
            print("The input data has the following sheets:")
            print(list(input_data.keys()))
        else:
            df = input_data[sheet_name]
            # check if columns are specified in input fields
            if "columns" in input_fields:
                columns = input_fields["columns"]
                # check if specified columns exist in the sheet
                missing_columns = [col for col in columns if col not in df.columns]
                if len(missing_columns) > 0:
                    print(f"The following columns were specified in the transformations section of the transform file:")
                    print(f"  {columns}")
                    print(f"But the following columns do not exist in the sheet {sheet_name}:")
                    print(f"  {missing_columns}")
                    print("The sheet has the following columns:")
                    print(f"  {list(df.columns)}")
                    print("Nothing will be returned.")
                    df = pd.DataFrame() # return nothing
                else:
                    # add the selected columns to the output data
                    df = df[columns]
            else:
                # return all columns from the sheet
                pass

    # instruct parent to delete all existing data in the output table before writing the new data returned from this function
    # this is required as the input table is a dictionary with the name of the sheet as key and the dataframe as value, and merging this with the output data (which is a single dataframe) will fail, as well as being be pointless given that we're extracting some columns from a single sheet.
    metadata = {"clear_input_data": True}

    return df, metadata
