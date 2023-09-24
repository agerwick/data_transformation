# data_transformation
A python data transformation framework for data wizardry, as well as ordinary data transformation.

## Purpose
Standardize and streamline the approach to reading data files, processing them in some way, then writing modified output files.

## Method
Input file(s), Transform file and Output file(s) are specified as command line parameters

Input and Output files can optionally be specified in the Transform file

The Transform file is a json file that defines how to transform the data from the input file(s) as well as what data to write to the output file(s).

The transformation file has three sections:
- **import** - defines which transform function(s) to import
- **transformations** - defines how to apply the transform functions and what to call the output fields
  - **input** - which input fields to send to the transform function
  - **function** - which transform function to use
  - **output** - what to call the output fields produced by the transform function
- **input** - a list of input files to read from, each with these attributes
  - **filename** - specified the filename of the input file. Can be overridden with command line param --input
  - **field_prefix** - an optional prefix to add to each field name for this file. Used to avoid two fields with the same name.
  - **field_suffix** - an optional suffix to add to each field name 
  - **rename_field** - contains a list of key/value pairs where the key is the field name in this file and the value is the field name to use instead. This is used in cases where only a few fields have the same name as previously loaded files
- **output** - a list of output files to produce, each with these attributes
  - **filename** - specifies the filename of the output file. Can be overridden with command line param --output
  - **fields** - which fields (from either input files or the transform function) to write to the output file
- **graphs** - a list of graphs to produce, each with these attributes
  - **filename** - specified the filename of the graph. The file format is decided by the extension (.png, .svg, ...)
  - **title** - the title of the graph
  - **series** - gre graphs to plot
  - **X-axis** - X-axis attributes (only title for now)
  - **Y-axis** - Y-axis attributes (only title for now)

# Examples
Execute these examples so see more information about what they do. The output from the script is pretty descriptive.

## Extract one field from the CSV input file
This example doesn't do any transformation at all, it simply reads the input file (with name and address) and creates a new output file with only one field (address).

```
$ python transform.py --input data/sample/input/names_and_addresses.csv --output data/sample/output/extracted_addresses.csv --transform data/sample/transform_files/extract_address.json
```

## Split name in first_name and last_name, and split address in street_name, house_number and suffix
This is a good example of how to sanitize address lists. The input field has a name field, with both first and last name and an address field with the complete address Norwegian addresses are used here, and while some are written correctly, others are written in an unstandard way, which happens often when people enter their own address on forms.

Here, two transform functions are used, one for name and one for address. Also, tree separate output files are produced. This could be used for input into different systems, for example.

All the output filenames are defined in the transform file, so we don't need a command line argument for output files.

```
$ python transform.py --input data/sample/input/names_and_addresses.csv --transform data/sample/transform_files/split_name_and_address.json
```

## Merge/join two files
For now, the only way to merge two files is to merge by row number, so the first customer gets the first customer number and so on. Later, we'll introduce more complex ways of merging files.
This example defines both input and output files in the transform file, so we only need to specify the transform file.

We have one file with a list of customers and addresses, and another file with a list of customer numbers. Each row corresponds to the same row in the other file. Just to show what happens, the file customer_number.csv has one moe entry than names_and_addresses.csv - the result is one blank line with only a customer number.

We also have a second file with a different set of customer numbers, but with the same field name in the header. If you try to load two files with the same field names, only the first one will be used and any subsequent field with an already existing name will be discarded. In order to avoid this, we have 3 ways of renaming fields; adding a prefix to all field names, adding a suffix to all field names, or renaming individual fields. In this example we've renamed the one field with the same name.

In the transform file's output section, we've specified the fields as `["customer_id", "customer_id2", "*"]` to demonstrate the use of asterisk to add _the rest of the fields_ (in alphabetical order).
```
$ python transform.py --transform data/sample/transform_files/join_two_files.json
```

## Sanitize data (fix name columns containing both "First Last" and "Last, First" entries)
In order to do this, we can simply split the "name" field in first and last name using a function that supports both formats, then combine them back to "name":
```
python transform.py --transform data/sample/transform_files/normalize_name.json
```

## Pivot table (add one new column for each value in a specified column)
For testing this, we use publicly available battery testing data. The test has been done in 100 cycles (one sample per second in each cycle), and we want to have the Capacity for each Cycle as one column instead. Try this, look at the input file and compare with the output file:
```
python transform.py --transform data/sample/transform_files/pivot_table.json
```

# Transform file documentation
The transform file is a JSON file located in `./data/transform_files/` or wherever you want to place it. Example transform files can be found in `./data/sample/transform_files/`.

## import section
If you are going to use transform functions, they need to be imported here. 

The **"module"** parameter contains the path to the library. In these two examples, the files containing the functions are located in `./transform_functions/public/` and are called `name.py` and `address_norway.py`.

**"functions"** contain a list of functions declared in these files.
See the "transformations" sections on how to call these functions.

    "import": [
        {
            "module": "transform_functions.public.name",
            "functions": ["split_name", "combine_name_to_first_last"]
        },
        {
            "module": "transform_functions.public.address_norway",
            "functions": ["split_address"]
        }
    ]
You can place your own private transform functions in `./transform_functions/private/`, as this directory is *not* committed to source control (git). Here's an example:

    "import": [
        {
            "module": "transform_functions.private.my_transform_functions",
            "functions": ["my_private_transform_function"]
        }
    ]
This will make the transform script look for a declared function named `my_private_transform_function` in `./transform_functions/private/my_transform_functions.py`

## input section
The entire input section is optional. If you don't need any field renaming and want to specify the filename on the command line.

Each node here represents an input file. All parameters optional, even filename (as this can be specified on the command line, even if you have renaming options specified here).

An empty node is a valid entry, and can be used for example if input file #1 will be specified on the command line, but you need to specify a filename or some parameters for input file #2.

In this example, a filename is specified for input file #2. It will be used unless another one is specified on the command line, and it will fail if input file #1 is not specified on the command line:

    "input": [
        {},
        {
            "filename": "data/sample/input/names_and_addresses.csv"
        }
    ]
In this example, if the input file contains a fields called "customer_id" and "name", they will be renamed to "original_customer_id_from_legacy_software" and "original_name_from_legacy_software", respectively.

    "input": [
        {
            "filename": "data/sample/input/customer_numbers.csv",
            "field_prefix": "original",
            "field_suffix": "from_legacy_software"
        }
    ]
In this example, if the input file contains a field name called "customer_id", it will be renamed to "customer_id2". If the field doesn't exist, nothing will happen.

    "input": [
        {
            "filename": "data/sample/input/customer_numbers2.csv",
            "rename_fields": {
                "customer_id": "customer_id2"
            }
        }
    ]
Note that if both field_prefix/suffix and rename_fields are used on the same input file, rename_fields needs to reference the field names with the added prefix/suffix, as prefixes and suffixes are added first.

## transformations section
In this example, the field "name" from any of the input files will be passed to the transform function called "split_name", which will generate two new fields named "first_name" and "last_name". If any of these field names are already in use (loaded from the input files or generared by previous transformations), they will be overwritten.

The entire section is optional. Transform functions will be executed in the given order, and one transform function can use the output from an earlier one.

Transform functions executed in the transform functions may return metadata in addition to output data. The following meta data is currently supported:

-  clear_input_data - if set to True, instead of merging output data with input data, use only the output data
-  variable_substitutions - a dictionary with variables and values to replace in graphs. For example, if the transform function returns {variable_substitutions: {"type": "R2D2"}} which is something derived from analyzing the data and if the graph title (as defined in the transform file) is "Blah graph for {type}" then the graph title will be replaced with "Blah graph for R2D2".

The "split_name" function must be declared in one of the modules imported in the import section.

    "transformations": [
        {
            "input": ["name"],
            "function": "split_name",
            "output": ["first_name", "last_name"]
        }
    ]

Depending on the transform functions, different input formats can be used. For example, on the pivot transform function (in transform_functions/public/pivot.py), the input is a dictionary instead of a list:

    "transformations": [
        {
            "function": "pivot",
            "input": {
                "index_column": "time (s)",
                "pivot_column": "cycle",
                "data_column": "capacity (Ah)"    
            },
            "output": []
        }
    ]

## output section
The output sections defines the file names and field names for the output file(s). The output section, as well as at least one node with the "fields" parameter are required. Any number of output files can be defined.

The **"filename"** parameter is optional, but must be provided on the command line if not defined. If defined both places, the file name provided on the command line takes precedence.
The **"fields"** sections defines which fields are to be written to the output file. Obviously these fields must all exist. They can be a combination of fields copied from the input files and output from the transform functions.

    "output": [
        {
            "filename": "data/sample/output/cutomer_id_names_addresses.csv",
            "fields": ["customer_id", "name", "address"]
        }
    ]

## graph section
The optional graph sections defines graphs to produce. Here's an example:
(in this example, the transform function returns metadata such as {"variable_substitution": {"date": "20230924"}}, which will be substituted in the filename and title)
    "graphs": [
        {
            "filename": "data/output/temperature_{date}.svg",
            "title": "Temperature at {date}",
            "size": [16, 8],
            "show_legend": "True",
            "X-axis": {
                "title": "Time",
                "range": [0, 24],
                "show_grid": "True"
            },
            "Y-axis": {
                "title": "Temperature (℃)",
                "range": [0, 25],
                "range_autoreduce": "False",
                "range_autoincrease": "True",
                "range_padding": 0.1,
                "show_grid": "True"
            },
            "series": [
                {
                    "label": "Sensor 1",
                    "x": {
                        "column": "Time"
                    },
                    "y": {
                        "column": "Sensor1"
                    }
                },
                {
                    "label": "Sensor 2",
                    "x": {
                        "column": "Time"
                    },
                    "y": {
                        "column": "Sensor2"
                    }
                }
            ]
        }
    ],
