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
- **input_files** - a list of input files to read from, each with these attributes
  - **filename** - specified the filename of the input file. Can be overridden with command line param --input
  - **field_prefix** - an optional prefix to add to each field name for this file. Used to avoid two fields with the same name.
  - **field_suffix** - an optional suffix to add to each field name 
  - **rename_field** - contains a list of key/value pairs where the key is the field name in this file and the value is the field name to use instead. This is used in cases where only a few fields have the same name as previously loaded files
- **output_files** - a list of output files to produce, each with these attributes
  - **filename** - specifies the filename of the output file. Can be overridden with command line param --output
  - **fields** - which fields (from either input files or the transform function) to write to the output file

# Examples
## Extract one field from the CSV input file
This example doesn't do any transformation at all, it simply reads the input file (with name and address) and creates a new output file with only one field (address).

```
$ python transform.py --input data/sample/input/names_and_addresses.csv --output data/sample/output/extracted_addresses.csv --transform data/sample/transform_files/extract_address.json

Input file #1: data/sample/input/names_and_addresses.csv (from command line)

Output file #1: data/sample/output/extracted_addresses.csv (from command line)

Input fields from file #1: name, address

All input fields:
name, address

No transformations specified in transform file. Output data is the same as input data.

Writing to output file #1 ['address']
```

## Split name in first_name and last_name, and split address in street_name, house_number and suffix
This is a good example of how to sanitize address lists. The input field has a name field, with both first and last name and an address field with the complete address Norwegian addresses are used here, and while some are written correctly, others are written in an unstandard way, which happens often when people enter their own address on forms.

Here, two transform functions are used, one for name and one for address. Also, tree separate output files are produced. This could be used for input into different systems, for example.

All the output filenames are defined in the transform file, so we don't need a command line argument for output files.

```
$ python transform.py --input data/sample/input/names_and_addresses.csv --transform data/sample/transform_files/split_name_and_address.json

Input file #1: data/sample/input/names_and_addresses.csv (from command line)

Output file #1: data/sample/output/names_and_addresses_split.csv (from transform file)
Output file #2: data/sample/output/name_only.csv (from transform file)
Output file #3: data/sample/output/address_only.csv (from transform file)

Input fields from file #1: name, address

All input fields:
name, address

Output fields produced by transform functions:
address_house_number, address_street, address_suffix, city, first_name, last_name, postal_code

Writing to output file #1 ['name', 'first_name', 'last_name', 'address', 'address_street', 'address_house_number', 'address_suffix', 'postal_code', 'city']
Writing to output file #2 ['first_name', 'last_name']
Writing to output file #3 ['address', 'address_street', 'address_house_number', 'address_suffix', 'postal_code', 'city']
```

## Merge/join two files
For now, the only way to merge two files is to merge by row number, so the first customer gets the first customer number and so on. Later, we'll introduce more complex ways of merging files.
This example defines both input and output files in the transform file, so we only need to specify the transform file.
We have one file with a list of customers and addresses, and another file with a list of customer numbers. Each row corresponds to the same row in the other file. Just to show what happens, the file customer_number.csv has one moe entry than names_and_addresses.csv - the result is one blank line with only a customer number.
We also have a second file with a different set of customer numbers, but with the same field name in the header. If you try to load two files with the same field names, only the first one will be used and any subsequent field with an already existing name will be discarded. In order to avoid this, we have 3 ways of renaming fields; adding a prefix to all field names, adding a suffix to all field names, or renaming individual fields. In this example we've renamed the one field with the same name.
```
$ python transform.py --transform data/sample/transform_files/join_two_files.json

Input file #1: data/sample/input/names_and_addresses.csv (from transform file)
Input file #2: data/sample/input/customer_numbers.csv (from transform file)
Input file #3: data/sample/input/customer_numbers2.csv (from transform file)

Output file #1: data/sample/output/cutomer_id_names_addresses.csv (from transform file)

Input fields from file #1: name, address
Input fields from file #2: customer_id
Input fields from file #3: customer_id2

All input fields:
address, customer_id, customer_id2, name

No transformations specified in transform file. Output data is the same as input data.

Writing to output file #1 ['customer_id', 'name', 'address']
```
