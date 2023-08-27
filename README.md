# data_transformation
A python data transformation framework for data wizardry, as well as ordinary data transformation.

## Purpose
Standardize and streamline the approach to reading data files, processing them in some way, then writing modified output files.

## Method
Input file(s), Transform file and Output file(s) are specified as command line parameters

Input and Output files can optionally be specified in the Transform file

The Transform file is a json file that defines how to transform the data from the input file(s) as well as what data to write to the output file(s).

The transformation file has three sections:
- import - defines which transform function(s) to import
- input_files - not used yet, but will be used later to define field prefix for input files and maybe fixed input file name(s)
- transformations - defines how to apply the transform functions and what to call the output fields
  - input - which input fields to send to the transform function
  - function - which transform function to use
  - output - what to call the output fields produced by the transform function
- output_files - a list of output files to produce, each of them have these attributes
  - filename - specifies the filename of the output file. Can be overridden with command line param --output
  - fields - which fields (from either input files or the transform function) to write to the output file

# Examples
## Extract one field from the CSV input file
This example doesn't do any transformation at all, it simply reads the input file (with name and address) and creates a new output file with only one field (address)
The transform file is: `data/sample/transform_files/extract_address.json`

`$ python transform.py --input data/sample/input/names_and_addresses.csv --output data/sample/output/extracted_addresses.csv --transform data/sample/transform_files/extract_address.json`

`Writing output to 'data/sample/output/extracted_addresses.csv' ['address']`

## Split name in first_name and last_name, and split address in street_name, house_number and suffix
This is a good example of how to sanitize address lists. The input field has a name field, with both first and last name and an address field with the complete address Norwegian addresses are used here, and while some are written correctly, others are written in an unstandard way, which happens often when people enter their own address on forms.

Here, two transform functions are used, one for name and one for address. Also, tree separate output files are produced. This could be used for input into different systems, for example.

All the output filenames are defined in the transform file, so we don't need a command line argument for output files.

`$ transform.py --input data/sample/input/names_and_addresses.csv --transform data/sample/transform_files/split_name_and_address.json`

`Writing output to 'data/sample/output/names_and_addresses_split.csv' ['name', 'first_name', 'last_name', 'address', 'address_street', 'address_house_number', 'address_suffix', 'postal_code', 'city']`

`Writing output to 'data/sample/output/name_only.csv' ['first_name', 'last_name']`

`Writing output to 'data/sample/output/address_only.csv' ['address', 'address_street', 'address_house_number', 'address_suffix', 'postal_code', 'city']`

