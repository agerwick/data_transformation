import re
import pandas as pd
from func.shared import check_data_source_and_entry, structure_dataframe

def split_address(input_data, input_section, output_fields):
    """
    Split Address into Street, House Number, Suffix, Postal Code, and City Columns.

    This function takes input data with full address information and splits them into
    separate columns for street name, house number, suffix, postal code, and city.
    The function is specifically designed for Norwegian addresses, but may work for
    other European addresses as well. It will not work for addresses in the US, as
    the US has a different address format.
    The input and output field names are specified to match the field names of the input
    and output DataFrame.

    Args:
        input_data (pd.DataFrame): The input DataFrame containing the address data to be split.
        input_fields (list): A list containing the name of the input field to be split.
        output_fields (list): A list containing the names of the output fields for 
                             street name, house number, suffix, postal code, and city.

    Returns:
        pd.DataFrame: A DataFrame with separate columns for street name, house number,
                      suffix, postal code, and city.

    Raises:
        Exception: If the length of input_fields is not exactly 1 or the length of
                   output_fields is not exactly 5.

    Example:
        input_data:
            address
            Storgata 6 leilighet 3, 0123 Oslo
            Parkveien 45, Seksjon 1, Inng.A, 1337 Sandvika
            Nedre Kirkegate 7B, 5005 Bergen

        input_fields:
            ['address']

        output_fields:
            ['address_street', 'address_house_number', 'address_suffix', 'postal_code', 'city']

        Code:
            import pandas as pd
            import re
            from transform_functions.public.address_norway import split_address
            input_data = pd.DataFrame({
                'address': [
                    'Storgata 6 leilighet 3, 0123 Oslo',
                    'Parkveien 45, Seksjon 1, Inng.A, 1337 Sandvika',
                    'Nedre Kirkegate 7B, 5005 Bergen'
                ]
            })
            input_fields = ['address']
            output_fields = ['address_street', 'address_house_number', 'address_suffix', 'postal_code', 'city']
            split_address(input_data, input_fields, output_fields)

        Returns:
            | address_street       | address_house_number | address_suffix    | postal_code | city     |
            |----------------------|----------------------|-------------------|-------------|----------|
            | Storgata             | 6                    | leilighet 3       | 0123        | Oslo     |
            | Parkveien            | 45                   | Seksjon 1, Inng.A | 1337        | Sandvika |
            | Nedre Kirkegate      | 7B                   |                   | 5005        | Bergen   |
    """

    input_section = check_data_source_and_entry(input_data, input_section)
    if not input_section:
        print("Exiting...")
        sys.exit(1)

    data_source = input_section['data_source']
    data_entry = input_section['data_entry']
    input_fields = input_section['fields']

    if len(input_fields) != 1:
        raise Exception(f"split_address() requires exactly one input field: address -- however, this was given: {input_fields}")
    if len(output_fields) != 5:
        raise Exception(f"split_address() requires exactly five output fields: address_street, address_house_number, address_suffix, postal_code, city -- however, this was given: {output_fields}")
    output_data = pd.DataFrame()
    address_streets = []
    address_house_numbers = []
    address_suffixes = []
    postal_codes = []
    cities = []
    for _, row in input_data.get(data_source).get(data_entry).iterrows():
        address_parts = row[input_fields[0]].split(',')
        
        if len(address_parts) > 1:
            # everything after last comma is postal code and city
            postal_code_city = address_parts.pop().strip()
            # everything before last comma is street address
            street_address = ','.join(address_parts).strip()

            # Match house number
            match = re.search(r'(?<=[a-zA-Z.] )((\d+(?:\s?\w?))|(\d+(?:(-|\/)\d+)))(?=(?: [a-zA-Z] )?($| |,))', street_address)
            # regex explanation:
            
            # Everything before the house number:
            # (?<=[a-zA-Z.] ) - Positive lookbehind assertion, matches a position that is preceded by a letter or a period, followed by a space. This prevents matching a number at the start of the street name like "7 Juni plassen" or "1884-trappen".

            # The house number(s):
            # (\d+(?:\s?\w?)) - Capturing group that matches one or more digits, optionally followed by a single space and an optional word character. This will capture the house number with optional letter, such as "1", "100", "100a" and even 100 a", but not "100abc", as the latter is not a valid house number.
            # | or
            # (\d+(?:(-|\/)\d+)) - Capturing group that matches one or more digits, followed by a hyphen or slash and one or more digits. This will capture addresses spanning multiple house numbers, such as "1-3", "10/12", "100-102", but not "100a-102b" or "100 a-102 b", or "100abc-102def", as the latter 3 are not valid house numbers. We assume that addresses spanning multiple house numbers do not have a letters, such as "a" or "b".

            # Everything after the house number:
            # (?=(?: [a-zA-Z] )?($| |,))   Positive lookahead assertion, matches a position that is followed by an optional space and a letter, which would indicate the beginning of optional extra info, such as apartment number, unit number or entrance, or the end of the string. Also treats a comma like a space for optional separation. It will thus handle both "Storgata 6 leilighet 3" and "Storgata 6, leilighet 3". Note that if the house number is separated from this suffix by a comma, the comma wil be included in the suffix, but we'll strip that below with .strip(", ").
            
            if match:
                # split into street, house number and suffix
                address_streets.append(street_address[:match.start()].strip())
                address_house_numbers.append(match.group())
                address_suffixes.append(street_address[match.end():].strip(", "))
            else:
                # no house number found: jam it all in street name as a last resort
                address_streets.append(street_address)
                address_house_numbers.append('')
                address_suffixes.append('')
            
            # Split postal code and city
            put_everything_in_city = True
            if ' ' in postal_code_city:
                postal_code, city = postal_code_city.split(' ', 1) # Split on first space
                if re.match(r'^(?:\d{4}|N-\d{4})$', postal_code):
                    # The regex will match valid norwegian postal codes, such as "4790" and "N-4790"
                    # ^: Start of the string.
                    # (?: ... ): A non-capturing group that contains two alternatives separated by a pipe |.
                    # either: \d{4}:   Exactly 4 digits.
                    # or:     N-\d{4}: Literal characters "N-" + exactly 4 digits.
                    # $: End of the string.
                    postal_codes.append(postal_code)
                    cities.append(city)
                    put_everything_in_city = False
            
            if put_everything_in_city:
                postal_codes.append('')
                cities.append(postal_code_city)
        else:
            address_streets.append(address_parts[0].strip())
            address_house_numbers.append('')
            address_suffixes.append('')
            postal_codes.append('')
            cities.append('')

    output_data[output_fields[0]] = address_streets
    output_data[output_fields[1]] = address_house_numbers
    output_data[output_fields[2]] = address_suffixes
    output_data[output_fields[3]] = postal_codes
    output_data[output_fields[4]] = cities

    metadata = {} 
    return structure_dataframe(output_data, input_data), metadata
