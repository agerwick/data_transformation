"""
This module contains functions for aggregating data into a dict and getting info from that dict

collect_data(aggregated_data, key, new_val, func=None, default=None)
- aggregated_data: (dict) The data structure to aggregate the data into.
- key: (tuple or immutable var) The key to use for the data structure.
- new_val: (int, float, str, etc.) The value to add to the data structure. Normally 1, but can be any value.
- func: (str or function) The function to use for aggregating the data. Can be a string (e.g. "+", "-", "*", "/") or a function (e.g. int.__add__, int.__sub__, int.__mul__, int.__div__). Default is "+", which is only valid for int and float values.
- default: (int, float, str, etc.) The default value to use for the key if it doesn't exist yet. Default is 0 for int and 0.0 for float.

get_max(aggregated_data, key=None)
- aggregated_data: (dict) The data structure to aggregate the data into.
- key: (tuple or immutable var) The key to use for the data structure. Default is None, which means all keys. If your keys are tuples, you can get all keys that start with a certain tuple by specifying that tuple as the key.

get_subgroup(aggregated_data, key=())
- aggregated_data: (dict)
- key: (tuple or immutable var)

usage:
import random
aggregated_data = {}
for i in range(10): # normally you'd iterate over a list of records, but this is just an example
    aggregated_data = aggregate_data(aggregated_data, f"key{i}", 1) # add 1 to the value for key "key{i}"
    aggregated_data = aggregate_data(aggregated_data, f"key{i}", round(random.random()*10)) # add a random value to make it interesting - normally you would just add one every time you see a record, if that's what you're iterating over

print(aggregated_data)
# result: {'key0': 6, 'key1': 6, 'key2': 10, 'key3': 9, 'key4': 4, 'key5': 4, 'key6': 6, 'key7': 3, 'key8': 1, 'key9': 10}
print(get_max(aggregated_data))
result: {'key': 'key2', 'count': 10, 'count_max': 59, 'percentage': 16.94915254237288}
"""

def collect_data(aggregated_data, key, new_val, func=None, default=None):
    from func.shared import stack_trace
    import sys
    """
    Collect data into the specified structure and increment counters for valid records.
    Args:
        aggregated_data: The data structure to aggregate the data into.
        key: The key to use for the data structure.
        new_val: The value to add to the data structure. Normally 1, but can be any value.
        func: The function to use for aggregating the data. Can be a string (e.g. "+", "-", "*", "/") or a function (e.g. int.__add__, int.__sub__, int.__mul__, int.__div__).
        default: The default value to use for the key if it doesn't exist yet.
    Returns:
        aggregated_data: The updated data structure.
    """
    if not isinstance(key, tuple):
        key = (key,)
    if key=="len()":
        print("ERROR: why is len() being used as a key?")
        stack_trace()
        sys.exit()
    if func == "=":
        func = lambda x, y: y
    elif func == "+":
        func = lambda x, y: x + y
    elif func == "-":
        func = lambda x, y: x - y
    elif func == "*":
        func = lambda x, y: x * y
    elif func == "/":
        func = lambda x, y: x / y
    elif func is None: # no function specified, set default based on type
        if type(new_val) is int or type(new_val) is float:
            func = lambda x, y: x + y
    if default is None:
        if type(new_val) is int:
            default = 0
        elif type(new_val) is float:
            default = 0.0
    if default is None:
        raise Exception(f"Unsupported default type: {type(default).__name__} - you need to specify your own default value (default=) and function (func=).")
    if new_val is None:
        raise Exception(f"Unsupported value type: {type(new_val).__name__} - you need to specify your own default value (default=) and function (func=).")
    if func is None:
        raise Exception(f"Function must be specified for this value type ({type(new_val).__name__}) - use for ecxample func=\"add\" or suppy your own func=int.__add__.")
    
    if not type(aggregated_data) is dict:
        aggregated_data = {}
    old_val = aggregated_data.get(key) or default
    aggregated_data[key] = func(old_val, new_val)
    return aggregated_data


# get_max(aggregated_data, key=None):
# returns a tuple of (key, value, total, percentage)

# call it with only aggregated_data to get the highest of all keys
# call it with a key to get the highest of all keys that start with that key

# examples of get_max:
# aggregated_data = {("file1", "sheet1", "col1"): 3, ("file1", "sheet1", "col2"): 2, ("file2", "sheet1", "col1"): 5, ("file1", "sheet2", "col3"): 3, ("file2", "sheet9", "col9"): 1}

# get the highest of all keys
# get_max(aggregated_data)
# returns: {"key": ('file2', 'sheet1', 'col1'), "count": 5, "count_max": 14, "percentage": 35.714285714285715}

# get the highest of all keys that start with ("file1",)
# get_max(aggregated_data, ("file1",)) 
# returns: {"key": ('file1', 'sheet1', 'col1'), "count": 3, "count_max": 8, "percentage": 37.5}

# aggregated_data = {(1, 0.33): 6, (2, 0.33): 1, (3, 2): 13, (0, 0.2): 100, (1, 0.5): 2, (1, 1): 7}

# get_max(aggregated_data)
# returns: {"key": (0, 0.2), "count": 100, "count_max": 129, "percentage": 77.51937984496124}

# get_max(aggregated_data, (1,))
# returns: {"key": (1, 1), "count": 7, "count_max": 15, "percentage": 46.666666666666664}

def get_max(aggregated_data, key=None):
    if key is None:
        key = ()
    if not isinstance(key, tuple):
        key = (key,)
    filtered_data = get_subgroup(aggregated_data, key)
    if filtered_data:
        key_max = max(filtered_data, key=filtered_data.get)
        total = sum(filtered_data.values())
        percentage = filtered_data[key_max] / total * 100
    else:
        key_max = None
        total = 0
        percentage = 0
    # unwrap key_max if it's a singleton tuple
    if len(key_max) == 1:
        key_output = key_max[0]
    else:
        key_output = key_max
    return {"key": key_output, "count": aggregated_data.get(key_max), "count_max": total, "percentage": percentage}


"""
d = {("file1", "sheet1", "col1"): 3, ("file1", "sheet1", "col2"): 2, ("file2", "sheet1", "col1"): 5, ("file1", "sheet2", "col3"): 3, ("file2", "sheet9", "col9"): 1}
get_subgroup(d, ("file1",))
# or
get_subgroup(d, "file1")
# returns: {('file1', 'sheet1', 'col1'): 3, ('file1', 'sheet1', 'col2'): 2, ('file1', 'sheet2', 'col3'): 3}
get_subgroup(d, ("file1","sheet1"))
# returns: {('file1', 'sheet1', 'col1'): 3, ('file1', 'sheet1', 'col2'): 2}
get_subgroup(d, ())
# returns the input dict (d)
"""
def get_subgroup(aggregated_data, key=(), sort=False, isolate_subgroup=False, unwrap_singleton_tuples=False, unwrap=False):
    if not isinstance(key, tuple):
        key = (key,)
    # if key is empty, return the whole dict
    # otherwise, return only the key:value pairs where the key (k) starts with the input key
    filtered_data = {k: v for k, v in aggregated_data.items() if k[:len(key)] == key}
    if sort:
        if sort == "highest_value":
            # sort the dict by value, descending
            filtered_data = dict(sorted(filtered_data.items(), key=lambda item: item[1], reverse=True))
        elif sort == "lowest_value":
            # sort the dict by value, ascending
            filtered_data = dict(sorted(filtered_data.items(), key=lambda item: item[1], reverse=False))
        else:
            # this is a developer error, so we should raise an exception
            raise Exception(f"Unsupported sort type: {sort}\nSupported sort types are: \"highest_value\", \"lowest_value\"")
    if isolate_subgroup and len(key) > 0:
        # return the dict with the filtered part of the key removed
        # filtered_data = {("file1", "sheet1", "col1"): 3, ("file1", "sheet1", "col2"): 2, ("file1", "sheet2", "col3"): 3}
        # key: ("file1")
        # result: {('sheet1', 'col1'): 3, ('sheet1', 'col2'): 2, ('sheet2', 'col3'): 3}
        # key: ("file1", "sheet1")
        # result: {"col1": 3, "col2": 2}
        # filtered_data = {k[1:] if len(k) > 1 else k: v for k, v in filtered_data.items()}
        filtered_data = {k[len(key):] if k[:len(key)] == key else k: v for k, v in filtered_data.items() if k[:len(key)] == key}
    if unwrap:
        # unwrap all tuples in the key to form a dict
        # example result: {
        #   "file1": { 
        #       "sheet1": {"col1": 3, "col2": 2}, 
        #       "sheet2": {"col3": 3}
        #       },
        #   "file2": {
        #       "sheet1": {"col1": 5}, 
        #       "sheet9": {"col9": 1}
        #       }
        # }
        filtered_data = unwrap_tuples(filtered_data)
    elif unwrap_singleton_tuples:
        # if the key is a singleton tuple (meaning just one element), then unwrap it
        # for example, if the key is ("file1",)
        # then the result will be {"sheet1": {"col1": 3, "col2": 2}, "sheet2": {"col3": 3}}
        filtered_data = {k[0] if len(k) == 1 else k: v for k, v in filtered_data.items()}
    return filtered_data

def get_subgroup_dict(aggregated_data, key=(), sort=False):
    return get_subgroup(aggregated_data, key=key, sort=sort, isolate_subgroup=False, unwrap=True)

"""
a = {(1, 0.33): 6, (2, 0.33): 1, (3, 2): 13, (0, 0.2): 100, (1, 0.5): 2, (1, 1): 7}
# example of get_aggregated_subgroup:
(max_key, max_value, total, percentage) = get_aggregated_max(a)
print(max_key, max_value, total, percentage)
# returns: (0, 0.2) 100 129 77.51937984496125
print(get_subgroup(a, (1,)))
# returns: {(1, 0.33): 6, (1, 0.5): 2, (1, 1): 7}
print(get_max(a, (1,)))
# returns: ((1, 1), 7, 15, 46.666666666666664
print(get_subgroup(a, (1, 0.33)))
# returns: {(1, 0.33): 6}
print(get_max(a, (1,0.33)))
# returns: ((1, 0.33), 6, 6, 100.0)
print()

b = {("file1", "sheet1", "col1"): 3, ("file1", "sheet1", "col2"): 2, ("file2", "sheet1", "col1"): 5, ("file1", "sheet2", "col3"): 3, ("file2", "sheet9", "col9"): 1}
print(get_aggregated_max(b))
# returns (('file2', 'sheet1', 'col1'), 5, 14, 35.714285714285715)
print(get_aggregated_max(b, ('file1',)))
# returns (('file1', 'sheet1', 'col1'), 3, 8, 37.5)
print(get_aggregated_max(b, ('file1','sheet1')))
# returns (('file1', 'sheet1', 'col1'), 3, 5, 60.0)
print(get_aggregated_max(b, ('file1','sheet1','col1')))
# returns (('file1', 'sheet1', 'col1'), 3, 3, 100.0)
"""

def unwrap_tuples(data):
    unwrapped_data = {}
    for k, v in data.items():
        if len(k) == 0:
            unwrapped_data[k] = v # return only the value
        if len(k) == 1:
            unwrapped_data[k[0]] = v
        else:
            first_element = k[0]
            remaining_tuple = k[1:]
            if first_element not in unwrapped_data:
                unwrapped_data[first_element] = {}
            unwrapped_data[first_element].update(unwrap_tuples({remaining_tuple: v}))
    return unwrapped_data
# data = {("file1", "sheet1", "col1"): 3, ("file1", "sheet1", "col2"): 2, ("file1", "sheet2", "col3"): 3}
# unwrapped_data = unwrap_tuples(data)
# print(unwrapped_data)
# # {
# #     "file1": {
# #         "sheet1": {
# #             "col1": 3,
# #             "col2": 2
# #         },
# #         "sheet2": {
# #             "col3": 3
# #         }
# #     }
# # }
