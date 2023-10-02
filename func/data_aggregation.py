
def aggregate_data(aggregated_data, key, new_val, func=None, default=None):
    """
    Collect data into the specified structure and increment counters for valid records.
    Args:
        aggregated_data: The data structure to aggregate the data into.
        key: The key to use for the data structure.
        new_val: The value to add to the data structure.
        func: The function to use for aggregating the data. Can be a string (e.g. "+", "-", "*", "/") or a function (e.g. int.__add__, int.__sub__, int.__mul__, int.__div__).
        default: The default value to use for the key if it doesn't exist yet.
    Returns:
        aggregated_data: The updated data structure.
    """
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


def get_aggregated_max(aggregated_data, key):
    if key is None: # no key specified, get the most used key for all keys
        # get the key with the highest value
        key_max = max(aggregated_data, key=aggregated_data.get)
        #get the total of all values
        total = sum(aggregated_data.values())
        #get the percentage of the max value
        percentage = aggregated_data[key_max] / total * 100
    else:
        if not type(key) is tuple:
            key = (key,)
        key_max = None
        total = None
        percentage = None
        # key can be a tuple of any length
        # aggregated data is a dict where the keys are tuples of the same length as key or longer
        # if they're longer, we need to check if the first part of the key matches
        # if they're the same length, we do the same
        # if they're longer, they won't match
        tmp_dict = {}
        for agg_key, agg_val in list(aggregated_data.items()):
            if len(agg_key) >= len(key): # search for part of the key
                if agg_key[:len(key)] == key: # matches the first part of the key
                    tmp_dict = aggregate_data(tmp_dict, key, agg_val, func="+")
        if len(tmp_dict) > 0:
            key_max, total, percentage = get_aggregated_max(tmp_dict, None)
    return key_max, total, percentage

#a = {(1, 0.33): 6, (2, 0.33): 1, (3, 2): 13, (0, 0.2): 100, (1, 0.5): 2, (1, 1): 7}
#print(get_most_used(a, (1,)))
#print(get_most_used(a, (1, 0.33)))
#print()