import matplotlib.pyplot as plt
from func.shared import replace_placeholders # function to replace {var} placeholders in strings with values from a dictionary, used for file names, titles and series labels in graphs
from func.shared import verify_data, check_data_source_and_entry, split_data_and_metadata

def generate_graphs(input_data, graph_settings, quiet=False):
    """
    Generate Graph

    This function generates a graph from the data data based on the specified
    graph settings from the transform file.

    Args:
        input_data (dict): The main_data dict (with keys "data" and "metadata")
        graph_settings (list): List of graph settings from the transform file.
        quiet (bool): If True, suppress all output messages.
    """
    existing_data, existing_metadata = split_data_and_metadata(verify_data(input_data))
    # The metadata is produced by the transformations. We need this to get variable_substitutions for the graph file names, titles and series labels.

    for graph_id, graph_setting in enumerate(graph_settings, start=1):
        # NOTE: Adding breakpoints within the plotting (between any of the plt. assignments) will cause the graph not to be plotted correctly and give the following error:
        # Backend TkAgg is interactive backend. Turning interactive mode on.
        
        # check the validify of the input section of the transform file
        input_section = check_data_source_and_entry(existing_data, graph_setting.get("input"), section=f"graphs")
        # if the above didn't fail (which would have exited the program), then the input section is valid, and these are safe to use:
        data_source = input_section['data_source']
        data_entry = input_section['data_entry']

        # get graph properties (e.g. size, title, what data to use on x and y, etc.)
        graph_properties = graph_setting['properties']

        # get input data as defined in graph config (tranform file)
        data_for_this_graph = existing_data.get(data_source).get(data_entry)
        metadata_for_this_graph = ((existing_metadata.get(data_source) or {}).get(data_entry) or {})
        # needed to add the "or {}" to avoid an error when metadata_for_this_graph is None

        variable_substitutions = metadata_for_this_graph.get('variable_substitution') or {}
        # example:
        # variable_substitutions = {"temperature": "25"}
        # graph_title = "Graph of something at {temperature} degrees Celsius"
        # print(replace_placeholders(graph_title, variable_substitutions))
        # output: "Graph of something at 25 degrees Celsius"

        output_section = graph_setting.get("output") or {}
        graph_filename = replace_placeholders(output_section.get("filename"), variable_substitutions)
        if not graph_filename:
            print(f"Filename not defined for graph #{graph_id} -- skipping graph generation")
            continue

        # check graph definition
        for graph_definition_key in ['title', 'series']:
            graph_definition_value = graph_properties.get(graph_definition_key)
            if not graph_definition_value:
                print(f"{graph_definition_key} not defined for graph #{graph_id} -- skipping graph generation")
                continue

        print()
        graph_title = replace_placeholders(graph_properties['title'], variable_substitutions)
        print(f"Generating graph #{graph_id} ({graph_title})")
        # NOTE: rcParams must be set before creating the figure, otherwise it will have no effect
        plt.rcParams['figure.figsize'] = graph_properties.get('size') or [16, 8]
        plt.title(
            graph_title, 
            fontsize = graph_properties.get('title_fontsize') or 'large',
            loc = graph_properties.get('title_loc') or 'center',
            fontweight = graph_properties.get('title_fontweight') or 'bold'
        )

        # check if X-axis and Y-axis are defined, or provide default values
        for graph_definition_key in ['X-axis', 'Y-axis']:
            axis_definition = graph_properties.get(graph_definition_key)
            if not axis_definition:
                # create an empty one
                graph_properties[graph_definition_key] = {}
            axis_title = replace_placeholders(axis_definition.get('title'), variable_substitutions)
            # set axis titles if defined
            if axis_title:
                if graph_definition_key == 'X-axis':
                    plt.xlabel(axis_title)
                else:
                    plt.ylabel(axis_title)

        series_labels = []
        # loop through series and plot the lines
        for line_id, line in enumerate(graph_properties['series']):
            # get x and y columns

            x_column_name = line.get('x').get('column')
            if not x_column_name:
                print(f"X-axis column not defined for series #{line_id}")
            elif not x_column_name in data_for_this_graph.columns:
                print(f"X-axis column '{x_column_name}' not found in output data")
                print(f"Available columns: {list(data_for_this_graph.columns)}")
                x_column_name = None
            else:
                # get series data
                x_column_data = data_for_this_graph[x_column_name]

            y_column_name = line.get('y').get('column')
            if not y_column_name:
                print(f"Y-axis column not defined for series #{line_id}")
            elif not y_column_name in data_for_this_graph.columns:
                print(f"Y-axis column '{y_column_name}' not found in output data")
                print(f"Available columns: {list(data_for_this_graph.columns)}")
                y_column_name = None
            else:
                # get series data
                y_column_data = data_for_this_graph[y_column_name]

            if not x_column_name or not y_column_name:
                print(f"Skipping series #{line_id}")
                continue

            # get series title
            line_label = replace_placeholders(line.get('label'), variable_substitutions)
            if not line_label:
                line_label = y_column_name # use y column name as series title if not defined

            series_labels.append(line_label)

            # plot series
            plt.plot(x_column_data, y_column_data, label=line_label)

        show_legend = False
        # show a legend on the plot
        if graph_properties.get('show_legend'):
            if graph_properties['show_legend'] in ['True', 'true', '1', 'yes', 'Yes', 'YES', 'y', 'Y']:
                show_legend = True

        if show_legend:
            plt.legend(series_labels)

        # save graph to file
        print(f"Saving graph #{graph_id} ({graph_title}) to file '{graph_filename}'")
        plt.savefig(graph_filename)


"""
# Define a function to process keys
def process_key(key, value):
    print(f"Processing key: {key}, Value: {value}")

# Your dictionary
data_dict = {
    "temp_01": "Value1",
    "temp_02": "Value2",
    "temp_03": "Value3",
    "other_key": "OtherValue"
}

# Your key pattern with placeholders
key_pattern = "temp_{*}"

# Check if the placeholder exists in the pattern
if "{*}" in key_pattern:
    # Get the base part of the pattern (before "{*}")
    base_pattern, placeholder = key_pattern.split("{*}")
    
    matching_keys = [key for key in data_dict.keys() if key.startswith(base_pattern)]
    
    if matching_keys:
        for key in matching_keys:
            # Process each matching key and its corresponding value
            process_key(key, data_dict[key])
    else:
        print("No keys matching the pattern found.")
else:
    # If there's no placeholder, process the single key
    if key_pattern in data_dict:
        process_key(key_pattern, data_dict[key_pattern])
    else:
        print("Key not found in the dictionary.")

"""
