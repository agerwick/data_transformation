from func.shared import replace_placeholders # function to replace {var} placeholders in strings with values from a dictionary, used for file names, titles and series labels in graphs

def generate_graphs(graphs, output_data, metadata={}):
    """
    Generate Graphs from Output Data.

    This function generates graphs from the output data based on the specified
    graph definitions in the transform file.

    Args:
        graphs (list): A list of graph definitions from the transform file.
        output_data (DataFrame): The output data from the transformations.
        metadata (dict): The metadata from the transformations. (we need this to get variable_substitutions for the graph file names, titles and series labels)
    """
    import matplotlib.pyplot as plt
    variable_substitutions = metadata.get('variable_substitution') or {}
    # example:
    # variable_substitutions = {"temperature": "25"}
    # graph_title = "The temperature is {temperature} degrees Celsius"
    # print(replace_placeholders(graph_title, variable_substitutions))
    # output: The temperature is 25 degrees Celsius

    for graph_id, graph in enumerate(graphs):
        # NOTE: Adding breakpoints within the plotting (between any of the plt. assignments) will cause the graph not to be plotted correctly and give the following error:
        # Backend TkAgg is interactive backend. Turning interactive mode on.

        graph_id += 1 # start at 1 instead of 0
        # check graph definition
        for graph_definition_key in ['filename', 'title', 'series']:
            graph_definition_value = graph.get(graph_definition_key)
            if not graph_definition_value:
                print(f"{graph_definition_key} not defined for graph #{graph_id} -- skipping graph generation")
                continue

        print()
        graph_title = replace_placeholders(graph['title'], variable_substitutions)
        graph_filename = replace_placeholders(graph['filename'], variable_substitutions)
        print(f"Generating graph #{graph_id} ({graph_title}))")
        # NOTE: rcParams must be set before creating the figure, otherwise it will have no effect
        plt.rcParams['figure.figsize'] = graph.get('size') or [16, 8]
        plt.title(
            graph_title, 
            fontsize = graph.get('title_fontsize') or 'large',
            loc = graph.get('title_loc') or 'center',
            fontweight = graph.get('title_fontweight') or 'bold'
        )

        # check if X-axis and Y-axis are defined, or provide default values
        for graph_definition_key in ['X-axis', 'Y-axis']:
            axis_definition = graph.get(graph_definition_key)
            if not axis_definition:
                # create an empty one
                graph[graph_definition_key] = {}
            axis_title = replace_placeholders(axis_definition.get('title'), variable_substitutions)
            # set axis titles if defined
            if axis_title:
                if graph_definition_key == 'X-axis':
                    plt.xlabel(axis_title)
                else:
                    plt.ylabel(axis_title)

        series_labels = []
        # loop through series and plot the lines
        for line_id, line in enumerate(graph['series']):
            # get x and y columns

            x_column_name = line.get('x').get('column')
            if not x_column_name:
                print(f"X-axis column not defined for series #{line_id}")
            elif not x_column_name in output_data.columns:
                print(f"X-axis column '{x_column_name}' not found in output data")
                print(f"Available columns: {list(output_data.columns)}")
                x_column_name = None
            else:
                # get series data
                x_column_data = output_data[x_column_name]

            y_column_name = line.get('y').get('column')
            if not y_column_name:
                print(f"Y-axis column not defined for series #{line_id}")
            elif not y_column_name in output_data.columns:
                print(f"Y-axis column '{y_column_name}' not found in output data")
                print(f"Available columns: {list(output_data.columns)}")
                y_column_name = None
            else:
                # get series data
                y_column_data = output_data[y_column_name]

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
        if graph.get('show_legend'):
            if graph['show_legend'] in ['True', 'true', '1', 'yes', 'Yes', 'YES', 'y', 'Y']:
                show_legend = True

        if show_legend:
            plt.legend(series_labels)

        # save graph to file
        print(f"Saving graph #{graph_id} ({graph_title}) to file '{graph_filename}'")
        plt.savefig(graph_filename)

