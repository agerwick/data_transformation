import pandas as pd
import time

# we have this data (spaced out for readability, truncated for brevity):
sample = """cycle ,voltage  ,current,capacity   ,energy     ,time
1     ,0.0186   ,3.058  ,0.0        ,0.0        ,0.0
1     ,26.9959  ,3.1101 ,0.00740775 ,0.02300869 ,1.0
1     ,26.994   ,3.1175 ,0.01490727 ,0.04636230 ,2.0
1     ,2.7046   ,4.1982 ,26.4918438 ,102.754713 ,4000.0
1     ,2.699    ,4.1982 ,26.4919263 ,102.755060 ,4000.11
2     ,0.0186   ,3.0803 ,0.0        ,0.0        ,0.0
2     ,26.9996  ,3.1324 ,0.0074075  ,0.02317408 ,1.0
2     ,26.994   ,3.1398 ,0.01490708 ,0.04669375 ,2.0
2     ,2.7083   ,4.1982 ,26.4293173 ,102.548913 ,4000.0
2     ,2.699    ,4.1982 ,26.4295427 ,102.549860 ,4000.11
3     ,0.0186   ,3.0952 ,0.0        ,0.0        ,0.0
3     ,26.9959  ,3.1491 ,0.00741055 ,0.02330511 ,1.0
3     ,26.9995  ,3.1547 ,0.01491030 ,0.04694888 ,2.0
3     ,2.7306   ,4.1982 ,26.3841730 ,102.386096 ,4000.0
3     ,2.699    ,4.1982 ,26.3846558 ,102.388123 ,4000.11
"""
# We want to extract the time and capacity columns and make one new column per cycle
"""
from io import StringIO
csvStringIO = StringIO(sample)
columns = [column.strip() for column in sample.strip().split('\n')[0].split(',')]
# columns = ['cycle', 'voltage', 'current', 'capacity', 'energy', 'time']
df = pd.read_csv(csvStringIO, sep=",", header=0, skipinitialspace=True, names=columns)
index_column = 'time'
pivot_column = 'cycle'
data_column = 'capacity'
input_data = {'index': index_column, 'pivot': pivot_column, 'data': data_column}
pivot(df, input_data, None)
"""
# so the data looks like this:
"""
   time  cycle 1 capacity  cycle 2 capacity  cycle 3 capacity
   0.00          0.000000          0.000000          0.000000
   1.00          0.007408          0.007408          0.007411
   2.00          0.014907          0.014907          0.014910
4000.00         26.491844         26.429317         26.384173
4000.11         26.491926         26.429543         26.384656
"""


def pivot(input_data, input_fields, output_fields):
    """
    Pivot a dataframe from long to wide format

    :param input_data: the dataframe to pivot
    :param input_fields: {
        "index": the field to use as the index (x-axis)
        "pivot": the field to use as the pivot (y-axis)
        "data":  the field to use as the data
        }
    :param output_fields: ignored
    :return: a new dataframe
    column names will be index, pivot_1_data, pivot_2_data, etc. where index is the name of the index column, pivot is the name of the pivot column and data is the name of the data column
    For example, if the index column is called 'time', the pivot column is called 'cycle' and the data column is called 'capacity', the columns will be called 'time', 'cycle_1_capacity', 'cycle_2_capacity', etc.
    """

    # get the index, pivot and data column names

    index_column = input_fields["index_column"]
    pivot_column = input_fields["pivot_column"]
    data_column  = input_fields["data_column"]

    # pivot the data
    df = input_data.pivot(index=index_column, columns=pivot_column, values=data_column)

    # rename the columns
    df.columns = [f'{pivot_column} [] {data_column}'.replace("[]", "{}").format(col) for col in df.columns]

    # flatten the header so the index column is a column again
    df = df.reset_index()

    # instruct parent to delete all existing data in the output table before writing the new data returned from this function
    # this is required as the output table is a pivot of the input table, so the output table will have a different number of rows and columns each time this function is run
    metadata = {"clear_input_data": True} 

    return df, metadata
