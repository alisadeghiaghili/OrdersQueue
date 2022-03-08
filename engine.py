import sqlalchemy as sa
import pandas as pd

class DataProviderMaker():
    def __init__(self):
        config = 'mssql+pyodbc://172.16.1.119/Future_DM?driver=SQL+Server+Native+Client+11.0'
        self.engine = sa.create_engine(config)
    def make_data_table(self, query):
        return pd.read_sql_query(query , self.engine)
    def make_data_table_striped(self, query):
        table = self.make_data_table(query)
        orders_obj = table.select_dtypes(['object'])
        table[orders_obj.columns] = orders_obj.apply(lambda x: x.str.strip())
        return table
def extract_one_side(table, side):
    return table[table.OrderType == side].reset_index(drop = True)
def make_empty_queue(table):
    queue = table.copy(deep=True)
    queue['QueuePosition'] = 999
    queue = queue.loc[queue.index.repeat(1000)].reset_index(drop = True)
    return queue
def extract_column_values_uniqued_sorted(table, columnName):
    return pd.Series(table[columnName].unique()).sort_values().to_list()
