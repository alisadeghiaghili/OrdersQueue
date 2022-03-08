from sql import queries
from sql.engine import *
import numpy as np
import pandas as pd

orderDataProvider = DataProviderMaker()
orders = orderDataProvider.make_data_table_striped(queries.orderLogsQuery)

buyOrders = extract_one_side(orders, 'B')
del orders

queue = make_empty_queue(buyOrders)
currentRow = 0
for contract in extract_column_values_uniqued_sorted(buyOrders, 'Contract_ID'):
    for time in pd.Series(buyOrders[buyOrders.Contract_ID == contract].LogTime.unique()).sort_values().to_list():
        queueMomentSnapshot = buyOrders[np.logical_and(buyOrders.Contract_ID == contract, buyOrders.LogTime == time)].sort_values(by = ['Price', 'LogID'], ascending = (False, True))
        numOfRows = queueMomentSnapshot.shape[0]
        if currentRow == 1:
            queueMomentSnapshot['QueuePosition'] = range(1, numOfRows + 1)
            queue[currentRow:(currentRow + numOfRows)] = queueMomentSnapshot[:]
        else:
            queueMomentSnapshot = pd.concat([queueMomentSnapshot, queuetMinusOne]).sort_values(by = ['Price', 'LogID'], ascending = (False, True))
            numOfRows = queueMomentSnapshot.shape[0]
            queue[currentRow:(currentRow + numOfRows)] = queueMomentSnapshot[:]
            
        queuetMinusOne = queueMomentSnapshot.copy(deep=True).reset_index(drop = True)
        tMinusOne = time
        currentRow += queueMomentSnapshot.shape[0]
