import pandas as pd 
import numpy as np 
import sys  
import time 
import os 


from util.graphdb_base import GraphDBBase 
from util.string_util import strip 



class YoochooseImporter(GraphDBBase): 
    def __init__(self, argv): 
        super().__init__(command=__file__, argv=argv) 


    def import_session_data(self, file):
        with self._driver.session() as session: 
            self.execute_without_exception("CREATE CONSTRAINT ON (s:Session) ASSERT s.sessionId IS UNIQUE")
            self.execute_without_exception("CREATE CONSTRAINT ON (i:Item) ASSERT i.itemId IS UNIQUE")
            dtype = {"sessionID": np.int64, "itemID": np.int64, "category": object}
            j = 0 
            for chunk in pd.read_csv(file,
                                     header=0, 
                                     dtype=dtype, 
                                     names=["sessionID", "timestamp", "itemID", "category"],
                                     parse_dates=['timestamp'],
                                     chunksize=10 ** 6):
                df = chunk 
                #print(df)
                tx = session.begin_transaction() 
                i = 0 
                query = """
                    MERGE (session:Session {sessionId: $sessionId})
                    MERGE (item: Item {itemId: $itemId, category: $category})
                    CREATE (click:Click {timestamp: $timestamp})
                    CREATE (session)-[:CONTAINS]->(click)
                    CREATE (click)-[:IS_RELATED_TO]->(item)
                """

                for row in df.itertuples():
                    try: 
                        timestamp = row.timestamp 
                        session_id = row.sessionID 
                        category = strip(row.category)
                        item_id = row.itemID 
                        tx.run(query, {"sessionId": session_id, 
                                       "itemId": item_id, 
                                       "timestamp": str(timestamp), 
                                       "category": category})
                        i += 1
                        j += 1
                        if i == 10000:
                            tx.commit() 
                            print(j, " lines processed")
                            i = 0
                            tx = session.begin_transaction()
                    except Exception as e: 
                        print(e, row)
                tx.commit() 
                print(j, " lines processed")
            print(j, " lines processed")


    def import_buys_data(self, file): 
        with self._driver.session() as session:
            dtype = {"sessionID": np.int64, "itemID": np.int64, "price":float, "quantity": int}
            j = 0 
            for chunk in pd.read_csv(file,
                                  header = 0, 
                                  dtype=dtype, 
                                  names=['sessionID', 'timestamp', 'itemID', 'price', 'quantity'],
                                  parse_dates=['timestamp'],
                                  chunksize= 10 ** 6):
                df = chunk 
                tx = session.begin_transaction()
                i = 0
                query = """ 
                    MATCH (session:Session {sessionId: $sessionId})
                    MATCH (item: Item {itemId: $itemId})
                    CREATE (buy:Buy:Click {timestamp: $timestamp})
                    CREATE (session)-[:CONTAINS]->(buy)
                    CREATE (buy)-[:IS_RELATED_TO]->(item)
                """
                for row in df.itertuples(): 
                    try: 
                        timestamp = row.timestamp 
                        session_id = row.sessionID 
                        item_id = row.itemID 
                        tx.run(query, {"sessionId": session_id, "itemId": item_id, "timestamp": str(timestamp)})
                        i += 1
                        j += 1 
                        if i == 10000:
                            tx.commit() 
                            print(j, "lines processed")
                            i = 0 
                            tx = session.begin_transaction() 
                    except Exception as e: 
                        print(e, row) 
                tx.commit() 
                print(j, "lines processed")
            print(j, " lines processed")

if __name__ == '__main__':
    start = time.time() 
    importer = YoochooseImporter(sys.argv[1:])
    base_path = importer.source_dataset_path
    if not base_path: 
        base_path = "../../../dataset/yoochoose"
    #importer.import_session_data(file=os.path.join(base_path, "yoochoose-clicks.dat"))
    importer.import_buys_data(file=os.path.join(base_path, "yoochoose-buys.dat"))
    end= time.time() - start 
    print("Time to complete: ", end)