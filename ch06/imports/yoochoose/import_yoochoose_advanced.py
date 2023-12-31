import pandas as pd
import numpy as np
import sys
import time
import operator
import os

from util.graphdb_base import GraphDBBase
from util.string_util import strip


class YoochooseImporter(GraphDBBase):

    def __init__(self, argv):
        super().__init__(command=__file__, argv=argv)

    def import_session_data(self, file):
        dtype = {"sessionID": np.int64, "itemID": np.int64, "category": object}
        j = 0
        sess_clicks = {}
        for chunk in pd.read_csv(file,
                                 header=0,
                                 dtype=dtype,
                                 names=['sessionID', 'timestamp', 'itemID', 'category'],
                                 parse_dates=['timestamp'],
                                 chunksize=10 ** 6):
            df = chunk
            for row in df.itertuples():
                timestamp = time.mktime(row.timestamp.timetuple())
                session_id = row.sessionID
                category = strip(row.category)
                item_id = row.itemID

                item = item_id, category, timestamp
                j += 1
                if session_id in sess_clicks:
                    sess_clicks[session_id] += [item]
                else:
                    sess_clicks[session_id] = [item]

            print(j, "lines processed")
        print(j, "lines processed")
        print("total number of sessions", len(sess_clicks))
        # Filter out length <5  sessions
        for s in list(sess_clicks):
            if len(sess_clicks[s]) < 5:
                del sess_clicks[s]

        for i in list(sess_clicks):
            sorted_clicks = sorted(sess_clicks[i], key=operator.itemgetter(2))
            sess_clicks[i] = [{'itemId': c[0], 'category': c[1], 'timestamp': c[2]} for c in sorted_clicks]
            # sess_clicks[i] = sorted_clicks

        print("total number of valid sessions", len(sess_clicks))
        print("start db ingestion")

        with self._driver.session() as session:
            self.executeNoException(session, "CREATE CONSTRAINT ON (s:Session) ASSERT s.sessionId IS UNIQUE")
            #self.executeNoException(session, "CREATE CONSTRAINT ON (i:Item) ASSERT i.itemId IS UNIQUE")

            tx = session.begin_transaction()
            i = 0
            j = 0
            query = """
                MERGE (session:Session {sessionId: $sessionId})
                WITH session
                UNWIND $items as entry
                MERGE (item:Item {itemId: entry.itemId, category: entry.category})
                CREATE (click:Click {timestamp: entry.timestamp})
                CREATE (click)-[:IS_RELATED_TO]->(item)
                CREATE (session)-[:CONTAINS]->(click)
            """
            for session_id in list(sess_clicks):
                try:
                    items = sess_clicks[session_id]
                    tx.run(query, {"sessionId": session_id, "items": items})
                    i += 1
                    j += 1
                    if i == 2000:
                        tx.commit()
                        print(j, "lines processed")
                        i = 0
                        tx = session.begin_transaction()
                    if j >= 10_000:
                        break
                except Exception as e:
                    print(e, session_id)
            try:
                if session.has_transaction():
                    tx.commit()
            except Exception as e:
                pass
                # print(e)
            print(j, "sessions created processed")
            tx.close()
        return sess_clicks

    def import_buys_data(self, file, sess_clicks):
        with self._driver.session() as session:
            dtype = {"sessionID": np.int64, "itemID": np.int64, "price": float, "quantity": int}
            i = 0
            j = 0
            query = """
                MATCH (session:Session {sessionId: $sessionId})
                MATCH (item:Item {itemId: $itemId})
                CREATE (buy:Buy:Click {timestamp: $timestamp})
                CREATE (session)-[:CONTAINS]->(buy)
                CREATE (buy)-[:IS_RELATED_TO]->(item)
            """
            tx = session.begin_transaction()
            for chunk in pd.read_csv(file,
                                     header=0,
                                     dtype=dtype,
                                     names=['sessionID', 'timestamp', 'itemID', 'price', 'quantity'],
                                     parse_dates=['timestamp'],
                                     chunksize=10 ** 6):
                df = chunk
                
                for row in df.itertuples():
                    try:
                        timestamp = time.mktime(row.timestamp.timetuple())
                        session_id = row.sessionID
                        item_id = row.itemID
                        if session_id in sess_clicks:
                            tx.run(query, {"sessionId": session_id, "itemId": item_id, "timestamp": timestamp})
                            i += 1
                        j += 1
                        if i == 2000:
                            tx.commit()
                            print(j, "lines processed")
                            i = 0
                            tx = session.begin_transaction()
                    except Exception as e:
                        print(e, row)
                try:
                    if session.has_transaction():
                        tx.commit()
                except Exception as e:
                    print(e)
                print(j, "lines processed")
            print(j, "lines processed")
            tx.close()

    def post_processing(self, sess_clicks):
        print("start post processing")
        with self._driver.session() as session:
            tx = session.begin_transaction()
            i = 0
            j = 0
            post_processing_query = """
                MATCH (s:Session {sessionId: $sessionId})-[:CONTAINS]->(click)
                WITH s, click
                ORDER BY click.timestamp
                WITH s, collect(click) as clicks
                WITH s, clicks, clicks[size(clicks) - 1] as lastClick
                CREATE (s)-[:LAST_CLICK]->(lastClick)
                WITH s, clicks
                UNWIND range(1, size(clicks) - 1) as i
                WITH clicks[i - 1] as source, clicks[i] as dest
                CREATE (source)-[:NEXT]->(dest)
            """
            for session_id in list(sess_clicks):
                try:
                    tx.run(post_processing_query, {"sessionId": session_id})
                    i += 1
                    j += 1
                    if i == 2000:
                        tx.commit()
                        print(j, "lines processed")
                        i = 0
                        tx = session.begin_transaction()
                except Exception as e:
                    print(e, session_id)
            tx.commit()
            print(j, "sessions created processed")
        return sess_clicks


if __name__ == '__main__':
    start = time.time()
    importer = YoochooseImporter(sys.argv[1:])
    base_path = importer.source_dataset_path
    if not base_path:
        base_path = "../../../dataset/yoochoose"

    start = time.time()
    sessions = importer.import_session_data(file=os.path.join(base_path, "yoochoose-clicks.dat"))
    print("Time to complete sessions ingestion:", time.time() - start)

    intermediate = time.time()
    importer.import_buys_data(file=os.path.join(base_path, "yoochoose-buys.dat"),
                              sess_clicks=sessions)
    print("Time to complete buys ingestion:", time.time() - intermediate)

    intermediate = time.time()
    importer.post_processing(sess_clicks=sessions)
    print("Time to complete post processing:", time.time() - intermediate)

    print("Time to complete end-to-end:", time.time() - start)

    importer.close()