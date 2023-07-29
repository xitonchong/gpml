import pandas as pd 
import numpy as np 
import time 
import sys 
import os 


from util.graphdb_base import GraphDBBase


class PaySimImporter(GraphDBBase):
    def __init__(self, argv): 
        super().__init__(command=__file__, argv=argv)

    def import_paysim(self, file): 
        dtype = {
            "step": np.int64,
            "type": object,
            "amount": np.float32,
            "nameOrig": object,
            "oldbalanceOrg": np.float32,
            "newbalanceOrig": np.float32,
            "nameDest": object,
            "oldbalanceDest": np.float32,
            "newbalanceDest": np.float32,
            "isFraud": np.int32,
            "isFlaggedFraud": np.int32
        }

        j = 0 
        transaction_by_user = {} 
        for chunk in pd.read_csv(file, 
                                header=0, 
                                dtype=dtype, 
                                names=['step', 'type', 'amount', 'nameOrig', 'oldbalanceOrg', 'newbalanceOrig',
                                        'nameDest', 'oldbalanceDest', 'newbalanceDest', 'isFraud', 'isFlaggedFraud'],
                                chunksize = 10 ** 3): 
            df = chunk 
            for record in df.to_dict("records"):
                row = record.copy() 
                j += 1
                row["sourceLabels"] = ["Customer"]
                row["destLabels"] = []
                row["transLabels"] = []
                row["relationshipType"] = row["type"].upper()
                if row["nameDest"].startswith("M"):
                    row["destLabels"] += ["Merchant"]
                else: 
                    row["destLabels"] += ["Customer"]
                if row["isFraud"] == 1:
                    row["transLabels"] += ["Fraud"]
                userId = row["nameOrig"]
                if userId in transaction_by_user: 
                    transaction_by_user[userId] += [row]
                else: 
                    transaction_by_user[userId] = [row]
                if j % 1000 == 0: 
                    print(j, " lines processed") 
            print(j, "lines processed")
        print(j, "total lines")

        print(f"total number of users after filtering: {len(transaction_by_user)}")

        query  = """ 
            WITH $row as map 
            MERGE (source:Entity {id: map.nameOrig})
            MERGE (dest:Entity {id: map.nameDest})
            WITH source, dest, map
            CALL apoc.create.addLabels( dest, map.destLabels) YIELD node as destNode
            WITH source, dest, map
            create (transaction: Transaction {id: $transId})
            SET transaction += map 
            SET transaction, source, dest, map
            CALL apoc.create.addLabels( transaction, map.transLabels) YIELD node 
            CREATE (source)<-[:TRANSACTION_SOURCE]-(transaction)
            CREATE (dest)<-[:TRANSACTION_DEST]-(transaction)
            CREATE (source)-[t:TRANSFER_MONEY_TO]->(dest)
            SET t = map
            WITH source, dest, map 
            CALL apoc.create.relationship( source, map.relationshipType, map, dest) YIELD rel
            RETURN map
        """

        with self._driver.session() as session:
            self.execute_without_exception("CREATE CONSTRAINT ON (s:Entity) ASSERT s.id IS UNIQUE")
            self.execute_without_exception("CREATE CONSTRAINT ON (s: Customer) ASSERT s.id IS UNIQUE")
            self.execute_without_exception("CREATE CONSTRAINT ON (s:Merchant) ASSERT s.id IS UNIQUE")
            self.execute_without_exception("CREATE CONSTRAINT ON (s:Transaction) AASERT s.id IS UNIQUE")
            tx = session.begin_transaction()
            j = 0 
            i = 0
            for user_id in list(transaction_by_user):
                for row in transaction_by_user[user_id]:
                    try: 
                        tx.run(query, {"row": row,  })
                    except:
                        pass
