import numpy as np 
import sys 

from util.sparse_vector import cosine_similarity 
from util.graphdb_base import GraphDBBase 


class SessionBasedRecommender(GraphDBBase): 
    def __init__(self, argv): 
        super().__init__(command=__file__, argv=argv) 

    def compute_and_store_similarity(self): 
        items_VSM = self.get_item_vectors() 
        for item in items_VSM: 
            print(f"item: {item}, type: {type(item)}")
            knn = self.compute_knn(item, items_VSM.copy(), 20) 
            print(f"knn: {knn}")
            self.store_knn(item, knn)


    def get_item_vectors(self): 
        # from an item, get the sessions that clicked on it and parse it as vector
        list_of_items_query = """
            match (item: Item) 
            return item.itemId as itemId
        """

        query = """
            MATCH (item:Item)<-[:IS_RELATED_TO]-(click:Click)<-[:CONTAINS]-(session:Session)
            WHERE item.itemId = $itemId
            with session 
            order by id(session) 
            return collect(distinct id(session)) as vector;
        """
        items_VSM_sparse = {} 
        with self._driver.session() as session:
            i = 0 
            for item in session.run(list_of_items_query): 
                item_id = item["itemId"]
                vector = session.run(query, {"itemId": item_id})
                # print(f"type of vector: {type(vector)}") # class 'neo4j.work.result.Result'
                items_VSM_sparse[item_id] = vector.single()[0]
                i += 1
                if i % 100 == 0:
                    print(i, "rows processed")
            print(i, " rows processed")
        print(len(items_VSM_sparse))
        return items_VSM_sparse


    def compute_knn(self, item, items, k): 
        dtype = [('itemId', 'U10'), ('value', 'f4')]
        knn_values = np.array([], dtype=dtype)
        # https; 
        print(f"type of items: {type(items)}")
        for other_item in items: 
            # print(f"other item: {other_item},")
            if other_item != item: 
                # cosine similarity range is -1,1
                value = cosine_similarity(items[item], items[other_item])
                if value > 0:
                    knn_values = np.concatenate((knn_values, np.array([(other_item, value)], dtype=dtype)))
                    print(f"after merging knn_values: {knn_values.shape}")
        knn_values = np.sort(knn_values, kind='mergesort', order='value')[::-1]
        print(f"knn values: {knn_values}")
        return np.split(knn_values, [k])[0]


    def store_knn(self, item, knn): 
        with self._driver.session() as session: 
            tx = session.begin_transaction() 
            knnMap = {a:b.item() for a, b in knn}
            clean_query = """
                MATCH (item:Item)-[s:SIMILAR_TO]->()
                where item.itemId = $itemId 
                DELETE s
            """
            query = """
                match (item:Item)
                where item.itemId = $itemId 
                UNWIND keys($knn) as otherItemId 
                match (other:Item) 
                where other.itemId = toInteger(otherItemId) 
                merge (item)-[:SIMILAR_TO {weight: $knn[otherItemId]}]->(other)
            """

            tx.run(clean_query, {"itemId": item})
            tx.run(query, {"itemId": item, "knn": knnMap})
            tx.commit()

    def recommend_to(self, item_id, k): 
        top_items = [] 
        query  = """
            MATCH (i:Item)-[:SIMILAR_TO]->(oi:Item)
            where i.itemId = $itemId 
            return oi.itemId as itemId, r.weight as score 
            order by score desc 
            limit %s
        """
        with self._driver.session() as session: 
            tx = session.begin_transaction() 
            for result in tx.run(query % (k), {"itemId": item_id}):
                top_items.append((result["itemId"], result["score"]))
        
        top_items.sort(key=lambda x: -x[1])
        return top_items 



if __name__ == "__main__":
    recommender = SessionBasedRecommender(sys.argv[1:])
    recommender.compute_and_store_similarity() 
    top10 = recommender.recommend_to(214842060, 10)
    recommender.close()
    print(top10)










