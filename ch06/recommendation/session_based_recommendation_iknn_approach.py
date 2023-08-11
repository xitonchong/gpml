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
            knn = self.compute_knn(item, items_VSM.copy(), 20) 
            print(f"knn: {knn}")
            #self.store_knn(item, knn)

    def get_item_vectors(self): 
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
                print(f"type of vector: {type(vector)}")
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
            print(f"other item: {other_item},")
            if other_item != item: 
                value = cosine_similarity(items[item], items[other_item])
                if value > 0:
                    knn_values = np.concatenate((knn_values, np.array([(other_item, value)], dtype=dtype)))
                    print(f"after merging knn_values: {knn_values}")
        knn_values = np.sort(knn_values, kind='mergesort', order='value')[::-1]
        return np.split(knn_values, [k])[0]




if __name__ == "__main__":
    recommender = SessionBasedRecommender(sys.argv[1:])
    recommender.compute_and_store_similarity() 
    # top10 = recommender.recommend_to(214842060, 10)
    recommender.close()
    #print(top10)










