from annoy import AnnoyIndex 
import sys 
import time 
from util.sparse_matrix import SparseMatrix 
from statistics import mean 
import gc 


from util.graphdb_base import GraphDBBase 



class SessionBasedRecommender(GraphDBBase): 
    
    def __init__(self, argv): 
        super().__init__(command=__file__, argv=argv) 
        self.__time_to_query = [] 
        self.__time_to_knn = [] 
        self.__time_to_sort = []
        self.__time_to_store = []

    def compute_and_store_similarity(self): 
        start = time.time() 
        sessions_VSM, sessions_id = self.get_session_vectors()
        print("time to create the vector: ", time.time() - start)
        print(f"sessions_VSM shape: {sessions_VSM.shape}")
        t = AnnoyIndex(sessions_VSM.shape[1], 'angular')
        t.on_disk_build('/tmp/test.ann')
        start = time.perf_counter()
        i = 0
        overall_size = sessions_VSM.shape[0]
        print(f"overall size: {overall_size}")
        for ix in range(overall_size): 
            x = sessions_VSM.getrow(ix) 
            #print(f"sessions_VSM type: {sessions_VSM}")
            #print(f"x typeL: {type(x)}, {x}")
            t.add_item(ix, x.toarray()[0]) 
            i += 1 
            if i % 1000 == 0:
                print(i, "rows processed over", overall_size) 
            if i > 40_000:
                break
        print("time to index: ", time.perf_counter() - start) 
        del sessions_VSM
        gc.collect() 

        start = time.perf_counter() 
        t.build(5) # 5 trees
        print("time to build: ", time.perf_counter() - start) 
        knn_start = time.perf_counter() 
        i = 0 
        for ix in range(overall_size): 
            knn = self.compute_knn(ix, sessions_id, t, 50) 
            start = time.perf_counter() 
            self.store_knn(sessions_id[ix], knn) 
            self.__time_to_store.append(time.perf_counter() - start) 
            i += 1
            if i % 100 == 0:
                print(i, "rows processed over", overall_size) 
                print(mean(self.__time_to_query), 
                      mean(self.__time_to_knn), 
                      mean(self.__time_to_sort), 
                      mean(self.__time_to_store)) 
                self.__time_to_query = []
                self.__time_to_knn = []  
                self.__time_to_sort = [] 
                self.__time_to_store = []
        print("time to compute knn:", time.perf_counter() - knn_start) 



    def store_knn(self, session_id, knn): 
        with self._driver.session() as session: 
            tx = session.begin_transaction() 
            knnMap = {str(a): b for a,b in knn}
            clean_query = """
                MATCH (session:Session)-[s:SIMILAR_TO]->()
                WHERE session.sessionId = $sessionId 
                DELETE s
            """
            query = """
                MATCH (session:Session) 
                WHERE session.sessionId = $sessionId 
                UNWIND keys($knn) as otherSessionId 
                MATCH (other:Session) 
                WHERE other.sessionId = toInteger(otherSessionId) 
                MERGE (session)-[:SIMILAR_TO {weight: $knn[otherSessionId]}]->(other)
            """
            tx.run(clean_query, {"sessionId": session_id})
            tx.run(query, {"sessionId": session_id, "knn": knnMap})
            tx.commit()



    def compute_knn(self, ix, sessions_id, t, k): 
        knn_values = [] 
        session_id = sessions_id[ix]
        start = time.perf_counter() 
        other_sessions = t.get_nns_by_item(ix, k, include_distances=True) 
        self.__time_to_query.append(time.perf_counter() - start) 
        start = time.perf_counter() 

        for iy in range(len(other_sessions[0])):
            if other_sessions[0][iy] != session_id: 
                value = 1 - other_sessions[1][iy]
                if value > 0: 
                    knn_values.append((other_sessions[0][iy], value))
        self.__time_to_knn.append(time.perf_counter() - start) 
        start = time.perf_counter() 
        knn_values.sort(key=lambda x: -x[1])
        self.__time_to_sort.append(time.perf_counter() - start)
        return knn_values[:k]


    def get_session_vectors(self):
        list_of_items_query = """
            MATCH (session:Session)
            RETURN session.sessionId as sessionId
        """

        query = """
            MATCH (item:Item)<-[:IS_RELATED_TO]-(click:Click)<-[:CONTAINS]-(session:Session) 
            WHERE session.sessionId = $sessionId 
            WITH item 
            order by click.timestamp desc 
            limit 1000 
            with item 
            order by id(item) 
            RETURN collect(distinct id(item)) as vector;
        """

        sessions_VSM_sparse = SparseMatrix() 
        sessions_id = [] 
        with self._driver.session() as session: 
            i = 0 
            for result in session.run(list_of_items_query): 
                session_id = result["sessionId"]
                vector = session.run(query, {"sessionId": session_id})
                sessions_VSM_sparse.addVector(vector.single()[0])
                sessions_id.append(session_id) 
                i += 1
                if i % 1000 == 0: 
                    print(i, " rows processed")
            print(i, " lines processed")
        return sessions_VSM_sparse.getMatrix(), sessions_id 
    

    def recommend_to(self, session_id, k): 
        top_items = [] 
        query = """
            MATCH (target:Session)-[r:SIMILAR_TO]->(d:Session)-[:CONTAINS]->(:Click)-[:IS_RELATED_TO]->(item:Item)
            WHERE target.sessionId = $sessionId 
            WITH DISTINCT item.itemId as itemId, r 
            RETURN itemId, sum(r.weight) as score
            ORDER BY score desc 
            LIMIT %s
        """
        with self._driver.session() as session: 
            tx = session.begin_transaction() 
            for result in tx.run(query % (k), {"sessionId": session_id}):
                top_items.append((result['itemId'], result['score']))
        top_items.sort(key=lambda x: -x[1]) # sort score from highest to lowest
        return top_items


if __name__ == "__main__":
    recommender = SessionBasedRecommender(sys.argv[1:])
    recommender.compute_and_store_similarity()
    top10 = recommender.recommend_to(12547, 10) 
    print(top10) 
    recommender.close()