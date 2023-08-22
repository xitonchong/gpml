import numpy as np
import sys 

from util.sparse_vector import cosine_similarity 

from util.graphdb_base import GraphDBBase 


class SessionBasedRecommender(GraphDBBase): 
    def __init__(self, argv): 
        super().__init__(command=__file__, argv=argv) 

    def compute_and_store_similarity(self): 
        session_VSM = self.get_session_vectors()
        for session in session_VSM: 
            print(f"session: {session}")
            knn = self.compute_knn(session, session_VSM.copy(), 20) 
            self.store_knn(session, knn) 

    def compute_knn(self, session, sessions, k): 
        dtype = [('itemId', 'U10'), ('value', 'f4')]
        knn_values = np.array([], dtype=dtype) 
        for other_session in sessions: 
            if other_session in sessions: 
                if other_session != session:
                    value = cosine_similarity(sessions[session], sessions[other_session])
                    if value > 0: 
                        knn_values = np.concatenate((knn_values, np.array([(other_session, value)], dtype=dtype)))
        knn_values = np.sort(knn_values, kind='mergesort', order='value')[::-1]
        return np.split(knn_values, [k])[0]


    def get_session_vectors(self): 
        list_of_session_query = """
                MATCH (session:Session) 
                RETURN session.sessionId as sessionId 
                LIMIT 2000    
        """

        query =  """
                MATCH (item:Item)<-[:IS_RELATED_TO]-(click:Click)<-[:CONTAINS]-(session:Session)
                where session.sessionId = $sessionId 
                with item 
                order by id(item) 
                return collect(distinct id(item)) as vector;
        """
        sessions_VSM_sparse = {} 
        with self._driver.session() as session: 
            i = 0 
            for result in session.run(list_of_session_query): 
                session_id = result["sessionId"]
                vector = session.run(query, {"sessionId": session_id})
                print(f"vector.single: {vector.single()}")
                sessions_VSM_sparse[session_id] = vector.single()[0]
                i += 1
                if i % 100:
                    print(i, "row processed")
                
            print(i, "rows processed")
        print(len(sessions_VSM_sparse))
        return sessions_VSM_sparse



    def store_knn(self, session_id, knn): 
        with self._driver() as session: 
            tx = session.begin_transaction() 
            knnMap = {str(a): b.item() for a, b in knn}

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



if __name__ == '__main__':
    recommender = SessionBasedRecommender(sys.argv[1:])
    recommender.compute_and_store_similarity()
    top10 = recommender.recommend_to(907, 10)
    recommender.close()
    print(top10)