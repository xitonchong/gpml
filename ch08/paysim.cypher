//CALL apoc.periodic.submit(
//'louvain',
//'CALL algo.louvain("Entity", "TRANSFER_TO",
//  {write:true, writeProperty:"louvainCommunity"})
//YIELD nodes, communityCount, iterations, loadMillis, computeMillis, writeMillis;'
//);


// create a graph projection first; then use community detection-louvain algo
CALL gds.graph.project(
    'myGraph',
    'Entity', // Both Customer and Merchant falls under Entity
    {
        TRANSFER: { //transfer is relationship_type 
            orientation: 'UNDIRECTED'
        }
    }
)
call gds.louvain.stats('myGraph') 
yield communityCount // this is the answer


// the following are used to check sample data, node count, relationship count, etc...

// check memory requirement for the algorithm 
CALL gds.louvain.write.estimate('myGraph', { writeProperty: 'community' })
YIELD nodeCount, relationshipCount, bytesMin, bytesMax, requiredMemory

//the following will run the algo and stream the result 
call gds.louvain.stream('myGraph') 
YIELD nodeId, communityId,  intermediateCommunity
return gds.util.asNode(nodeId).identity as name, communityId
order by name asc
limit 10; // to prevent OOM






