https://github.com/alenegro81/gpml


# remeber to install graph data science and apoc library 
before starting the database else you will get transaction commit error during import. 


CREATE allows you to create a new node (or relationship). 
parentheses define the boundaries of the created not instances,
p, t, k are instances of the label Movie. 

The simple model ha multiple drawbacks
 - data duplication:  in each property, data is duplicated. 
 - Error proneness
 - difficult to extend/enrich
 - navigation compexity:  any access or search is based on value comparison or, worse string comparison.


Listing 4.3 Query to find the actors who worked together (simple model) 
```
match (m: Movie) 
with m.actors as actors 
unwind actors as actor
match (n: movie)
where actor in n.actors
with actor, n.actors as otherActors, n.title as title 
unwind otherActors as otherActor 
with actor, otherActor, title 
where actor <> otherActor 
return actor, otherActor, title 
order by actor 

```

1. the first match searches for all the movies
2. with is used to forward the results to the next step. the first on eforwards only the actors lst. 
3. with `unwind`, you can transform any list back to individualrows. the list of actors in each movie  is converted to a sequence of actors. 
4. for each actor, the next `match` with the where condition finds all the movies they acted in.
5.  the second with forwards the actor considered in this iteration, the list of actors in each movie they acted in , and the movie title. 
7. the last where filters our pairs in which the actors is paried with their own self. 
8.  the query returns the names in each pair and the title of the movie in which both acted.
9. the results are sorted, with the clause order by, by the name of the first actor in the pair.    



## Cypher Notation 
FOREACH 
SET -  assign a new specific label to the node, depending on the needs
MERGE - checks (and creates if necessary) the relationship between two entities, i.e. `Person` and `Movie`.  

> Modeling pro tips
you can use mulitple labels for the same node. In this cae, this approach is both useful and necessary because, in the model, we would like to have each person represented uniquely regardless of the role they play in the movie (actor, writer, or director). 
For this reason, we opt for MERGE instead of CREATE and use a common label for all of them. At the same time, the graph model assigns a specific label for each role the person has.  After it is assigned, a label becomes assigned to the node, so it will be easier and more performant to run queries such as "Find me all the producers who...">


Completely clear or delete neo4j data 
```
MATCH (n)
DETACH DELETE n

```bash