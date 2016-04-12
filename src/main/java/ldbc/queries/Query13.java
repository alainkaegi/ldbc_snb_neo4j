/**
 * Complex read query 13.
 *
 * Copyright © 2016 Alain Kägi
 */

package ldbc.queries;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.traversal.TraversalDescription;

import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery13Result;

import ldbc.db.LdbcUtils;

public class Query13 {

    /**
     * Single shortest path (13th complex read query).
     * @param db         A database handle
     * @param person1Id  A first person ID
     * @param person2Id  A second person ID
     * @return the shortest path between person1 and person2.
     */
    public static LdbcQuery13Result query(GraphDatabaseService db,
                                          long person1Id, long person2Id) {
        LdbcQuery13Result result = null;

        // Create a traversal description for a person's friends.
        TraversalDescription knowsTraversal = db.traversalDescription()
            .breadthFirst()
            .relationships(LdbcUtils.EdgeType.KNOWS);

        try (Transaction tx = db.beginTx()) {
            Node person1 = LdbcUtils.findPersonById(db, person1Id);
            int pathLength = -1;

            // Iterate over person 1's friends.
            for (Path pathToFriend : knowsTraversal.traverse(person1)) {

                Node person2 = pathToFriend.endNode();
                if (LdbcUtils.getId(person2) == person2Id) {
                    pathLength = pathToFriend.length();
                    break;
                }

            }

            result = new LdbcQuery13Result(pathLength);
        }
        return result;
    }

}
