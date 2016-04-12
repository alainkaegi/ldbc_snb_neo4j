/**
 * Update query 8.
 *
 * Copyright © 2016 Alain Kägi
 */

package ldbc.queries;

import java.util.Map;
import java.util.HashMap;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.Node;

import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate8AddFriendship;

import ldbc.db.LdbcUtils;

public class UpdateQuery8 {

    /**
     * Add a friendship.
     * @param db          A database handle
     * @param parameters  The parameters of this transaction
     */
    public static void query(GraphDatabaseService db,
                             LdbcUpdate8AddFriendship parameters) {
        // Prepare the properties.
        Map<String, Object> eProps = new HashMap<>(1);
        eProps.put(LdbcUtils.Keys.CREATIONDATE, parameters.creationDate().getTime());

        try (Transaction tx = db.beginTx()) {
            Node person1 = LdbcUtils.findPersonById(db, parameters.person1Id());
            Node person2 = LdbcUtils.findPersonById(db, parameters.person2Id());
            LdbcUtils.createKnowsEdge(person1, person2, eProps);

            tx.success();
        }
    }

}
