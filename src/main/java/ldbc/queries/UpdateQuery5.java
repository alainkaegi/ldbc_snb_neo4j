/**
 * Update query 5.
 *
 * Copyright © 2016 Alain Kägi
 */

package ldbc.queries;

import java.util.Map;
import java.util.HashMap;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.Node;

import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate5AddForumMembership;

import ldbc.db.LdbcUtils;

public class UpdateQuery5 {

    /**
     * Add an edge from a forum to a person..
     * @param db          A database handle
     * @param parameters  The parameters of this transaction
     */
    public static void query(GraphDatabaseService db,
                             LdbcUpdate5AddForumMembership parameters) {
        // Prepare the properties.
        Map<String, Object> eProps = new HashMap<>(1);
        eProps.put(LdbcUtils.Keys.JOINDATE, parameters.joinDate().getTime());

        try (Transaction tx = db.beginTx()) {
            Node forum = LdbcUtils.findForumById(db, parameters.forumId());
            Node person = LdbcUtils.findPersonById(db, parameters.personId());
            LdbcUtils.createHasMemberEdge(forum, person, eProps);

            tx.success();
        }
    }

}
