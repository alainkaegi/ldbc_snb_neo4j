/**
 * Update query 2.
 *
 * Copyright © 2016 Alain Kägi
 */

package ldbc.queries;

import java.util.Map;
import java.util.HashMap;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.Node;

import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate2AddPostLike;

import ldbc.db.LdbcUtils;

public class UpdateQuery2 {

    /**
     * Add a 'likes' edge from a person to a post.
     * @param db          A database handle
     * @param parameters  The parameters of this transaction
     */
    public static void query(GraphDatabaseService db,
                             LdbcUpdate2AddPostLike parameters) {
        // Prepare the properties.
        Map<String, Object> eProps = new HashMap<>(1);
        eProps.put(LdbcUtils.Keys.CREATIONDATE, parameters.creationDate().getTime());

        try (Transaction tx = db.beginTx()) {
            Node person = LdbcUtils.findPersonById(db, parameters.personId());
            Node post = LdbcUtils.findPostById(db, parameters.postId());
            LdbcUtils.createLikesEdge(person, post, eProps);

            tx.success();
        }
    }

}
