/**
 * Update query 4.
 *
 * Copyright © 2016 Alain Kägi
 */

package ldbc.queries;

import java.util.Map;
import java.util.HashMap;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.Node;

import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate4AddForum;

import ldbc.db.LdbcUtils;

public class UpdateQuery4 {

    /**
     * Add a forum.
     * @param db          A database handle
     * @param parameters  A forum's full description
     */
    public static void query(GraphDatabaseService db,
                             LdbcUpdate4AddForum parameters) {
        // Prepare the in-node forum's properties.
        Map<String, Object> props = new HashMap<>(3);
        props.put(LdbcUtils.Keys.ID, parameters.forumId());
        props.put(LdbcUtils.Keys.TITLE, parameters.forumTitle());
        props.put(LdbcUtils.Keys.CREATIONDATE, parameters.creationDate().getTime());

        try (Transaction tx = db.beginTx()) {
            // Add the forum and its in-node properties.
            Node forum = LdbcUtils.createForum(db, props);

            // Add a link to the moderator.
            Node moderator = LdbcUtils.findPersonById(db, parameters.moderatorPersonId());
            LdbcUtils.createHasModeratorEdge(forum, moderator);

            // Add links to the tags.
            for (long tagId : parameters.tagIds()) {
                Node tag = LdbcUtils.findTagById(db, tagId);
                LdbcUtils.createHasTagEdge(forum, tag);
            }

            tx.success();
        }
    }

}
