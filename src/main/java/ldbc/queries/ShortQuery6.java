/**
 * Short read query 6.
 *
 * Copyright © 2016 Alain Kägi
 */

package ldbc.queries;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Direction;

import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery6MessageForumResult;

import ldbc.db.DbUtils;
import ldbc.db.LdbcUtils;

public class ShortQuery6 {

    /**
     * Get a message's forum.
     * @param db         A database handle
     * @param messageId  A message ID
     * @return the forum in which the message was posted
     */
    public static LdbcShortQuery6MessageForumResult query(GraphDatabaseService db,
                                                          long messageId) {
        LdbcShortQuery6MessageForumResult result = null;

        try (Transaction tx = db.beginTx()) {
            Node message = LdbcUtils.findMessageById(db, messageId);

            if (message == null) return null;

            Node post = DbUtils.findProgenitor(
                db,
                message,
                LdbcUtils.EdgeType.REPLY_OF,
                Direction.OUTGOING);

            Node forum = LdbcUtils.findNeighbor(
                db,
                post,
                LdbcUtils.EdgeType.CONTAINER_OF,
                Direction.INCOMING);

            Node moderator = LdbcUtils.findNeighbor(
                db,
                forum,
                LdbcUtils.EdgeType.HAS_MODERATOR,
                Direction.OUTGOING);

            result = new LdbcShortQuery6MessageForumResult(
                LdbcUtils.getId(forum),
                LdbcUtils.getTitle(forum),
                LdbcUtils.getId(moderator),
                LdbcUtils.getFirstName(moderator),
                LdbcUtils.getLastName(moderator));
        }

        return result;
    }

}
