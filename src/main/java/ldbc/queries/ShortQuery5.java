/**
 * Short read query 5.
 *
 * Copyright © 2016 Alain Kägi
 */

package ldbc.queries;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Direction;

import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery5MessageCreatorResult;

import ldbc.db.LdbcUtils;

public class ShortQuery5 {

    /**
     * Get a message's creator.
     * @param db         A database handle
     * @param messageId  A message ID
     * @return the message's creator
     */
    public static LdbcShortQuery5MessageCreatorResult query(GraphDatabaseService db,
                                                            long messageId) {
        LdbcShortQuery5MessageCreatorResult result = null;

        try (Transaction tx = db.beginTx()) {
            Node message = LdbcUtils.findMessageById(db, messageId);

            if (message == null) return null;

            Node creator = LdbcUtils.findNeighbor(
                db,
                message,
                LdbcUtils.EdgeType.HAS_CREATOR,
                Direction.OUTGOING);

            result = new LdbcShortQuery5MessageCreatorResult(
                LdbcUtils.getId(creator),
                LdbcUtils.getFirstName(creator),
                LdbcUtils.getLastName(creator));
        }

        return result;
    }

}
