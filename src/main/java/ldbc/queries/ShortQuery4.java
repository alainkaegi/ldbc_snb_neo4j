/**
 * Short short query 4.
 *
 * Copyright © 2016 Alain Kägi
 */

package ldbc.queries;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.Node;

import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery4MessageContentResult;

import ldbc.db.LdbcUtils;

public class ShortQuery4 {

    /**
     * Get a message's content.
     * @param db         A database handle
     * @param messageId  A message ID
     * @return the message's content
     */
    public static LdbcShortQuery4MessageContentResult query(GraphDatabaseService db,
                                                            long messageId) {
        LdbcShortQuery4MessageContentResult result = null;

        try (Transaction tx = db.beginTx()) {
            Node message = LdbcUtils.findMessageById(db, messageId);

            if (message == null) return null;

            result = new LdbcShortQuery4MessageContentResult(
                LdbcUtils.getContent(message),
                LdbcUtils.getCreationDate(message));
        }

        return result;
    }

}
