/**
 * Complex read query 8.
 *
 * Copyright © 2016 Alain Kägi
 */

package ldbc.queries;

import java.util.List;
import java.util.ArrayList;
import java.util.Queue;
import java.util.PriorityQueue;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Direction;

import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery8Result;

import ldbc.db.LdbcUtils;
import ldbc.helpers.Query8SortResult;

public class Query8 {

    /**
     * Recent replies (eigth complex read query).
     * @param db        A database handle
     * @param personId  A person ID
     * @param limit     An upper bound on the size of results returned
     * @return The person's most recent replies.
     */
    public static List<LdbcQuery8Result> query(GraphDatabaseService db,
                                               long personId, int limit) {
        List<LdbcQuery8Result> result = new ArrayList<>();

        // Create a priority queue to keep the results sorted and
        // limited to at most 'limit' entries.  To make this work, we
        // inverse the sort order so we know it is safe to remove the
        // entry with highest priority when the queue reaches 'limit +
        // 1' elements.  Also we delay gathering the full set of data
        // until we know a given entry is a keeper.
        Queue<Query8SortResult> queue = new PriorityQueue<>(limit + 1);

        try (Transaction tx = db.beginTx()) {
            Node person = LdbcUtils.findPersonById(db, personId);

            // Iterate over the person's messages.
            for (Relationship edgeFromMessage : person.getRelationships(LdbcUtils.EdgeType.HAS_CREATOR)) {
                Node message = edgeFromMessage.getStartNode();

                // Iterate over the message's replies.
                for (Relationship edgeFromComment : message.getRelationships(Direction.INCOMING, LdbcUtils.EdgeType.REPLY_OF)) {
                    Node comment = edgeFromComment.getStartNode();
                    Node replier = LdbcUtils.findCreatorOfMessage(db, comment);

                    // Create a new temporary result entry.
                    Query8SortResult r = new Query8SortResult(
                        replier,
                        comment,
                        LdbcUtils.getId(comment),
                        LdbcUtils.getCreationDate(comment));

                    // Add the entry to the queue.
                    queue.add(r);

                    // Eliminate the 'highest' priority entry if we have
                    // reached the target number of results.
                    Query8SortResult rr;
                    if (queue.size() > limit)
                        rr = queue.poll();
                }
            }

            // Copy the results by adding elements at the beginning of
            // the final list filling the additional fields as we go.
            while (queue.size() != 0) {
                Query8SortResult r = queue.poll();
                Node replier = r.replier();
                Node comment = r.comment();
                LdbcQuery8Result s = new LdbcQuery8Result(
                    LdbcUtils.getId(replier),
                    LdbcUtils.getFirstName(replier),
                    LdbcUtils.getLastName(replier),
                    r.commentCreationDate(),
                    r.commentId(),
                    LdbcUtils.getContent(comment));
                result.add(0, s);
            }
        }
        return result;
    }

}
