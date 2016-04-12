/**
 * Complex read query 9.
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
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.graphdb.traversal.Evaluators;

import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery9Result;

import ldbc.db.LdbcUtils;
import ldbc.helpers.Query9SortResult;

public class Query9 {

    /**
     * Recent messages by friends (ninth complex read query).
     * @param db        A database handle
     * @param personId  A person ID
     * @param date      A date in milliseconds since 1/1/1970 00:00:00 GMT
     * @param limit     An upper bound on the size of results returned
     * @return Recent posts by friends
     */
    public static List<LdbcQuery9Result> query(GraphDatabaseService db,
                                               long personId, long date,
                                               int limit) {
        List<LdbcQuery9Result> result = new ArrayList<>();

        // Create a priority queue to keep the results sorted and
        // limited to at most 'limit' entries.  To make this work, we
        // inverse the sort order so we know it is safe to remove the
        // entry with highest priority when the queue reaches 'limit +
        // 1' elements.  Also we delay gathering the full set of data
        // until we know a given entry is a keeper.
        Queue<Query9SortResult> queue = new PriorityQueue<>(limit + 1);

        // Create a traversal description for the person's friends
        // and friends of friends.
        TraversalDescription knowsTraversal = db.traversalDescription()
            .breadthFirst()
            .relationships(LdbcUtils.EdgeType.KNOWS)
            .evaluator(Evaluators.toDepth(2))
            .evaluator(Evaluators.excludeStartPosition());

        try (Transaction tx = db.beginTx()) {
            Node person = LdbcUtils.findPersonById(db, personId);

            // Iterate over the person's friends and friends of friends.
            for (Node friend : knowsTraversal.traverse(person).nodes()) {

                // Iterate over the friend's messages.
                for (Relationship edgeFromMessage : friend.getRelationships(LdbcUtils.EdgeType.HAS_CREATOR)) {
                    Node message = edgeFromMessage.getStartNode();

                    // Skip messages created on or after date.
                    long messageCreationDate = LdbcUtils.getCreationDate(message);
                    if (messageCreationDate >= date)
                        continue;

                    // Create a new temporary result entry.
                    Query9SortResult r = new Query9SortResult(
                        friend,
                        message,
                        LdbcUtils.getId(message),
                        messageCreationDate);

                    // Add the entry to the queue.
                    queue.add(r);

                    // Eliminate the 'highest' priority entry if we have
                    // reached the target number of results.
                    Query9SortResult rr;
                    if (queue.size() > limit)
                        rr = queue.poll();
                }
            }

            // Copy the results by adding elements at the beginning of
            // the final list filling the additional fields as we go.
            while (queue.size() != 0) {
                Query9SortResult r = queue.poll();
                Node friend = r.friend();
                Node message = r.message();
                LdbcQuery9Result s = new LdbcQuery9Result(
                    LdbcUtils.getId(friend),
                    LdbcUtils.getFirstName(friend),
                    LdbcUtils.getLastName(friend),
                    r.messageId(),
                    LdbcUtils.getContent(message),
                    r.messageCreationDate());
                result.add(0, s);
            }

        }

        return result;
    }

}
