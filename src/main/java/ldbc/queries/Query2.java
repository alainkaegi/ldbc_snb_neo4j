/**
 * Complex read query 2.
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

import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery2Result;

import ldbc.db.LdbcUtils;
import ldbc.helpers.Query2SortResult;

public class Query2 {

    /**
     * Recent messages by your friends (second complex read query).
     * @param db        A database handle
     * @param personId  A person ID
     * @param date      A date in milliseconds since 1/1/1970 00:00:00 GMT
     * @param limit     An upper bound on the size of results returned
     * @return the top 'limit' recent messages posted the person's friends
     */
    public static List<LdbcQuery2Result> query(GraphDatabaseService db,
                                               long personId,
                                               long date,
                                               int limit) {
        List<LdbcQuery2Result> result = new ArrayList<>();

        // Create a priority queue to keep the results sorted and
        // limited to at most 'limit' entries.  To make this work, we
        // inverse the sort order so we know it is safe to remove the
        // entry with highest priority when the queue reaches 'limit +
        // 1' elements.  Also we delay gathering the full set of data
        // until we know a given entry is a keeper.
        Queue<Query2SortResult> queue = new PriorityQueue<>(limit + 1);

        try (Transaction tx = db.beginTx()) {
            Node person = LdbcUtils.findPersonById(db, personId);

            // Iterate over the person's friends.
            for (Relationship edgeToFriend : person.getRelationships(Direction.BOTH, LdbcUtils.EdgeType.KNOWS)) {
                Node friend = edgeToFriend.getOtherNode(person);

                // Iterate over the friend's messages skipping those
                // older than date and add to the results.
                for (Relationship edgeFromMessage : friend.getRelationships(LdbcUtils.EdgeType.HAS_CREATOR)) {
                    Node message = edgeFromMessage.getStartNode();

                    long creationDate = LdbcUtils.getCreationDate(message);
                    if (creationDate <= date) {
                        // Create a new temporary result entry.
                        Query2SortResult r = new Query2SortResult(
                            friend,
                            message,
                            LdbcUtils.getId(message),
                            creationDate);

                        // Add the entry to the queue.
                        queue.add(r);

                        // Eliminate the 'highest' priority entry if we have
                        // reached the target number of results.
                        Query2SortResult rr;
                        if (queue.size() > limit)
                            rr = queue.poll();
                    }
                }
            }

            // Copy the results by adding elements at the beginning of
            // the final list filling the additional fields as we go.
            while (queue.size() != 0) {
                Query2SortResult r = queue.poll();
                Node friend = r.friend();
                Node message = r.message();
                LdbcQuery2Result s = new LdbcQuery2Result(
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
