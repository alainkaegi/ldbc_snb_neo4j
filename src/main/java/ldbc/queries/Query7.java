/**
 * Complex read query 7.
 *
 * Copyright © 2016 Alain Kägi
 */

package ldbc.queries;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Queue;
import java.util.PriorityQueue;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery7Result;

import ldbc.db.LdbcUtils;
import ldbc.helpers.Query7SortResult;

public class Query7 {

    /**
     * Recent likes (seventh complex read query).
     * @param db        A database handle
     * @param personId  A person ID
     * @param limit     An upper bound on the size of results returned
     * @return The person's most recent likes.
     */
    public static List<LdbcQuery7Result> query(GraphDatabaseService db,
                                               long personId, int limit) {
        List<LdbcQuery7Result> result = new ArrayList<>();

        // A hash mapping a liker to the most recent liked relationship.
        Map<Node, Relationship> recentLikes = new HashMap<>();

        try (Transaction tx = db.beginTx()) {
            Node person = LdbcUtils.findPersonById(db, personId);

            // Iterate over the person's messages.
            for (Relationship edgeFromMessage : person.getRelationships(LdbcUtils.EdgeType.HAS_CREATOR)) {
                Node newMessage = edgeFromMessage.getStartNode();

                long newMessageId = LdbcUtils.getId(newMessage);

                // Iterate over the message's likers.
                for (Relationship edgeFromNewLiker : newMessage.getRelationships(LdbcUtils.EdgeType.LIKES)) {
                    Node newLiker = edgeFromNewLiker.getStartNode();

                    // Have we seen this liker?
                    if (recentLikes.containsKey(newLiker)) {
                        // If so, we will update our map only if it is
                        // a newer liker.  We break ties with message ID.
                        long newLikesCreationDate = LdbcUtils.getCreationDate(edgeFromNewLiker);

                        Relationship oldLikes = recentLikes.get(newLiker);
                        long oldLikesCreationDate = LdbcUtils.getCreationDate(oldLikes);
                        Node oldMessage = oldLikes.getEndNode();
                        long oldMessageId = LdbcUtils.getId(oldMessage);

                        if (newLikesCreationDate > oldLikesCreationDate
                            || newLikesCreationDate == oldLikesCreationDate
                            && newMessageId < oldMessageId)
                            recentLikes.put(newLiker, edgeFromNewLiker);
                    }
                    else
                        // If not, simply add to our map.
                        recentLikes.put(newLiker, edgeFromNewLiker);
                }
            }

            // Create a priority queue to keep the results sorted and
            // limited to at most 'limit' entries.  To make this work,
            // we inverse the sort order so we know it is safe to
            // remove the entry with highest priority when the queue
            // reaches 'limit + 1' elements.  Also we delay gathering
            // the full set of data until we know a given entry is a
            // keeper.
            Queue<Query7SortResult> queue = new PriorityQueue<>(limit + 1);

            // Collect the results.
            for (Map.Entry<Node, Relationship> e : recentLikes.entrySet()) {
                Node liker = e.getKey();
                Relationship edgeFromLiker = e.getValue();
                Node message = edgeFromLiker.getEndNode();
                long likesCreationDate = LdbcUtils.getCreationDate(edgeFromLiker);
                long messageCreationDate = LdbcUtils.getCreationDate(message);
                int latency = (int)((likesCreationDate - messageCreationDate)/60000);

                // Create a new temporary result entry.
                Query7SortResult r = new Query7SortResult(
                    liker,
                    message,
                    LdbcUtils.getId(liker),
                    likesCreationDate,
                    latency);

                // Add the entry to the queue.
                queue.add(r);

                // Eliminate the 'highest' priority entry if we have
                // reached the target number of results.
                Query7SortResult rr;
                if (queue.size() > limit)
                    rr = queue.poll();
            }

            // Copy the results by adding elements at the beginning of
            // the final list filling the additional fields as we go.
            while (queue.size() != 0) {
                Query7SortResult r = queue.poll();
                Node liker = r.liker();
                Node message = r.message();
                LdbcQuery7Result s = new LdbcQuery7Result(
                    LdbcUtils.getId(liker),
                    LdbcUtils.getFirstName(liker),
                    LdbcUtils.getLastName(liker),
                    r.likesCreationDate(),
                    LdbcUtils.getId(message),
                    LdbcUtils.getContent(message),
                    r.latency(),
                    !LdbcUtils.areTheyFriend(db, person, liker));
                result.add(0, s);
            }
        }
        return result;
    }

}
