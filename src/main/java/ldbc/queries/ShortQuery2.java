/**
 * Short read query 2.
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

import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery2PersonPostsResult;

import ldbc.db.DbUtils;
import ldbc.db.LdbcUtils;
import ldbc.helpers.ShortQuery2SortResult;

public class ShortQuery2 {

    /**
     * Get a person's recent messages.
     * @param db        A database handle
     * @param personId  A person ID
     * @param limit      An upper bound on the size of results returned
     * @return up to 'limit' messages created by the person
     */
    public static List<LdbcShortQuery2PersonPostsResult> query(GraphDatabaseService db,
                                                               long personId,
                                                               int limit) {
        List<LdbcShortQuery2PersonPostsResult> result = new ArrayList<>();

        // Create a priority queue to keep the results sorted and
        // limited to at most 'limit' entries.  To make this work, we
        // inverse the sort order so we know it is safe to remove the
        // entry with highest priority when the queue reaches 'limit +
        // 1' elements.  Also we delay gathering the full set of data
        // until we know a given entry is a keeper.
        Queue<ShortQuery2SortResult> queue = new PriorityQueue<>(limit + 1);

        try (Transaction tx = db.beginTx()) {
            Node person = LdbcUtils.findPersonById(db, personId);

            if (person == null) return result;

            // Iterate over the person's messages.
            for (Relationship edgeToPerson : person.getRelationships(Direction.INCOMING, LdbcUtils.EdgeType.HAS_CREATOR)) {
                Node message = edgeToPerson.getStartNode();

                // Create a new temporary result entry.
                ShortQuery2SortResult r = new ShortQuery2SortResult(
                    message,
                    LdbcUtils.getId(message),
                    LdbcUtils.getCreationDate(message));

                // Add the entry to the queue.
                queue.add(r);

                // Eliminate the 'highest' priority entry if we have
                // reached the target number of results.
                ShortQuery2SortResult rr;
                if (queue.size() > limit)
                    rr = queue.poll();
            }

            // Copy the results by adding elements at the beginning of
            // the final list filling the additional fields as we go.
            while (queue.size() != 0) {
                ShortQuery2SortResult r = queue.poll();
                Node message = r.message();
                long messageId = r.messageId();

                // Get the original post (the message may be it).
                Node originalPost
                    = DbUtils.findProgenitor(
                        db, message,
                        LdbcUtils.EdgeType.REPLY_OF,
                        Direction.OUTGOING);
                long originalPostId = LdbcUtils.getId(originalPost);

                // Get the author of the original post.
                Node originalPostAuthor;
                long originalPostAuthorId;
                if (originalPostId == messageId) {
                    originalPostAuthor = person;
                    originalPostAuthorId = personId;
                }
                else {
                    originalPostAuthor
                        = LdbcUtils.findNeighbor(
                            db, originalPost,
                            LdbcUtils.EdgeType.HAS_CREATOR,
                            Direction.OUTGOING);
                    originalPostAuthorId = LdbcUtils.getId(originalPostAuthor);
                }

                // Gather all the info into one result item.
                LdbcShortQuery2PersonPostsResult s
                    = new LdbcShortQuery2PersonPostsResult(
                        messageId,
                        LdbcUtils.getContent(message),
                        r.messageCreationDate(),
                        originalPostId,
                        originalPostAuthorId,
                        LdbcUtils.getFirstName(originalPostAuthor),
                        LdbcUtils.getLastName(originalPostAuthor));

                // Add it to the list.
                result.add(0, s);
            }
        }

        return result;
    }

}
