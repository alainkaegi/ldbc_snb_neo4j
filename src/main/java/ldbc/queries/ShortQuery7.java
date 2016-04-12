/**
 * Short read query 7.
 *
 * Copyright © 2016 Alain Kägi
 */

package ldbc.queries;

import java.util.List;
import java.util.ArrayList;
import java.util.Queue;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.HashSet;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Direction;

import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery7MessageRepliesResult;

import ldbc.db.LdbcUtils;
import ldbc.helpers.ShortQuery7SortResult;

public class ShortQuery7 {

    /**
     * Get a message's replies.
     * @param db         A database handle
     * @param messageId  A message ID
     * @return the replies to the given message
     */
    public static List<LdbcShortQuery7MessageRepliesResult> query(GraphDatabaseService db,
                                                                  long messageId) {
        List<LdbcShortQuery7MessageRepliesResult> result = new ArrayList<>();

        // Create a priority queue to sort the entries as we
        // accumulate results.
        Queue<ShortQuery7SortResult> queue = new PriorityQueue<>();

        try (Transaction tx = db.beginTx()) {
            Node message = LdbcUtils.findMessageById(db, messageId);

            if (message == null) return result;

            Node messageAuthor = LdbcUtils.findNeighbor(
                db,
                message,
                LdbcUtils.EdgeType.HAS_CREATOR,
                Direction.OUTGOING);

            // Get all the friends of the message's author.
            Set<Node> messageAuthorFriends = new HashSet<>();
            for (Relationship edgeFromAuthor
                     : messageAuthor.getRelationships(
                           Direction.BOTH,
                           LdbcUtils.EdgeType.KNOWS))
                messageAuthorFriends.add(edgeFromAuthor.getOtherNode(messageAuthor));

            // Iterate over the message's replies.
            for (Relationship edgeFromComment : message.getRelationships(Direction.INCOMING, LdbcUtils.EdgeType.REPLY_OF)) {
                Node comment = edgeFromComment.getStartNode();
                Node commentAuthor = LdbcUtils.findNeighbor(
                    db,
                    comment,
                    LdbcUtils.EdgeType.HAS_CREATOR,
                    Direction.OUTGOING);

                // Create a new temporary result entry.
                ShortQuery7SortResult r = new ShortQuery7SortResult(
                    comment,
                    commentAuthor,
                    LdbcUtils.getCreationDate(comment),
                    LdbcUtils.getId(commentAuthor));

                // Add the entry to the queue.
                queue.add(r);
            }

            // Copy the results to the permanent result structure.
            while (queue.size() != 0) {
                ShortQuery7SortResult r = queue.poll();
                Node comment = r.comment();
                Node commentAuthor = r.commentAuthor();
                LdbcShortQuery7MessageRepliesResult s = new LdbcShortQuery7MessageRepliesResult(
                    LdbcUtils.getId(comment),
                    LdbcUtils.getContent(comment),
                    r.commentCreationDate(),
                    r.commentAuthorId(),
                    LdbcUtils.getFirstName(commentAuthor),
                    LdbcUtils.getLastName(commentAuthor),
                    messageAuthorFriends.contains(commentAuthor));
                result.add(s);
            }
        }

        return result;
    }

}
