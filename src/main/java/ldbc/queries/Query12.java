/**
 * Complex read query 12.
 *
 * Copyright © 2016 Alain Kägi
 */

package ldbc.queries;

import java.util.List;
import java.util.ArrayList;
import java.util.Set;
import java.util.TreeSet;
import java.util.Map;
import java.util.HashMap;
import java.util.Queue;
import java.util.PriorityQueue;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Relationship;

import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery12Result;

import ldbc.db.LdbcUtils;
import ldbc.helpers.Query12SortResult;

public class Query12 {

    /**
     * Expert search (12th complex read query).
     * @param db            A database handle
     * @param personId      A person ID
     * @param tagClassName  A class of tags
     * @param limit         An upper bound on the size of results returned
     * @return count replies to posts with tags within the class
     */
    public static List<LdbcQuery12Result> query(GraphDatabaseService db,
                                                long personId,
                                                String tagClassName,
                                                int limit) {
        List<LdbcQuery12Result> result = new ArrayList<>();

        // Create a priority queue to keep the results sorted and
        // limited to at most 'limit' entries.  To make this work,
        // we inverse the sort order so we know it is safe to
        // remove the entry with highest priority when the queue
        // reaches 'limit + 1' elements.  Also we delay gathering
        // the full set of data until we know a given entry is a
        // keeper.
        Queue<Query12SortResult> queue = new PriorityQueue<>(limit + 1);

        Map<Node, Set<String>> tags = new HashMap<>();
        Map<Node, Integer> counts = new HashMap<>();

        try (Transaction tx = db.beginTx()) {
            Node person = LdbcUtils.findPersonById(db, personId);
            Node tagClass = LdbcUtils.findTagClassByName(db, tagClassName);
            long tagClassId = LdbcUtils.getId(tagClass);

            // Iterate over the person's friends.
            for (Relationship edgeToFriend : person.getRelationships(Direction.BOTH, LdbcUtils.EdgeType.KNOWS)) {
                Node friend = edgeToFriend.getOtherNode(person);

                // Iterate over the friend's messages.
                for (Relationship edgeFromMessage : friend.getRelationships(LdbcUtils.EdgeType.HAS_CREATOR)) {
                    Node message = edgeFromMessage.getStartNode();

                    // Skip posts.
                    if (LdbcUtils.isMessagePost(message))
                        continue;

                    // Find parent of comment (must have one).
                    Node parentMessage = LdbcUtils.findNeighbor(db, message, LdbcUtils.EdgeType.REPLY_OF, Direction.OUTGOING);

                    // Skip parent message that is a comment.
                    if (!LdbcUtils.isMessagePost(parentMessage))
                        continue;

                    // Iterate over the parent post's tags.
                    boolean AtLeastOneTagProcessed = false;
                    for (Relationship edgeToTag : parentMessage.getRelationships(LdbcUtils.EdgeType.HAS_TAG)) {
                        Node tag = edgeToTag.getEndNode();

                        // Skip tag not falling under the tag class' umbrella.
                        Node tagClassOfThisTag = LdbcUtils.findNeighbor(db, tag, LdbcUtils.EdgeType.HAS_TYPE, Direction.OUTGOING);
                        if (LdbcUtils.getId(tagClassOfThisTag) != tagClassId
                            && !LdbcUtils.isNodeDescendantOfAncestor(db, tagClassOfThisTag, tagClass, LdbcUtils.EdgeType.IS_SUBCLASS_OF, Direction.OUTGOING))
                            continue;

                        String tagName = LdbcUtils.getName(tag);

                        if (counts.get(friend) == null) {
                            counts.put(friend, 1);
                            Set<String> tempTags = new TreeSet<>();
                            tempTags.add(tagName);
                            tags.put(friend, tempTags);
                        }
                        else {
                            if (!AtLeastOneTagProcessed)
                                counts.put(friend, counts.get(friend) + 1);
                            Set<String> tempTags = tags.get(friend);
                            tempTags.add(tagName);
                            tags.remove(friend);
                            tags.put(friend, tempTags);
                        }
                        AtLeastOneTagProcessed = true;
                    }

                }
            }

            for (Map.Entry<Node, Integer> e : counts.entrySet()) {
                Node friend = e.getKey();

                // Create a new temporary result entry.
                Query12SortResult r = new Query12SortResult(
                    friend,
                    LdbcUtils.getId(friend),
                    e.getValue());

                // Add the entry to the queue.
                queue.add(r);

                // Eliminate the 'highest' priority entry if we have
                // reached the target number of results.
                Query12SortResult rr;
                if (queue.size() > limit)
                    rr = queue.poll();
            }

            // Copy the results by adding elements at the beginning of
            // the final list filling the additional fields as we go.
            while (queue.size() != 0) {
                Query12SortResult r = queue.poll();
                Node friend = r.friend();
                LdbcQuery12Result s = new LdbcQuery12Result(
                    r.friendId(),
                    LdbcUtils.getFirstName(friend),
                    LdbcUtils.getLastName(friend),
                    tags.get(friend),
                    r.replyCount());
                result.add(0, s);
            }
        }
        return result;
    }

}
