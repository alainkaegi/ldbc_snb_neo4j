/**
 * Complex read query 4.
 *
 * Copyright © 2016 Alain Kägi
 */

package ldbc.queries;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;
import java.util.Queue;
import java.util.PriorityQueue;

import java.util.Comparator;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Direction;

import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery4Result;

import ldbc.db.LdbcUtils;

public class Query4 {

    /**
     * New topics (fourth complex read query).
     * @param db          A database handle
     * @param personId    A person ID
     * @param startDdate  A date in milliseconds since 1/1/1970 00:00:00 GMT
     * @param duration    A duration in days
     * @param limit       An upper bound on the size of results returned
     * @return topics first created in the range provided
     */
    public static List<LdbcQuery4Result> query(GraphDatabaseService db,
                                               long personId,
                                               long startDate, int duration,
                                               int limit) {
        List<LdbcQuery4Result> result = new ArrayList<>();

        // Create a priority queue to keep the results sorted and
        // limited to at most 'limit' entries.  To make this work, we
        // inverse the sort order so we know it is safe to remove the
        // entry with highest priority when the queue reaches 'limit +
        // 1' elements.
        Queue<LdbcQuery4Result> queue = new PriorityQueue<>(
            limit,
            new Comparator<LdbcQuery4Result>() {
                @Override
                public int compare(LdbcQuery4Result r1, LdbcQuery4Result r2) {
                    long c1 = r1.postCount();
                    long c2 = r2.postCount();
                    if (c1 == c2) {
                        String t1 = r1.tagName();
                        String t2 = r2.tagName();
                        return t2.compareTo(t1);
                    }
                    else
                        return Long.compare(c1, c2); // descending
                }
            });

        Map<Node, Integer> counts = new HashMap<Node, Integer>();
        Set<Node> exclude = new HashSet<Node>();

        try (Transaction tx = db.beginTx()) {
            Node person = LdbcUtils.findPersonById(db, personId);
            long endDate = startDate + (long)duration * 24 * 60 * 60 * 1000;

            // Iterate over the person's friends.
            for (Relationship edgeToFriend : person.getRelationships(Direction.BOTH, LdbcUtils.EdgeType.KNOWS)) {
                Node friend = edgeToFriend.getOtherNode(person);

                // Iterate over the friend's messages.
                for (Relationship edgeFromMessage : friend.getRelationships(LdbcUtils.EdgeType.HAS_CREATOR)) {
                    Node message = edgeFromMessage.getStartNode();

                    // Skip comments.
                    if (!LdbcUtils.isMessagePost(message))
                        continue;

                    long creationDate = LdbcUtils.getCreationDate(message);

                    // Skip messages too young.
                    if (creationDate >= endDate)
                        continue;

                    // Iterate over the post's tags.
                    for (Relationship edgeToTag : message.getRelationships(LdbcUtils.EdgeType.HAS_TAG)) {
                        Node tag = edgeToTag.getEndNode();

                        // If not already excluded ...
                        if (!exclude.contains(tag)) {
                            // ... exclude tags associated with older posts.
                            if (creationDate < startDate) {
                                exclude.add(tag);
                                counts.remove(tag);
                            }
                            // Otherwise update the tag's count.
                            else {
                                if (counts.get(tag) == null)
                                    counts.put(tag, 1);
                                else
                                    counts.put(tag, counts.get(tag) + 1);
                            }
                        }
                    }
                }
            }

            // Collect the results.
            for (Node tag : counts.keySet()) {
                // Create a new entry.
                LdbcQuery4Result r = new LdbcQuery4Result(
                    LdbcUtils.getName(tag),
                    counts.get(tag));

                // Add the entry to the queue.
                queue.add(r);

                // Eliminate the 'highest' priority entry if we have
                // reached the target number of results.
                LdbcQuery4Result rr;
                if (queue.size() > limit)
                    rr = queue.poll();
            }

        }

        // Copy the results by adding elements at the
        // beginning of the list.
        while (queue.size() != 0)
            result.add(0, queue.poll());

        return result;
    }

}
