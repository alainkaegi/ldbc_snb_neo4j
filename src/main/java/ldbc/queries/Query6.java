/**
 * Complex read query 6.
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

import java.util.Comparator;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.graphdb.traversal.Evaluators;

import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery6Result;

import ldbc.db.LdbcUtils;

public class Query6 {

    /**
     * Tag co-occurrence (sixth complex read query).
     * @param db        A database handle
     * @param personId  A person ID
     * @param tagName   A tag
     * @param limit     An upper bound on the size of results returned
     * @return other tags that occur together with given tag
     */
    public static List<LdbcQuery6Result> query(GraphDatabaseService db,
                                               long personId, String tagName,
                                               int limit) {
        List<LdbcQuery6Result> result = new ArrayList<>();

        // Create a priority queue to keep the results sorted and
        // limited to at most 'limit' entries.  To make this work, we
        // inverse the sort order so we know it is safe to remove the
        // entry with highest priority when the queue reaches 'limit +
        // 1' elements.
        Queue<LdbcQuery6Result> queue = new PriorityQueue<>(
            limit,
            new Comparator<LdbcQuery6Result>() {
                @Override
                public int compare(LdbcQuery6Result r1, LdbcQuery6Result r2) {
                    int c1 = r1.postCount();
                    int c2 = r2.postCount();
                    if (c1 == c2) {
                        String name1 = r1.tagName();
                        String name2 = r2.tagName();
                        return name2.compareTo(name1);
                    }
                    else
                        return Integer.compare(c1, c2); // descending
                }
            });

        // A hash mapping a post to its associated count.
        Map<Node, Integer> counts = new HashMap<Node, Integer>();

        // Create a traversal description for the person's friends
        // and friends of friends.
        TraversalDescription knowsTraversal = db.traversalDescription()
            .breadthFirst()
            .relationships(LdbcUtils.EdgeType.KNOWS)
            .evaluator(Evaluators.toDepth(2))
            .evaluator(Evaluators.excludeStartPosition());

        try (Transaction tx = db.beginTx()) {
            Node person = LdbcUtils.findPersonById(db, personId);
            Node tag = LdbcUtils.findTagByName(db, tagName);

            // Iterate over the person's friends and friends of friends.
            for (Node friend : knowsTraversal.traverse(person).nodes()) {

                // Iterate over the friend's messages.
                for (Relationship edgeFromMessage: friend.getRelationships(LdbcUtils.EdgeType.HAS_CREATOR)) {
                    Node message = edgeFromMessage.getStartNode();

                    // Skip comments.
                    if (!LdbcUtils.isMessagePost(message))
                        continue;

                    // Skip post that doesn't contain tag.
                    if (!LdbcUtils.hasMessageTag(db, message, tag))
                        continue;

                    // Iterate over the post's tags.
                    for (Relationship edgeToTag : message.getRelationships(LdbcUtils.EdgeType.HAS_TAG)) {
                        Node otherTag = edgeToTag.getEndNode();

                        // Skip the input tag.
                        if (LdbcUtils.getId(tag) == LdbcUtils.getId(otherTag))
                            continue;

                        // Update the count.
                        if (counts.get(otherTag) == null)
                            counts.put(otherTag, 1);
                        else
                            counts.put(otherTag, counts.get(otherTag) + 1);
                    }
                }
            }

            // Collect the results.
            for (Map.Entry<Node, Integer> e : counts.entrySet()) {
                // Create a new entry.
                LdbcQuery6Result r = new LdbcQuery6Result(LdbcUtils.getName(e.getKey()), e.getValue());

                // Add the entry to the queue.
                queue.add(r);

                // Eliminate the 'highest' priority entry if we have
                // reached the target number of results.
                LdbcQuery6Result rr;
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
