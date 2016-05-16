/**
 * Complex read query 5.
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

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.graphdb.traversal.Evaluator;
import org.neo4j.graphdb.traversal.Evaluators;

import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery5Result;

import ldbc.db.LdbcUtils;
import ldbc.helpers.Query5SortResult;

public class Query5 {

    /**
     * New groups (fifth complex read query).
     * @param db        A database handle
     * @param personId  A person ID
     * @param date      A date in milliseconds since 1/1/1970 00:00:00 GMT
     * @param limit     An upper bound on the size of results returned
     * @return topics first created in the range provided
     */
    public static List<LdbcQuery5Result> query(GraphDatabaseService db,
                                               long personId, long date,
                                               int limit) {
        List<LdbcQuery5Result> result = new ArrayList<>();

        // Create a priority queue to keep the results sorted and
        // limited to at most 'limit' entries.  To make this work, we
        // inverse the sort order so we know it is safe to remove the
        // entry with highest priority when the queue reaches 'limit +
        // 1' elements.
        Queue<Query5SortResult> queue = new PriorityQueue<>(limit + 1);

        // A hash mapping a friend to a set of forum.
        Map<Node, Set<Node>> forumsOfFriends = new HashMap<Node, Set<Node>>();

        // Maintain a number of posts per forum.
        Map<Node, Integer> counts = new HashMap<Node, Integer>();

        // In a multithreaded environment successive use of the same
        // Neo4j traversal description may yield different results.
        // So we remember our friends in a private hash instead.
        Set<Node> friends = new HashSet<Node>();

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

                friends.add(friend);

                // Iterate over the friend's forums.
                Set<Node> forums = new HashSet<Node>();
                for (Relationship edgeFromForum : friend.getRelationships(LdbcUtils.EdgeType.HAS_MEMBER)) {
                    if ((long)edgeFromForum.getProperty(LdbcUtils.Keys.JOINDATE) <= date)
                        continue;
                    Node forum = edgeFromForum.getStartNode();
                    forums.add(forum);
                    counts.put(forum, 0);
                }
                forumsOfFriends.put(friend, forums);
            }

            // Iterate over these friends again.
            for (Node friend : friends) {

                // And iterate over the friend's posts.
                for (Relationship edgeFromMessage : friend.getRelationships(LdbcUtils.EdgeType.HAS_CREATOR)) {
                    Node message = edgeFromMessage.getStartNode();

                    if (!LdbcUtils.isMessagePost(message))
                        continue;

                    Node forum = LdbcUtils.findForumOfPost(db, message);
                    if (forumsOfFriends.get(friend).contains(forum))
                        counts.put(forum, counts.get(forum) + 1);
                }
            }

            // Collect the results.
            for (Map.Entry<Node, Integer> e : counts.entrySet()) {
                // Create a new temporary result entry.
                Query5SortResult r = new Query5SortResult(
                    e.getKey(),
                    LdbcUtils.getId(e.getKey()),
                    e.getValue());

                // Add the entry to the queue.
                queue.add(r);

                // Eliminate the 'highest' priority entry if we have
                // reached the target number of results.
                Query5SortResult rr;
                if (queue.size() > limit)
                    rr = queue.poll();
            }

            // Copy the results by adding elements at the
            // beginning of the list.
            while (queue.size() != 0) {
                Query5SortResult r = queue.poll();
                LdbcQuery5Result s = new LdbcQuery5Result(
                    LdbcUtils.getTitle(r.forum()),
                    r.postCount());
                result.add(0, s);
            }
        }
        return result;
    }

}
