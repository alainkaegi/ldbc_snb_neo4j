/**
 * Complex read query 3.
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

import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery3Result;

import ldbc.db.LdbcUtils;
import ldbc.helpers.Query3SortResult;

public class Query3 {

    /**
     * Friends that have been to countries X and Y (third complex read query).
     * @param db          A database handle
     * @param personId    A person ID
     * @param xCountry    A country name
     * @param yCountry    Another country name
     * @param startDdate  A date in milliseconds since 1/1/1970 00:00:00 GMT
     * @param duration    A duration in days
     * @param limit       An upper bound on the size of results returned
     * @return friends that have been to both countries at a certain time
     */
    public static List<LdbcQuery3Result> query(GraphDatabaseService db,
                                               long personId,
                                               String xCountry,
                                               String yCountry,
                                               long startDate, int duration,
                                               int limit) {
        List<LdbcQuery3Result> result = new ArrayList<>();

        // Create a priority queue to keep the results sorted and
        // limited to at most 'limit' entries.  To make this work, we
        // inverse the sort order so we know it is safe to remove the
        // entry with highest priority when the queue reaches 'limit +
        // 1' elements.  Also we delay gathering the full set of data
        // until we know a given entry is a keeper.
        Queue<Query3SortResult> queue = new PriorityQueue<>(limit + 1);

        // Create a traversal description for the person's friends
        // and friends of friends.
        TraversalDescription knowsTraversal = db.traversalDescription()
            .breadthFirst()
            .relationships(LdbcUtils.EdgeType.KNOWS)
            .evaluator(Evaluators.toDepth(2))
            .evaluator(Evaluators.excludeStartPosition());

        try (Transaction tx = db.beginTx()) {
            Node person = LdbcUtils.findPersonById(db, personId);
            long endDate = startDate + (long)duration * 24 * 60 * 60 * 1000;

            long xCountryId = LdbcUtils.getId(LdbcUtils.findCountryByName(db, xCountry));
            long yCountryId = LdbcUtils.getId(LdbcUtils.findCountryByName(db, yCountry));

            // Iterate over the person's friends and friends of friends.
            for (Node friend : knowsTraversal.traverse(person).nodes()) {
                long xCount = 0;
                long yCount = 0;
                long friendCountryId = LdbcUtils.getId(LdbcUtils.findCountryOfPerson(db, friend));

                // Skip if friend is a national of either xCountry or yCountry.
                if (friendCountryId == xCountryId || friendCountryId == yCountryId)
                    continue;

                // Iterate over the friend's messages skipping those
                // outside the period and add to the results.
                for (Relationship edgeFromMessage : friend.getRelationships(LdbcUtils.EdgeType.HAS_CREATOR)) {
                    Node message = edgeFromMessage.getStartNode();

                    long creationDate = LdbcUtils.getCreationDate(message);
                    if (startDate <= creationDate && creationDate < endDate) {
                        long messageCountryId = LdbcUtils.getId(LdbcUtils.findCountryOfMessage(db, message));

                        if (messageCountryId == xCountryId) xCount++;
                        if (messageCountryId == yCountryId) yCount++;
                    }
                }
                if (xCount + yCount != 0 && xCount != 0 && yCount != 0) {
                    // Create a new temporary result entry.
                    Query3SortResult r = new Query3SortResult(
                        friend,
                        LdbcUtils.getId(friend),
                        xCount,
                        yCount);

                    // Add the entry to the queue.
                    queue.add(r);

                    // Eliminate the 'highest' priority entry if we have
                    // reached the target number of results.
                    Query3SortResult rr;
                    if (queue.size() > limit)
                        rr = queue.poll();
                }
            }

            // Copy the results by adding elements at the beginning of
            // the final list filling the additional fields as we go.
            while (queue.size() != 0) {
                Query3SortResult r = queue.poll();
                Node friend = r.friend();
                LdbcQuery3Result s = new LdbcQuery3Result(
                    r.friendId(),
                    LdbcUtils.getFirstName(friend),
                    LdbcUtils.getLastName(friend),
                    r.xCount(),
                    r.yCount(),
                    r.xCount() + r.yCount());
                result.add(0, s);
            }
        }

        return result;
    }

}
