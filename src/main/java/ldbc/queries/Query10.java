/**
 * Complex read query 10.
 *
 * Copyright © 2016 Alain Kägi
 */

package ldbc.queries;

import java.util.List;
import java.util.ArrayList;
import java.util.Set;
import java.util.HashSet;
import java.util.Queue;
import java.util.PriorityQueue;

import java.util.Date;
import java.util.TimeZone;
import java.util.Calendar;

import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.graphdb.traversal.Evaluators;

import org.neo4j.graphdb.GraphDatabaseService;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery10Result;

import ldbc.db.LdbcUtils;
import ldbc.helpers.Query10SortResult;

public class Query10 {

    /**
     * Friend recommendation (tenth complex read query).
     * @param db        A database handle
     * @param personId  A person ID
     * @param month     A month (between 1 and 12 inclusive)
     * @param limit     An upper bound on the size of results returned
     * @return Recent posts by friends
     */
    public static List<LdbcQuery10Result> query(GraphDatabaseService db,
                                                long personId, int month,
                                                int limit) {
        List<LdbcQuery10Result> result = new ArrayList<>();

        // Create a priority queue to keep the results sorted and
        // limited to at most 'limit' entries.  To make this work, we
        // inverse the sort order so we know it is safe to remove the
        // entry with highest priority when the queue reaches 'limit +
        // 1' elements.  Also we delay gathering the full set of data
        // until we know a given entry is a keeper.
        Queue<Query10SortResult> queue = new PriorityQueue<>(limit + 1);

        // Create a traversal description for the person's friends of
        // friends.
        TraversalDescription knowsTraversal = db.traversalDescription()
            .breadthFirst()
            .relationships(LdbcUtils.EdgeType.KNOWS)
            .evaluator(Evaluators.atDepth(2));

        try (Transaction tx = db.beginTx()) {
            Node person = LdbcUtils.findPersonById(db, personId);

            // The person's set of interests.
            Set<Node> personInterests = new HashSet<Node>();

            // Iterate over the person's interests.
            for (Relationship edgeToTag : person.getRelationships(LdbcUtils.EdgeType.HAS_INTEREST))
                personInterests.add(edgeToTag.getEndNode());

            // Iterate over the person's friends of friends.
            for (Node friend : knowsTraversal.traverse(person).nodes()) {

                // Eliminate friends not born in the month.
                Date birthday = new Date(LdbcUtils.getBirthday(friend));
                Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
                cal.setTime(birthday);
                // Calendar.MONTH ranges from 0 to 11;
                // month ranges from 1 to 12.
                if (!(cal.get(Calendar.MONTH) == month - 1 && cal.get(Calendar.DAY_OF_MONTH) >= 21
                      || cal.get(Calendar.MONTH) == month%12 && cal.get(Calendar.DAY_OF_MONTH) < 22))
                    continue;

                // Compute commonality.
                int common = 0;
                int uncommon = 0;

                // Iterate over the friend's messages.
                for (Relationship edgeFromMessage : friend.getRelationships(LdbcUtils.EdgeType.HAS_CREATOR)) {
                    Node message = edgeFromMessage.getStartNode();

                    // Skip comments.
                    if (!LdbcUtils.isMessagePost(message))
                        continue;

                    // Iterate over the post's tags.
                    boolean notFound = true;
                    for (Relationship edgeToTag : message.getRelationships(LdbcUtils.EdgeType.HAS_TAG)) {
                        Node tag = edgeToTag.getEndNode();

                        if (personInterests.contains(tag)) {
                            notFound = false;
                            break;
                        }
                    }
                    if (notFound)
                        uncommon++;
                    else
                        common++;
                }

                // Create a new temporary result entry.
                Query10SortResult r = new Query10SortResult(
                    friend,
                    LdbcUtils.getId(friend),
                    common - uncommon);

                // Add the entry to the queue.
                queue.add(r);

                // Eliminate the 'highest' priority entry if we have
                // reached the target number of results.
                Query10SortResult rr;
                if (queue.size() > limit)
                    rr = queue.poll();
            }

            // Copy the results by adding elements at the beginning of
            // the final list filling the additional fields as we go.
            while (queue.size() != 0) {
                Query10SortResult r = queue.poll();
                Node friend = r.friend();
                LdbcQuery10Result s = new LdbcQuery10Result(
                    r.friendId(),
                    LdbcUtils.getFirstName(friend),
                    LdbcUtils.getLastName(friend),
                    r.commonInterestScore(),
                    LdbcUtils.getGender(friend),
                    LdbcUtils.getPlace(db, friend));
                result.add(0, s);
            }
        }
        return result;
    }

}
