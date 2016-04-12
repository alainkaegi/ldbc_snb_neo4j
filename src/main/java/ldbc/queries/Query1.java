/**
 * Complex read query 1.
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
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.graphdb.traversal.Evaluator;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.graphdb.traversal.Evaluation;

import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery1Result;

import ldbc.db.LdbcUtils;
import ldbc.helpers.Query1SortResult;

public class Query1 {

    /**
     * Friends with certain name (first complex read query).
     * @param db         A database handle
     * @param personId   A person ID
     * @param firstName  A first name
     * @param limit      An upper bound on the size of results returned
     * @return up to 'limit' friends with the given first name
     */
    public static List<LdbcQuery1Result> query(GraphDatabaseService db,
                                               long personId,
                                               final String firstName,
                                               int limit) {
        List<LdbcQuery1Result> result = new ArrayList<>();

        // Create a priority queue to keep the results sorted and
        // limited to at most 'limit' entries.  To make this work, we
        // inverse the sort order so we know it is safe to remove the
        // entry with highest priority when the queue reaches 'limit +
        // 1' elements.  Also we delay gathering the full set of data
        // until we know a given entry is a keeper.
        Queue<Query1SortResult> queue = new PriorityQueue<>(limit + 1);

        // Create a traversal description for a person's circle of
        // friends (up to 3 relations away).
        TraversalDescription knowsTraversal = db.traversalDescription()
            .breadthFirst()
            .relationships(LdbcUtils.EdgeType.KNOWS)
            .evaluator(Evaluators.toDepth(3))
            .evaluator(Evaluators.excludeStartPosition())
            .evaluator(new Evaluator()
                {
                    @Override
                    public Evaluation evaluate(final Path path) {
                        Node n = path.endNode();
                        if (n.getProperty(LdbcUtils.Keys.FIRSTNAME).equals(firstName))
                            return Evaluation.INCLUDE_AND_CONTINUE;
                        return Evaluation.EXCLUDE_AND_CONTINUE;
                    }
                });

        try (Transaction tx = db.beginTx()) {
            Node person = LdbcUtils.findPersonById(db, personId);

            // Iterate over the person's circle of friends.
            for (Path pathToFriend : knowsTraversal.traverse(person)) {
                Node friend = pathToFriend.endNode();
                int distance = pathToFriend.length();

                // Create a new temporary result entry.
                Query1SortResult r = new Query1SortResult(
                    friend,
                    LdbcUtils.getId(friend),
                    LdbcUtils.getLastName(friend),
                    distance);

                // Add the entry to the queue.
                queue.add(r);

                // Eliminate the 'highest' priority entry if we have
                // reached the target number of results.
                Query1SortResult rr;
                if (queue.size() > limit)
                    rr = queue.poll();
            }

            // Copy the results by adding elements at the beginning of
            // the final list filling the additional fields as we go.
            while (queue.size() != 0) {
                Query1SortResult r = queue.poll();
                Node friend = r.friend();
                LdbcQuery1Result s = new LdbcQuery1Result(
                    r.friendId(),
                    r.friendLastName(),
                    r.distanceFromFriend(),
                    LdbcUtils.getBirthday(friend),
                    LdbcUtils.getCreationDate(friend),
                    LdbcUtils.getGender(friend),
                    LdbcUtils.getBrowserUsed(friend),
                    LdbcUtils.getLocationIp(friend),
                    LdbcUtils.getEmails(friend),
                    LdbcUtils.getLanguages(friend),
                    LdbcUtils.getPlace(db, friend),
                    LdbcUtils.getSchools(db, friend),
                    LdbcUtils.getOrganizations(db, friend));
                result.add(0, s);
            }
        }
        return result;
    }

}
