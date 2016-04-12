/**
 * Complex read query 11.
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

import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery11Result;

import ldbc.db.LdbcUtils;
import ldbc.helpers.Query11SortResult;

public class Query11 {

    /**
     * Job referral (11th complex read query).
     * @param db           A database handle
     * @param personId     A person ID
     * @param countryName  A country's name
     * @param year         A year
     * @param limit        An upper bound on the size of results returned
     * @return friends who've started a job in that country before that year
     */
    public static List<LdbcQuery11Result> query(GraphDatabaseService db,
                                                long personId,
                                                String countryName, int year,
                                                int limit) {
        List<LdbcQuery11Result> result = new ArrayList<>();

        // Create a priority queue to keep the results sorted and
        // limited to at most 'limit' entries.  To make this work, we
        // inverse the sort order so we know it is safe to remove the
        // entry with highest priority when the queue reaches 'limit +
        // 1' elements.  Also we delay gathering the full set of data
        // until we know a given entry is a keeper.
        Queue<Query11SortResult> queue = new PriorityQueue<>(limit + 1);

        // Create a traversal description for the person's friends and
        // friends of friends.
        TraversalDescription knowsTraversal = db.traversalDescription()
            .breadthFirst()
            .relationships(LdbcUtils.EdgeType.KNOWS)
            .evaluator(Evaluators.toDepth(2))
            .evaluator(Evaluators.excludeStartPosition());

        try (Transaction tx = db.beginTx()) {
            Node person = LdbcUtils.findPersonById(db, personId);
            Node country = LdbcUtils.findCountryByName(db, countryName);
            long countryId = LdbcUtils.getId(country);

            // Iterate over the person's friends and friends of friends.
            for (Node friend : knowsTraversal.traverse(person).nodes()) {

                // Iterate over the friend's organizations.
                for (Relationship edgeToOrganization : friend.getRelationships(LdbcUtils.EdgeType.WORKS_AT)) {
                    // Skip start date on or after year.
                    int startYear = LdbcUtils.getWorkFrom(edgeToOrganization);
                    if (startYear >= year)
                        continue;

                    // Skip organizations not in the country.
                    Node organization = edgeToOrganization.getEndNode();
                    if (LdbcUtils.getId(LdbcUtils.findCountryOfOrganization(db, organization)) != countryId)
                        continue;

                    // Create a new temporary result entry.
                    Query11SortResult r = new Query11SortResult(
                        friend,
                        LdbcUtils.getId(friend),
                        LdbcUtils.getName(organization),
                        startYear);

                    // Add the entry to the queue.
                    queue.add(r);

                    // Eliminate the 'highest' priority entry if we have
                    // reached the target number of results.
                    Query11SortResult rr;
                    if (queue.size() > limit)
                        rr = queue.poll();
                }
            }

            // Copy the results by adding elements at the beginning of
            // the final list filling the additional fields as we go.
            while (queue.size() != 0) {
                Query11SortResult r = queue.poll();
                Node friend = r.friend();
                LdbcQuery11Result s = new LdbcQuery11Result(
                    r.friendId(),
                    LdbcUtils.getFirstName(friend),
                    LdbcUtils.getLastName(friend),
                    r.organizationName(),
                    r.organizationStartYear());
                result.add(0, s);
            }
        }
        return result;
    }

}
