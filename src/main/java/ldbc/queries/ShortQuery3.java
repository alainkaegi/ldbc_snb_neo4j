/**
 * Short read query 3.
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

import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery3PersonFriendsResult;

import ldbc.db.LdbcUtils;
import ldbc.helpers.ShortQuery3SortResult;

public class ShortQuery3 {

    /**
     * Get a person's friends.
     * @param db        A database handle
     * @param personId  A person ID
     * @return the person's friends
     */
    public static List<LdbcShortQuery3PersonFriendsResult> query(GraphDatabaseService db,
                                                                 long personId) {
        List<LdbcShortQuery3PersonFriendsResult> result = new ArrayList<>();

        // Create a priority queue to sort the entries as we
        // accumulate results.
        Queue<ShortQuery3SortResult> queue = new PriorityQueue<>();

        try (Transaction tx = db.beginTx()) {
            Node person = LdbcUtils.findPersonById(db, personId);

            if (person == null) return result;

            // Iterate over the person's friends.
            for (Relationship edgeFromPerson : person.getRelationships(Direction.BOTH, LdbcUtils.EdgeType.KNOWS)) {
                Node friend = edgeFromPerson.getOtherNode(person);

                // Create a new temporary result entry.
                ShortQuery3SortResult r = new ShortQuery3SortResult(
                    friend,
                    LdbcUtils.getId(friend),
                    LdbcUtils.getCreationDate(edgeFromPerson));

                // Add the entry to the queue.
                queue.add(r);
            }

            // Copy the results to the permanent result structure.
            while (queue.size() != 0) {
                ShortQuery3SortResult r = queue.poll();
                Node friend = r.friend();
                LdbcShortQuery3PersonFriendsResult s
                    = new LdbcShortQuery3PersonFriendsResult(
                        r.friendId(),
                        LdbcUtils.getFirstName(friend),
                        LdbcUtils.getLastName(friend),
                        r.friendshipCreationDate());
                result.add(s);
            }
        }

        return result;
    }

}
