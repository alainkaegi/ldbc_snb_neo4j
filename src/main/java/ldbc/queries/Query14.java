/**
 * Complex read query 14.
 *
 * Copyright © 2016 Alain Kägi
 */

package ldbc.queries;

import java.util.List;
import java.util.ArrayList;

import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PathExpanders;
import org.neo4j.graphalgo.GraphAlgoFactory;
import org.neo4j.graphalgo.PathFinder;

import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery14Result;

import ldbc.db.LdbcUtils;

public class Query14 {

    /**
     * Weighted paths (14th complex read query).
     * @param db         A database handle
     * @param person1Id  A first person ID
     * @param person2Id  A second person ID
     * @return weighted shortest paths between person1 and person2.
     */
    public static List<LdbcQuery14Result> query(GraphDatabaseService db,
                                                long person1Id,
                                                long person2Id) {
        List<LdbcQuery14Result> result = new ArrayList<>();

        // Create a path finder for all shortest paths between
        // person1 and person2.
        PathFinder<Path> finder = GraphAlgoFactory.shortestPath(
            PathExpanders.forTypeAndDirection(
                LdbcUtils.EdgeType.KNOWS,
                Direction.BOTH),
            Integer.MAX_VALUE);

        try (Transaction tx = db.beginTx()) {
            Node person1 = LdbcUtils.findPersonById(db, person1Id);
            Node person2 = LdbcUtils.findPersonById(db, person2Id);

            // Go through each path, identify the nodes, and compute
            // their weight.
            for (Path path : finder.findAllPaths(person1, person2)) {
                double weight = 0.0;
                Node prevFriend = null;
                long prevFriendId = -1;
                List<Long> idsInPath = new ArrayList<>();

                // For each node on the path.
                for (Node friend : path.nodes()) {
                    long friendId = LdbcUtils.getId(friend);

                    // If we have a pair of nodes...
                    if (prevFriend != null) {
                        // First look at the relationship previous friend -> friend.

                        // Scan the previous friend's messages.
                        for (Relationship edgeFromMessage : prevFriend.getRelationships(LdbcUtils.EdgeType.HAS_CREATOR)) {
                            Node message = edgeFromMessage.getStartNode();

                            // Skip posts.
                            if (LdbcUtils.isMessagePost(message))
                                continue;

                            // Get the message being replied to.
                            Node parentMessage = LdbcUtils.findNeighbor(db, message, LdbcUtils.EdgeType.REPLY_OF, Direction.OUTGOING);
                            // And its author.
                            Node parentMessageAuthor = LdbcUtils.findNeighbor(db, parentMessage, LdbcUtils.EdgeType.HAS_CREATOR, Direction.OUTGOING);

                            if (LdbcUtils.getId(parentMessageAuthor) == friendId) {
                                if (LdbcUtils.isMessagePost(parentMessage))
                                    weight += 1.0;
                                else
                                    weight += 0.5;
                            }
                        }

                        // Then look at the relationship friend -> previous friend.
                        // Scan the current friend's messages.
                        for (Relationship edgeFromMessage :  friend.getRelationships(LdbcUtils.EdgeType.HAS_CREATOR)) {
                            Node message= edgeFromMessage.getStartNode();

                            // Skip posts.
                            if (LdbcUtils.isMessagePost(message))
                                continue;

                            // Get the message being replied to.
                            Node parentMessage = LdbcUtils.findNeighbor(db, message, LdbcUtils.EdgeType.REPLY_OF, Direction.OUTGOING);
                            // And its author.
                            Node parentMessageAuthor = LdbcUtils.findNeighbor(db, parentMessage, LdbcUtils.EdgeType.HAS_CREATOR, Direction.OUTGOING);

                            if (LdbcUtils.getId(parentMessageAuthor) == prevFriendId) {
                                if (LdbcUtils.isMessagePost(parentMessage))
                                    weight += 1.0;
                                else
                                    weight += 0.5;
                            }
                        }
                    }
                    idsInPath.add(friendId);
                    prevFriend = friend;
                    prevFriendId = friendId;
                }
                LdbcQuery14Result r = new LdbcQuery14Result(idsInPath, weight);
                result.add(r);
            }
        }

        // Sort our results.
        Collections.sort(
            result,
            new Comparator<Object>() {
                @Override
                public int compare(Object o1, Object o2) {
                    LdbcQuery14Result r1 = (LdbcQuery14Result)o1;
                    LdbcQuery14Result r2 = (LdbcQuery14Result)o2;
                    double w1 = r1.pathWeight();
                    double w2 = r2.pathWeight();
                    return Double.compare(w2, w1); // Descending
                }
            });

        return result;
    }

}
