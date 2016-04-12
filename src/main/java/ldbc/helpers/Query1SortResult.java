/**
 * Helper class for complex read query 1.
 *
 * Copyright © 2016 Alain Kägi
 */

package ldbc.helpers;

import org.neo4j.graphdb.Node;

/**
 * A temporary holder of one query 1 result.
 *
 * We accumulate query 1 results temporarily in a priority queue
 * derived from this class.  We only accumulate enough information
 * either necessary to sort elements or to retrieve additional fields
 * later.
 */
public class Query1SortResult implements Comparable<Query1SortResult> {
    private final Node friend;
    private final long friendId;
    private final String friendLastName;
    private final int distanceFromFriend;

    public Query1SortResult(Node friend,
                            long friendId, String friendLastName,
                            int distanceFromFriend) {
        this.friend = friend;
        this.friendId = friendId;
        this.friendLastName = friendLastName;
        this.distanceFromFriend = distanceFromFriend;
    }

    public Node friend() { return friend; }
    public long friendId() { return friendId; }
    public String friendLastName() { return friendLastName; }
    public int distanceFromFriend() { return distanceFromFriend; }

    /**
     * Define a sort order for our class.
     *
     * It is exactly the reverse of the intended order so we can
     * identify an unwanted element by its 'high' priority.
     */
    public int compareTo(Query1SortResult r) {
        int d1 = this.distanceFromFriend;
        int d2 = r.distanceFromFriend();
        if (d1 == d2) {
            String n1 = this.friendLastName;
            String n2 = r.friendLastName();
            if (n1.compareToIgnoreCase(n2) == 0) {
                Long id1 = this.friendId;
                Long id2 = r.friendId();
                return id2.compareTo(id1);
            }
            else
                return n2.compareToIgnoreCase(n1);
        }
        else
            return Integer.compare(d2, d1);
    }
}
