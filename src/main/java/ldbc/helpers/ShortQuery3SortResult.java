/**
 * Helper class for short read query 3.
 *
 * Copyright © 2016 Alain Kägi
 */

package ldbc.helpers;

import org.neo4j.graphdb.Node;

/**
 * A temporary holder of one short query 3 result.
 *
 * We accumulate short query 3 results temporarily in a priority queue
 * derived from this class.  We only accumulate enough information
 * either necessary to sort elements or to retrieve additional fields
 * later.
 */
public class ShortQuery3SortResult implements Comparable<ShortQuery3SortResult> {
    private final Node friend;
    private final long friendId;
    private final long friendshipCreationDate;

    public ShortQuery3SortResult(Node friend,
                                 long friendId, long friendshipCreationDate) {
        this.friend = friend;
        this.friendId = friendId;
        this.friendshipCreationDate = friendshipCreationDate;
    }

    public Node friend() { return friend; }
    public long friendId() { return friendId; }
    public long friendshipCreationDate() { return friendshipCreationDate; }

    /**
     * Define a sort order for our class.
     */
    public int compareTo(ShortQuery3SortResult r) {
        long d1 = this.friendshipCreationDate;
        long d2 = r.friendshipCreationDate();
        if (d1 == d2) {
            long id1 = this.friendId;
            long id2 = r.friendId();
            return Long.compare(id1, id2);
        }
        else
            return Long.compare(d2, d1); // descending
    }
}
