/**
 * Helper class for complex read query 12.
 *
 * Copyright © 2016 Alain Kägi
 */

package ldbc.helpers;

import org.neo4j.graphdb.Node;

/**
 * A temporary holder of one query 12 result.
 *
 * We accumulate query 12 results temporarily in a priority queue
 * derived from this class.  We only accumulate enough information
 * either necessary to sort elements or to retrieve additional fields
 * later.
 */
public class Query12SortResult implements Comparable<Query12SortResult> {
    private final Node friend;
    private final long friendId;
    private final int replyCount;

    public Query12SortResult(Node friend, long friendId, int replyCount) {
        this.friend = friend;
        this.friendId = friendId;
        this.replyCount = replyCount;
    }

    public Node friend() { return friend; }
    public long friendId() { return friendId; }
    public int replyCount() { return replyCount; }

    /**
     * Define a sort order for our class.
     *
     * It is exactly the reverse of the intended order so we can
     * identify an unwanted element by its 'high' priority.
     */
    public int compareTo(Query12SortResult r) {
        int c1 = this.replyCount;
        int c2 = r.replyCount();
        if (c1 == c2) {
            long id1 = this.friendId;
            long id2 = r.friendId();
            return Long.compare(id2, id1);
        }
        else
            return Integer.compare(c1, c2); // descending
    }
}
