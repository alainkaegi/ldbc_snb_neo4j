/**
 * Helper class for complex read query 3.
 *
 * Copyright © 2016 Alain Kägi
 */

package ldbc.helpers;

import org.neo4j.graphdb.Node;

/**
 * A temporary holder of one query 3 result.
 *
 * We accumulate query 3 results temporarily in a priority queue
 * derived from this class.  We only accumulate enough information
 * either necessary to sort elements or to retrieve additional fields
 * later.
 */
public class Query3SortResult implements Comparable<Query3SortResult> {
    private final Node friend;
    private final long friendId;
    private final long xCount;
    private final long yCount;

    public Query3SortResult(Node friend, long friendId,
                            long xCount, long yCount) {
        this.friend = friend;
        this.friendId = friendId;
        this.xCount = xCount;
        this.yCount = yCount;
    }

    public Node friend() { return friend; }
    public long friendId() { return friendId; }
    public long xCount() { return xCount; }
    public long yCount() { return yCount; }

    /**
     * Define a sort order for our class.
     *
     * It is exactly the reverse of the intended order so we can
     * identify an unwanted element by its 'high' priority.
     */
    public int compareTo(Query3SortResult r) {
        long c1 = this.xCount + this.yCount;
        long c2 = r.xCount() + r.yCount();
        if (c1 == c2) {
            long id1 = this.friendId;
            long id2 = r.friendId();
            return Long.compare(id2, id1);
        }
        else
            return Long.compare(c1, c2); // descending
    }
}
