/**
 * Helper class for complex read query 11.
 *
 * Copyright © 2016 Alain Kägi
 */

package ldbc.helpers;

import org.neo4j.graphdb.Node;

/**
 * A temporary holder of one query 11 result.
 *
 * We accumulate query 11 results temporarily in a priority queue
 * derived from this class.  We only accumulate enough information
 * either necessary to sort elements or to retrieve additional fields
 * later.
 */
public class Query11SortResult implements Comparable<Query11SortResult> {
    private final Node friend;
    private final long friendId;
    private final String organizationName;
    private final int organizationStartYear;

    public Query11SortResult(Node friend, long friendId,
                             String organizationName,
                             int organizationStartYear) {
        this.friend = friend;
        this.friendId = friendId;
        this.organizationName = organizationName;
        this.organizationStartYear = organizationStartYear;
    }

    public Node friend() { return friend; }
    public long friendId() { return friendId; }
    public String organizationName() { return organizationName; }
    public int organizationStartYear() { return organizationStartYear; }

    /**
     * Define a sort order for our class.
     *
     * It is exactly the reverse of the intended order so we can
     * identify an unwanted element by its 'high' priority.
     */
    public int compareTo(Query11SortResult r) {
        int y1 = this.organizationStartYear;
        int y2 = r.organizationStartYear();
        if (y1 == y2) {
            long id1 = this.friendId;
            long id2 = r.friendId();
            if (id1 == id2) {
                String c1 = this.organizationName;
                String c2 = r.organizationName();
                return c1.compareTo(c2); // descending
            }
            else
                return Long.compare(id2, id1);
        }
        else
            return Integer.compare(y2, y1);
    }
}
