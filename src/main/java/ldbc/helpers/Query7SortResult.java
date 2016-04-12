/**
 * Helper class for complex read query 7.
 *
 * Copyright © 2016 Alain Kägi
 */

package ldbc.helpers;

import org.neo4j.graphdb.Node;

/**
 * A temporary holder of one query 7 result.
 *
 * We accumulate query 7 results temporarily in a priority queue
 * derived from this class.  We only accumulate enough information
 * either necessary to sort elements or to retrieve additional fields
 * later.
 */
public class Query7SortResult implements Comparable<Query7SortResult> {
    private final Node liker;
    private final Node message;
    private final long likerId;
    private final long likesCreationDate;
    private final int latency;

    public Query7SortResult(Node liker, Node message, long likerId,
                            long likesCreationDate, int latency) {
        this.liker = liker;
        this.message = message;
        this.likerId = likerId;
        this.likesCreationDate = likesCreationDate;
        this.latency = latency;
    }

    public Node liker() { return liker; }
    public Node message() { return message; }
    public long likerId() { return likerId; }
    public long likesCreationDate() { return likesCreationDate; }
    public int latency() { return latency; }

    /**
     * Define a sort order for our class.
     *
     * It is exactly the reverse of the intended order so we can
     * identify an unwanted element by its 'high' priority.
     */
    public int compareTo(Query7SortResult r) {
        long d1 = this.likesCreationDate;
        long d2 = r.likesCreationDate();
        if (d1 == d2) {
            long id1 = this.likerId;
            long id2 = r.likerId();
            return Long.compare(id2, id1);
        }
        else
            return Long.compare(d1, d2); // descending
    }
}
