/**
 * Helper class for short read query 2.
 *
 * Copyright © 2016 Alain Kägi
 */

package ldbc.helpers;

import org.neo4j.graphdb.Node;

/**
 * A temporary holder of one short query 2 result.
 *
 * We accumulate short query 2 results temporarily in a priority queue
 * derived from this class.  We only accumulate enough information
 * either necessary to sort elements or to retrieve additional fields
 * later.
 */
public class ShortQuery2SortResult implements Comparable<ShortQuery2SortResult> {
    private final Node message;
    private final long messageId;
    private final long messageCreationDate;

    public ShortQuery2SortResult(Node message,
                                 long messageId, long messageCreationDate) {
        this.message = message;
        this.messageId = messageId;
        this.messageCreationDate = messageCreationDate;
    }

    public Node message() { return message; }
    public long messageId() { return messageId; }
    public long messageCreationDate() { return messageCreationDate; }

    /**
     * Define a sort order for our class.
     *
     * It is exactly the reverse of the intended order so we can
     * identify an unwanted element by its 'high' priority.
     */
    public int compareTo(ShortQuery2SortResult r) {
        long d1 = this.messageCreationDate;
        long d2 = r.messageCreationDate();
        if (d1 == d2) {
            long id1 = this.messageId;
            long id2 = r.messageId();
            return Long.compare(id1, id2); // descending
        }
        else
            return Long.compare(d1, d2); // descending
    }
}
