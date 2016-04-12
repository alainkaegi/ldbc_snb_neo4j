/**
 * Helper class for complex read query 9.
 *
 * Copyright © 2016 Alain Kägi
 */

package ldbc.helpers;

import org.neo4j.graphdb.Node;

/**
 * A temporary holder of one query 9 result.
 *
 * We accumulate query 9 results temporarily in a priority queue
 * derived from this class.  We only accumulate enough information
 * either necessary to sort elements or to retrieve additional fields
 * later.
 */
public class Query9SortResult implements Comparable<Query9SortResult> {
    private final Node friend;
    private final Node message;
    private final long messageId;
    private final long messageCreationDate;

    public Query9SortResult(Node friend, Node message,
                            long messageId, long messageCreationDate) {
        this.friend = friend;
        this.message = message;
        this.messageId = messageId;
        this.messageCreationDate = messageCreationDate;
    }

    public Node friend() { return friend; }
    public Node message() { return message; }
    public long messageId() { return messageId; }
    public long messageCreationDate() { return messageCreationDate; }

    /**
     * Define a sort order for our class.
     *
     * It is exactly the reverse of the intended order so we can
     * identify an unwanted element by its 'high' priority.
     */
    public int compareTo(Query9SortResult r) {
        long d1 = this.messageCreationDate;
        long d2 = r.messageCreationDate();
        if (d1 == d2) {
            long id1 = this.messageId;
            long id2 = r.messageId();
            return Long.compare(id2, id1);
        }
        else
            return Long.compare(d1, d2); // descending
    }
}
