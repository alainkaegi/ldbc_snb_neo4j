/**
 * Helper class for complex read query 8.
 *
 * Copyright © 2016 Alain Kägi
 */

package ldbc.helpers;

import org.neo4j.graphdb.Node;

/**
 * A temporary holder of one query 8 result.
 *
 * We accumulate query 8 results temporarily in a priority queue
 * derived from this class.  We only accumulate enough information
 * either necessary to sort elements or to retrieve additional fields
 * later.
 */
public class Query8SortResult implements Comparable<Query8SortResult> {
    private final Node replier;
    private final Node comment;
    private final long commentId;
    private final long commentCreationDate;

    public Query8SortResult(Node replier, Node comment, long commentId,
                            long commentCreationDate) {
        this.replier = replier;
        this.comment = comment;
        this.commentId = commentId;
        this.commentCreationDate = commentCreationDate;
    }

    public Node replier() { return replier; }
    public Node comment() { return comment; }
    public long commentId() { return commentId; }
    public long commentCreationDate() { return commentCreationDate; }

    /**
     * Define a sort order for our class.
     *
     * It is exactly the reverse of the intended order so we can
     * identify an unwanted element by its 'high' priority.
     */
    public int compareTo(Query8SortResult r) {
        long d1 = this.commentCreationDate;
        long d2 = r.commentCreationDate();
        if (d1 == d2) {
            long id1 = this.commentId;
            long id2 = r.commentId();
            return Long.compare(id2, id1);
        }
        else
            return Long.compare(d1, d2); // descending
    }
}
