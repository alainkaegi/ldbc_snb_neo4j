/**
 * Helper class for short read query 7.
 *
 * Copyright © 2016 Alain Kägi
 */

package ldbc.helpers;

import org.neo4j.graphdb.Node;

/**
 * A temporary holder of one short query 7 result.
 *
 * We accumulate short query 7 results temporarily in a priority queue
 * derived from this class.  We only accumulate enough information
 * either necessary to sort elements or to retrieve additional fields
 * later.
 */
public class ShortQuery7SortResult implements Comparable<ShortQuery7SortResult> {
    private final Node comment;
    private final Node commentAuthor;
    private final long commentCreationDate;
    private final long commentAuthorId;

    public ShortQuery7SortResult(Node comment, Node commentAuthor,
                                 long commentCreationDate,
                                 long commentAuthorId) {
        this.comment = comment;
        this.commentAuthor = commentAuthor;
        this.commentCreationDate = commentCreationDate;
        this.commentAuthorId = commentAuthorId;
    }

    public Node comment() { return comment; }
    public Node commentAuthor() { return commentAuthor; }
    public long commentCreationDate() { return commentCreationDate; }
    public long commentAuthorId() { return commentAuthorId; }

    /**
     * Define a sort order for our class.
     */
    public int compareTo(ShortQuery7SortResult r) {
        long d1 = this.commentCreationDate;
        long d2 = r.commentCreationDate();
        if (d1 == d2) {
            long id1 = this.commentAuthorId;
            long id2 = r.commentAuthorId();
            return Long.compare(id1, id2);
        }
        else
            return Long.compare(d2, d1); // descending
    }
}
