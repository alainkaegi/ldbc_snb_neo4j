/**
 * Helper class for complex read query 5.
 *
 * Copyright © 2016 Alain Kägi
 */

package ldbc.helpers;

import org.neo4j.graphdb.Node;

/**
 * A data structure to sort the results produced by query 5.
 */
public class Query5SortResult implements Comparable<Query5SortResult> {

    private final Node forum;
    private final long forumId;
    private final int postCount;

    public Query5SortResult(Node forum, long forumId, int postCount) {
        this.forum = forum;
        this.forumId = forumId;
        this.postCount = postCount;
    }

    public Node forum() { return forum; }
    public long forumId() { return forumId; }
    public int postCount() { return postCount; }

    /**
     * Define a sort order for our class.
     *
     * It is exactly the reverse of the intended order so we can
     * identify an unwanted element by its 'high' priority.
     */
    public int compareTo(Query5SortResult r) {
        int c1 = this.postCount;
        int c2 = r.postCount();
        if (c1 == c2) {
            long id1 = this.forumId;
            long id2 = r.forumId();
            return Long.compare(id2, id1);
        }
        else
            return Integer.compare(c1, c2); // descending
    }
}
