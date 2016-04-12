/**
 * Helper class for complex read query 10.
 *
 * Copyright © 2016 Alain Kägi
 */

package ldbc.helpers;

import org.neo4j.graphdb.Node;

/**
 * A temporary holder of one query 10 result.
 *
 * We accumulate query 10 results temporarily in a priority queue
 * derived from this class.  We only accumulate enough information
 * either necessary to sort elements or to retrieve additional fields
 * later.
 */
public class Query10SortResult implements Comparable<Query10SortResult> {
    private final Node friend;
    private final long friendId;
    private final int commonInterestScore;

    public Query10SortResult(Node friend, long friendId,
                             int commonInterestScore) {
        this.friend = friend;
        this.friendId = friendId;
        this.commonInterestScore = commonInterestScore;
    }

    public Node friend() { return friend; }
    public long friendId() { return friendId; }
    public int commonInterestScore() { return commonInterestScore; }

    /**
     * Define a sort order for our class.
     *
     * It is exactly the reverse of the intended order so we can
     * identify an unwanted element by its 'high' priority.
     */
    public int compareTo(Query10SortResult r) {
        int s1 = this.commonInterestScore;
        int s2 = r.commonInterestScore();
        if (s1 == s2) {
            long id1 = this.friendId;
            long id2 = r.friendId();
            return Long.compare(id2, id1);
        }
        else
            return Integer.compare(s1, s2); // descending
    }
}
