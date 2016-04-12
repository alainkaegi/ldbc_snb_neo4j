/**
 * Update query 7.
 *
 * Copyright © 2016 Alain Kägi
 */

package ldbc.queries;

import java.util.Map;
import java.util.HashMap;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.Node;

import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate7AddComment;

import ldbc.db.LdbcUtils;

public class UpdateQuery7 {

    /**
     * Add a comment.
     * @param db          A database handle
     * @param parameters  A comment's full description
     */
    public static void query(GraphDatabaseService db,
                             LdbcUpdate7AddComment parameters) {
        // Prepare the in-node comment's properties.
        Map<String, Object> props = new HashMap<>(6);
        props.put(LdbcUtils.Keys.ID, parameters.commentId());
        props.put(LdbcUtils.Keys.CREATIONDATE, parameters.creationDate().getTime());
        props.put(LdbcUtils.Keys.LOCATIONIP, parameters.locationIp());
        props.put(LdbcUtils.Keys.BROWSERUSED, parameters.browserUsed());
        props.put(LdbcUtils.Keys.CONTENT, parameters.content());
        props.put(LdbcUtils.Keys.LENGTH, parameters.length());

        try (Transaction tx = db.beginTx()) {
            // Add the comment and its in-node properties.
            Node comment = LdbcUtils.createComment(db, props);

            // Add a link to the comment's author.
            Node author = LdbcUtils.findPersonById(db, parameters.authorPersonId());
            LdbcUtils.createHasCreatorEdge(comment, author);

            // Add a link to the country where the comment was uploaded.
            Node country = LdbcUtils.findCountryById(db, parameters.countryId());
            LdbcUtils.createIsLocatedInEdge(comment, country);

            // Add link to a previous post or comment.
            long postId = parameters.replyToPostId();
            if (postId != -1) {
                Node post = LdbcUtils.findPostById(db, postId);
                LdbcUtils.createReplyOfEdge(comment, post);
            }
            long earlierCommentId = parameters.replyToCommentId();
            if (earlierCommentId != -1) {
                Node earlierComment = LdbcUtils.findCommentById(db, earlierCommentId);
                LdbcUtils.createReplyOfEdge(comment, earlierComment);
            }

            // Add links to tags.
            for (long tagId : parameters.tagIds()) {
                Node tag = LdbcUtils.findTagById(db, tagId);
                LdbcUtils.createHasTagEdge(comment, tag);
            }

            tx.success();
        }
    }

}
