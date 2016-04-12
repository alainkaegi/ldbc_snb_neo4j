/**
 * Update query 6.
 *
 * Copyright © 2016 Alain Kägi
 */

package ldbc.queries;

import java.util.Map;
import java.util.HashMap;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.Node;

import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate6AddPost;

import ldbc.db.LdbcUtils;

public class UpdateQuery6 {

    /**
     * Add a post.
     * @param db          A database handle
     * @param parameters  A post's full description
     */
    public static void query(GraphDatabaseService db,
                             LdbcUpdate6AddPost parameters) {
        // Prepare the in-node post's properties.
        Map<String, Object> props = new HashMap<>(8);
        props.put(LdbcUtils.Keys.ID, parameters.postId());
        props.put(LdbcUtils.Keys.IMAGEFILE, parameters.imageFile());
        props.put(LdbcUtils.Keys.CREATIONDATE, parameters.creationDate().getTime());
        props.put(LdbcUtils.Keys.LOCATIONIP, parameters.locationIp());
        props.put(LdbcUtils.Keys.BROWSERUSED, parameters.browserUsed());
        props.put(LdbcUtils.Keys.LANGUAGE, parameters.language());
        props.put(LdbcUtils.Keys.CONTENT, parameters.content());
        props.put(LdbcUtils.Keys.LENGTH, parameters.length());

        try (Transaction tx = db.beginTx()) {
            // Add the post and its in-node properties.
            Node post = LdbcUtils.createPost(db, props);

            // Add a link to the post's author.
            Node author = LdbcUtils.findPersonById(db, parameters.authorPersonId());
            LdbcUtils.createHasCreatorEdge(post, author);

            // Add a link to the hosting forum.
            Node forum = LdbcUtils.findForumById(db, parameters.forumId());
            LdbcUtils.createContainerOfEdge(forum, post);

            // Add a link to the country where the post was uploaded.
            Node country = LdbcUtils.findCountryById(db, parameters.countryId());
            LdbcUtils.createIsLocatedInEdge(post, country);

            // Add links to tags.
            for (long tagId : parameters.tagIds()) {
                Node tag = LdbcUtils.findTagById(db, tagId);
                LdbcUtils.createHasTagEdge(post, tag);
            }

            tx.success();
        }
    }

}
