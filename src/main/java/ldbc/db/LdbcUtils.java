/**
 * LDBC-specific extensions to Neo4j's interface.
 *
 * Copyright © 2016 Alain Kägi
 */

package ldbc.db;

import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;

import java.util.Iterator;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.graphdb.traversal.Evaluators;

public class LdbcUtils {

    /*
     * Definition:
     * - Message: either a post or a comment
     */

    /**
     * Node labels.
     *
     * Each node is assigned a label.  COUNTRY and CITY are special
     * labels, see the loader for details.
     */
    public enum NodeLabel implements Label {
        CITY,
        COMMENT,
        COUNTRY,
        FORUM,
        ORGANISATION,
        PERSON,
        PLACE,
        POST,
        TAG,
        TAG_CLASS;
    }

    /** Edge types. */
    public enum EdgeType implements RelationshipType {
        CONTAINER_OF,
        HAS_CREATOR,
        HAS_INTEREST,
        HAS_MEMBER,
        HAS_MODERATOR,
        HAS_TAG,
        HAS_TYPE,
        IS_LOCATED_IN,
        IS_PART_OF,
        IS_SUBCLASS_OF,
        KNOWS,
        LIKES,
        REPLY_OF,
        STUDY_AT,
        WORKS_AT;
    }

    /**
     * Keys to the values stored with each node and some edges.
     *
     * The key "language" is overloaded.  For a person it consists of
     * an array of strings; for a post it is a single string.  I chose
     * not to change the loader.  I do, however, define two distinct
     * keys (assigned the same string) and two distinct accessors
     * functions.  These accessors are, for a person, getLanguages()
     * and, for a post, getLanguage().
     */
    public static class Keys {
        public static final String ID = "id";
        public static final String FIRSTNAME = "firstName";
        public static final String LASTNAME = "lastName";
        public static final String GENDER = "gender";
        public static final String BIRTHDAY = "birthday";
        public static final String CREATIONDATE = "creationDate";
        public static final String LOCATIONIP = "locationIP";
        public static final String BROWSERUSED = "browserUsed";
        public static final String EMAILS = "email";
        public static final String LANGUAGES = "language";
        public static final String IMAGEFILE = "imageFile";
        public static final String LANGUAGE = "language";
        public static final String CONTENT = "content";
        public static final String LENGTH = "length";
        public static final String TITLE = "title";
        public static final String TYPE = "type";
        public static final String NAME = "name";
        public static final String CLASSYEAR = "classYear";
        public static final String WORKFROM = "workFrom";
        public static final String JOINDATE = "joinDate";
    }

    /* Node properties */
    public static long getId(Node node) { return (long)node.getProperty(Keys.ID); }
    public static String getFirstName(Node person) { return person.getProperty(Keys.FIRSTNAME).toString(); }
    public static String getLastName(Node person) { return person.getProperty(Keys.LASTNAME).toString(); }
    public static String getGender(Node person) { return person.getProperty(Keys.GENDER).toString(); }
    public static long getBirthday(Node person) { return (long)person.getProperty(Keys.BIRTHDAY); }
    public static long getCreationDate(Node node) { return (long)node.getProperty(Keys.CREATIONDATE); }
    public static String getLocationIp(Node node) { return node.getProperty(Keys.LOCATIONIP).toString(); }
    public static String getBrowserUsed(Node node) { return node.getProperty(Keys.BROWSERUSED).toString(); }
    public static String getLanguage(Node post) { return post.getProperty(Keys.LANGUAGE).toString(); }
    public static String getTitle(Node forum) { return forum.getProperty(Keys.TITLE).toString(); }
    public static String getType(Node place) { return place.getProperty(Keys.TYPE).toString(); }
    public static String getName(Node node) { return node.getProperty(Keys.NAME).toString(); }

    public static ArrayList<String> getEmails(Node person) {
        if (person.hasProperty(Keys.EMAILS))
            return new ArrayList<String>(Arrays.asList((String [])person.getProperty(Keys.EMAILS)));
        return new ArrayList<String>();
    }

    public static ArrayList<String> getLanguages(Node person) {
        if (person.hasProperty(Keys.LANGUAGES))
            return new ArrayList<String>(Arrays.asList((String [])person.getProperty(Keys.LANGUAGES)));
        return new ArrayList<String>();
    }

    /**
     * Get content of a message node.
     * @return the content field or the image file field if content is empty
     */
    public static String getContent(Node message) {
        String content = message.getProperty(Keys.CONTENT).toString();
        if (content.length() == 0)
            content = message.getProperty(Keys.IMAGEFILE).toString();
        return content;
    }

    /* Edge properties */
    public static int getClassYear(Relationship edge) { return (int)edge.getProperty(Keys.CLASSYEAR); }
    public static int getWorkFrom(Relationship edge) { return (int)edge.getProperty(Keys.WORKFROM); }
    public static long getCreationDate(Relationship edge) { return (long)edge.getProperty(Keys.CREATIONDATE); }

    /* "Graph" properties */
    /**
     * Get the place assigned to a node.
     * @return the node's place through the isLocatedIn edge
     * A city for a person or a school; a country for an organization
     * or a message.
     */
    public static String getPlace(GraphDatabaseService db, Node node) {
        return getName(node.getSingleRelationship(
                                EdgeType.IS_LOCATED_IN,
                                Direction.OUTGOING).getEndNode());
    }

    /**
     * Collect all the schools attended by the given person.
     * @return the schools, class years, and locations
     */
    public static List<List<Object>> getSchools(GraphDatabaseService db, Node person) {
        List<List<Object>> allSchools = new ArrayList<>();
        Iterable<Relationship> allEdgesToSchools = person.getRelationships(
                                                              EdgeType.STUDY_AT,
                                                              Direction.OUTGOING);
        for (Relationship edgeToSchool : allEdgesToSchools) {
            ArrayList<Object> aSchool = new ArrayList<>();
            Node school = edgeToSchool.getEndNode();
            aSchool.add(getName(school));
            aSchool.add(getClassYear(edgeToSchool));
            aSchool.add(getPlace(db, school));
            allSchools.add(aSchool);
        }
        return allSchools;
    }

    /**
     * Collect all the organizations which employed the given person.
     * @return the organizations, start years, and locations
     */
    public static List<List<Object>> getOrganizations(GraphDatabaseService db, Node person) {
        List<List<Object>> allOrganizations = new ArrayList<>();
        Iterable<Relationship> allEdgesToOrganizations = person.getRelationships(
                                                                    EdgeType.WORKS_AT,
                                                                    Direction.OUTGOING);
        for (Relationship edgeToOrganization : allEdgesToOrganizations) {
            ArrayList<Object> anOrganization = new ArrayList<>();
            Node organization = edgeToOrganization.getEndNode();
            anOrganization.add(getName(organization));
            anOrganization.add(getWorkFrom(edgeToOrganization));
            anOrganization.add(getPlace(db, organization));
            allOrganizations.add(anOrganization);
        }
        return allOrganizations;
    }

    /* Node labels */
    /**
     * Is the message a post?
     */
    public static boolean isMessagePost(Node message) {
        return message.hasLabel(NodeLabel.POST);
    }

    /* Simple lookups */
    /** @return a node or null if none matches the search criteria */
    public static Node findNodeByLabelAndId(GraphDatabaseService db,
                                            Label label,
                                            long id) {
        ResourceIterator<Node> allMatchingNodes
            = db.findNodesByLabelAndProperty(label, Keys.ID, id).iterator();
        return allMatchingNodes.hasNext() ? allMatchingNodes.next() : null;
    }

    /** @return the node or null if none matches the search criteria */
    public static Node findCityById(GraphDatabaseService db, long id) {
        return findNodeByLabelAndId(db, NodeLabel.CITY, id);
    }

    /** @return the node or null if none matches the search criteria */
    public static Node findCommentById(GraphDatabaseService db, long id) {
        return findNodeByLabelAndId(db, NodeLabel.COMMENT, id);
    }

    /** @return the node or null if none matches the search criteria */
    public static Node findCountryById(GraphDatabaseService db, long id) {
        return findNodeByLabelAndId(db, NodeLabel.COUNTRY, id);
    }

    /** @return the node or null if none matches the search criteria */
    public static Node findCountryByName(GraphDatabaseService db, String name) {
        return DbUtils.findNodeByLabelAndProperty(db, NodeLabel.COUNTRY, Keys.NAME, name);
    }

    /** @return the node or null if none matches the search criteria */
    public static Node findForumById(GraphDatabaseService db, long id) {
        return findNodeByLabelAndId(db, NodeLabel.FORUM, id);
    }

    /** @return the node or null if none matches the search criteria */
    public static Node findOrganizationById(GraphDatabaseService db, long id) {
        return findNodeByLabelAndId(db, NodeLabel.ORGANISATION, id);
    }

    /** @return the node or null if none matches the search criteria */
    public static Node findPersonById(GraphDatabaseService db, long id) {
        return findNodeByLabelAndId(db, NodeLabel.PERSON, id);
    }

    /** @return the node or null if none matches the search criteria */
    public static Node findPostById(GraphDatabaseService db, long id) {
        return findNodeByLabelAndId(db, NodeLabel.POST, id);
    }

    /** @return the node or null if none matches the search criteria */
    public static Node findTagById(GraphDatabaseService db, long id) {
        return findNodeByLabelAndId(db, NodeLabel.TAG, id);
    }

    /** @return the node or null if none matches the search criteria */
    public static Node findTagByName(GraphDatabaseService db, String name) {
        return DbUtils.findNodeByLabelAndProperty(db, NodeLabel.TAG, Keys.NAME, name);
    }

    /** @return the node or null if none matches the search criteria */
    public static Node findTagClassByName(GraphDatabaseService db, String name) {
        return DbUtils.findNodeByLabelAndProperty(db, NodeLabel.TAG_CLASS, Keys.NAME, name);
    }

    /** @return the node or null if none matches the search criteria */
    public static Node findMessageById(GraphDatabaseService db, long id) {
        Node message = findCommentById(db, id);
        if (message == null)
            message = findPostById(db, id);
        return message;
    }

    /* Complex lookups */
    /**
     * Look up a neighbor following an edge of a certain type and direction.
     * @return a neighbor or null if no such edge exists
     */
    public static Node findNeighbor(GraphDatabaseService db,
                                    Node startNode,
                                    RelationshipType edgeType,
                                    Direction edgeDirection) {
        Iterator<Relationship> allEdgesFromStartNode
            = startNode.getRelationships(edgeDirection, edgeType).iterator();
        if (!allEdgesFromStartNode.hasNext())
            return null;
        switch (edgeDirection) {
        case OUTGOING:
            return allEdgesFromStartNode.next().getEndNode();
        case INCOMING:
            return allEdgesFromStartNode.next().getStartNode();
        case BOTH:
            Node oneEnd = allEdgesFromStartNode.next().getStartNode();
            if (getId(oneEnd) == getId(startNode))
                return allEdgesFromStartNode.next().getEndNode();
            else
                return oneEnd;
        }
        return null; // To keep the compiler quiet.
    }

    /**
     * Look up the country from which the message was posted.
     *
     * A message node always points directly to a country.
     */
    public static Node findCountryOfMessage(GraphDatabaseService db, Node message) {
        return findNeighbor(db, message, EdgeType.IS_LOCATED_IN, Direction.OUTGOING);
    }

    /**
     * Look up the country in which an organization is located.
     *
     * An organization node always points directly to a country.
     */
    public static Node findCountryOfOrganization(GraphDatabaseService db, Node organization) {
        return findNeighbor(db, organization, EdgeType.IS_LOCATED_IN, Direction.OUTGOING);
    }

    /**
     * Look up the country in which the person is located.
     *
     * A person node always point to a city which points to a country.
     */
    public static Node findCountryOfPerson(GraphDatabaseService db, Node person) {
        return findNeighbor(db,
                            findNeighbor(db,
                                         person,
                                         EdgeType.IS_LOCATED_IN,
                                         Direction.OUTGOING),
                            EdgeType.IS_PART_OF, Direction.OUTGOING);
    }

    /** @return the creator of a message or null if none can be found */
    public static Node findCreatorOfMessage(GraphDatabaseService db, Node message) {
        return findNeighbor(db, message, EdgeType.HAS_CREATOR, Direction.OUTGOING);
    }

    /** @return the forum containing the post. */
    public static Node findForumOfPost(GraphDatabaseService db, Node post) {
        return findNeighbor(db, post, EdgeType.CONTAINER_OF, Direction.INCOMING);
    }

    /**
     * Look up the original post of the given messageId.
     *
     * The message ID may correspond to a post in which case it is the
     * original post.
     */
    public static Node findOriginalPostOfMessage(GraphDatabaseService db, long messageId) {
        Node message = findCommentById(db, messageId);
        if (message == null)
            return findPostById(db, messageId);
        else
            return DbUtils.findProgenitor(db, message, EdgeType.REPLY_OF, Direction.OUTGOING);
    }

    /* Connectivity questions */
    /** @return does the given start node have candidate as neighbor? */
    public static boolean hasNeighbor(GraphDatabaseService db,
                                      Node startNode,
                                      Node possibleNeighbor,
                                      RelationshipType edgeType,
                                      Direction edgeDirection) {
        Iterable<Relationship> allEdges = startNode.getRelationships(edgeDirection, edgeType);
        switch (edgeDirection) {
        case OUTGOING:
            for (Relationship edgeToCandidateNeighbor : allEdges) {
                Node candidateNeighbor = edgeToCandidateNeighbor.getEndNode();
                if (getId(candidateNeighbor) == getId(possibleNeighbor))
                    return true;
            }
            break;
        case INCOMING:
            for (Relationship edgeToCandidateNeighbor : allEdges) {
                Node candidateNeighbor = edgeToCandidateNeighbor.getStartNode();
                if (getId(candidateNeighbor) == getId(possibleNeighbor))
                    return true;
            }
            break;
        case BOTH:
            for (Relationship edgeToCandidateNeighbor : allEdges) {
                Node candidateNeighbor = edgeToCandidateNeighbor.getStartNode();
                if (getId(candidateNeighbor) == getId(startNode))
                    candidateNeighbor = edgeToCandidateNeighbor.getEndNode();
                if (getId(candidateNeighbor) == getId(possibleNeighbor))
                    return true;
            }
            break;
        }
        return false;
    }

    /** @return true if a series of edges connects the child to the parent */
    public static boolean isNodeDescendantOfAncestor(GraphDatabaseService db,
                                                     Node node,
                                                     Node ancestor,
                                                     RelationshipType edgeType,
                                                     Direction edgeDirection) {
        TraversalDescription isDescendantTraversal = db.traversalDescription()
            .depthFirst()
            .relationships(edgeType, edgeDirection);
        for (Node candidate : isDescendantTraversal.traverse(node).nodes())
            if (getId(candidate) == getId(ancestor))
                return true;
        return false;
    }

    /** @return true if the message contains tag */
    public static boolean hasMessageTag(GraphDatabaseService db,
                                        Node message,
                                        Node tag) {
        return hasNeighbor(db, message, tag, EdgeType.HAS_TAG, Direction.OUTGOING);
    }

    /** @return true if person 1 knows person 2 */
    public static boolean areTheyFriend(GraphDatabaseService db,
                                        Node person1,
                                        Node person2) {
        return hasNeighbor(db, person1, person2, EdgeType.KNOWS, Direction.BOTH);
    }

    /* Add nodes */
    /**
     * Add a comment node with properties.
     * @return the new node
     */
    public static Node createComment(GraphDatabaseService db,
                                     Map<String, Object> keyValuePairs) {
        return DbUtils.createNode(db, NodeLabel.COMMENT, keyValuePairs);
    }

    /**
     * Add a forum node with properties.
     * @return the new node
     */
    public static Node createForum(GraphDatabaseService db,
                                   Map<String, Object> keyValuePairs) {
        return DbUtils.createNode(db, NodeLabel.FORUM, keyValuePairs);
    }

    /**
     * Add a person node with properties.
     * @return the new node
     */
    public static Node createPerson(GraphDatabaseService db,
                                    Map<String, Object> keyValuePairs) {
        return DbUtils.createNode(db, NodeLabel.PERSON, keyValuePairs);
    }

    /**
     * Add a Post node with properties.
     * @return the new node
     */
    public static Node createPost(GraphDatabaseService db,
                                  Map<String, Object> keyValuePairs) {
        return DbUtils.createNode(db, NodeLabel.POST, keyValuePairs);
    }

    /* Add edges */
    /** Add forum is container of message edge. */
    public static void createContainerOfEdge(Node forum, Node message) {
        DbUtils.createEdge(forum, message, EdgeType.CONTAINER_OF);
    }

    /** Add message has creator edge. */
    public static void createHasCreatorEdge(Node message, Node creator) {
        DbUtils.createEdge(message, creator, EdgeType.HAS_CREATOR);
    }

    /** Add forum has member edge with properties. */
    public static void createHasMemberEdge(Node forum, Node member,
                                           Map<String, Object> keyValuePairs) {
        DbUtils.createEdge(forum, member, EdgeType.HAS_MEMBER, keyValuePairs);
    }

    /** Add forum has moderator edge. */
    public static void createHasModeratorEdge(Node forum, Node moderator) {
        DbUtils.createEdge(forum, moderator, EdgeType.HAS_MODERATOR);
    }

    /** Add forum has tag edge. */
    public static void createHasTagEdge(Node forum, Node tag) {
        DbUtils.createEdge(forum, tag, EdgeType.HAS_TAG);
    }

    /** Add message is located in country edge. */
    public static void createIsLocatedInEdge(Node message, Node country) {
        DbUtils.createEdge(message, country, EdgeType.IS_LOCATED_IN);
    }

    /** Add person1 knows person2 edge with properties.  */
    public static void createKnowsEdge(Node person1, Node person2,
                                       Map<String, Object> keyValuePairs) {
        DbUtils.createEdge(person1, person2, EdgeType.KNOWS, keyValuePairs);
    }

    /** Add person likes message edge with properties. */
    public static void createLikesEdge(Node person, Node message,
                                       Map<String, Object> keyValuePairs) {
        DbUtils.createEdge(person, message, EdgeType.LIKES, keyValuePairs);
    }

    /** Add comment replyOf message edge. */
    public static void createReplyOfEdge(Node comment, Node message) {
        DbUtils.createEdge(comment, message, EdgeType.REPLY_OF);
    }

    /** Add person study at school edge with properties. */
    public static void createStudyAtEdge(Node person, Node school,
                                         Map<String, Object> keyValuePairs) {
        DbUtils.createEdge(person, school, EdgeType.STUDY_AT, keyValuePairs);
    }

    /** Add person works at organization edge with properties. */
    public static void createWorkAtEdge(Node person, Node organization,
                                        Map<String, Object> keyValuePairs) {
        DbUtils.createEdge(person, organization, EdgeType.WORKS_AT, keyValuePairs);
    }

}
