/**
 * Extensions to Neo4j's interface.
 *
 * Copyright © 2016 Alain Kägi
 */

package ldbc.db;

import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.graphdb.traversal.Evaluators;

public class DbUtils {

    /**
     * Look up a node based on its label and one of its properties.
     * @return a node or null if none matches the search criteria
     * If more than one node exists, return one.
     */
    public static Node findNodeByLabelAndProperty(GraphDatabaseService db,
                                                  Label label,
                                                  String propertyKey,
                                                  Object propertyValue)
    {
        ResourceIterator<Node> allMatchingNodes
            = db.findNodes(label, propertyKey, propertyValue);
        return allMatchingNodes.hasNext() ? allMatchingNodes.next() : null;
    }

    /**
     * Look up a progenitor following edges of a certain type and direction.
     * @return the progenitor or start node if no such edge exists
     * From the start node follow edges of the given type and given
     * direction until no such edge exists.  Return the reached node.
     * If at a particular node multiple edges exist, an arbitrary edge
     * will be taken.  If the edges form a cycle, this function's
     * behavior is undefined and may never return.
     */
    public static Node findProgenitor(GraphDatabaseService db,
                                      Node startNode,
                                      RelationshipType edgeType, Direction edgeDirection) {
        TraversalDescription progenitorTraversal = db.traversalDescription()
            .depthFirst()
            .relationships(edgeType, edgeDirection)
            .evaluator(Evaluators.excludeStartPosition());
        Node candidateProgenitor = startNode;
        for (Node nextNode : progenitorTraversal.traverse(startNode).nodes())
            candidateProgenitor = nextNode;
        return candidateProgenitor;
    }

    /**
     * Add a node with properties.
     * @return the new node
     */
    public static Node createNode(GraphDatabaseService db,
                                  Label label,
                                  Map<String, Object> keyValuePairs) {
        Node newNode = db.createNode();
        newNode.addLabel(label);
        for (Map.Entry<String, Object> keyValuePair : keyValuePairs.entrySet())
            newNode.setProperty(keyValuePair.getKey(), keyValuePair.getValue());
        return newNode;
    }

    /** Add a directed, typed edge from source to destination. */
    public static void createEdge(Node source,
                                  Node destination,
                                  RelationshipType edgeType) {
        source.createRelationshipTo(destination, edgeType);
    }

    /** Add a directed, typed edge from source to destination with properties. */
    public static void createEdge(Node source,
                                  Node destination,
                                  RelationshipType edgeType,
                                  Map<String, Object> keyValuePairs) {
        Relationship newEdge = source.createRelationshipTo(destination, edgeType);
        for (Map.Entry<String, Object> keyValuePair : keyValuePairs.entrySet())
            newEdge.setProperty(keyValuePair.getKey(), keyValuePair.getValue());
    }

}
