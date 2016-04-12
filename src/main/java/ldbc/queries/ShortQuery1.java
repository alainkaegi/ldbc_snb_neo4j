/**
 * Short read query 1.
 *
 * Copyright © 2016 Alain Kägi
 */

package ldbc.queries;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Direction;

import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery1PersonProfileResult;

import ldbc.db.LdbcUtils;

public class ShortQuery1 {

    /**
     * Get a person's profile.
     * @param db        A database handle
     * @param personId  A person ID
     * @return the person's profile
     */
    public static LdbcShortQuery1PersonProfileResult query(GraphDatabaseService db,
                                                           long personId) {
        LdbcShortQuery1PersonProfileResult result;

        try (Transaction tx = db.beginTx()) {
            Node person = LdbcUtils.findPersonById(db, personId);

            if (person == null) return null;

            // Find the person's location.
            Node place = LdbcUtils.findNeighbor(
                db,
                person,
                LdbcUtils.EdgeType.IS_LOCATED_IN,
                Direction.OUTGOING);

            // Gather all the info into a result item.
            result = new LdbcShortQuery1PersonProfileResult(
                LdbcUtils.getFirstName(person),
                LdbcUtils.getLastName(person),
                LdbcUtils.getBirthday(person),
                LdbcUtils.getLocationIp(person),
                LdbcUtils.getBrowserUsed(person),
                LdbcUtils.getId(place),
                LdbcUtils.getGender(person),
                LdbcUtils.getCreationDate(person));
        }

        return result;
    }

}
