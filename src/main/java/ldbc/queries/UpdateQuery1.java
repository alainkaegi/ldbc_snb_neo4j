/**
 * Update query 1.
 *
 * Copyright © 2016 Alain Kägi
 */

package ldbc.queries;

import java.util.Map;
import java.util.HashMap;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.Node;

import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate1AddPerson;

import ldbc.db.LdbcUtils;

public class UpdateQuery1 {

    /**
     * Add a person.
     * @param db          A database handle
     * @param parameters  A person's full description
     */
    public static void query(GraphDatabaseService db,
                             LdbcUpdate1AddPerson parameters) {
        // Prepare the in-node person's properties.
        Map<String, Object> props = new HashMap<>(10);
        props.put(LdbcUtils.Keys.ID, parameters.personId());
        props.put(LdbcUtils.Keys.FIRSTNAME, parameters.personFirstName());
        props.put(LdbcUtils.Keys.LASTNAME, parameters.personLastName());
        props.put(LdbcUtils.Keys.GENDER, parameters.gender());
        props.put(LdbcUtils.Keys.BIRTHDAY, parameters.birthday().getTime());
        props.put(LdbcUtils.Keys.CREATIONDATE, parameters.creationDate().getTime());
        props.put(LdbcUtils.Keys.LOCATIONIP, parameters.locationIp());
        props.put(LdbcUtils.Keys.BROWSERUSED, parameters.browserUsed());
        props.put(LdbcUtils.Keys.LANGUAGES, parameters.languages().toArray(new String[0]));
        props.put(LdbcUtils.Keys.EMAILS, parameters.emails().toArray(new String[0]));

        try (Transaction tx = db.beginTx()) {
            // Add the person and its in-node properties.
            Node person = LdbcUtils.createPerson(db, props);

            // Add link to a place.
            Node city = LdbcUtils.findCityById(db, parameters.cityId());
            LdbcUtils.createIsLocatedInEdge(person, city);

            // Add links to schools.
            for (LdbcUpdate1AddPerson.Organization s : parameters.studyAt()) {
                Node school = LdbcUtils.findOrganizationById(db, s.organizationId());
                Map<String, Object> eProps = new HashMap<>(1);
                eProps.put(LdbcUtils.Keys.CLASSYEAR, s.year());
                LdbcUtils.createStudyAtEdge(person, school, eProps);
            }

            // And add links to organizations.
            for (LdbcUpdate1AddPerson.Organization c : parameters.workAt()) {
                Node organization = LdbcUtils.findOrganizationById(db, c.organizationId());
                Map<String, Object> eProps = new HashMap<>(1);
                eProps.put(LdbcUtils.Keys.WORKFROM, c.year());
                LdbcUtils.createWorkAtEdge(person, organization, eProps);
            }

            tx.success();
        }
    }

}
