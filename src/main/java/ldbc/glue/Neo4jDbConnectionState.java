/**
 * DB connection manager for this Neo4j LDBC SNB implementation.
 *
 * Copyright © 2016, 2018 Alain Kägi
 */

package ldbc.glue;

import java.io.IOException;
import java.io.File;

import com.ldbc.driver.DbConnectionState;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

public class Neo4jDbConnectionState extends DbConnectionState {

    private GraphDatabaseService client;

    public Neo4jDbConnectionState(String url) {
        client = new GraphDatabaseFactory().newEmbeddedDatabase(new File(url));
    }

    public GraphDatabaseService getClient() {
        return client;
    }

    @Override
    public void close() throws IOException {
    }
}
