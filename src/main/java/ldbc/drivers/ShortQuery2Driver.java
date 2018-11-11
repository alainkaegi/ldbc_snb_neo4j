/**
 * Driver for short read query 2.
 *
 * Copyright © 2016, 2018 Alain Kägi
 */

package ldbc.drivers;

import java.io.PrintStream;
import java.io.File;

import java.util.List;

import java.util.Date;
import java.util.TimeZone;
import java.text.SimpleDateFormat;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery2PersonPostsResult;

import ldbc.queries.ShortQuery2;

public class ShortQuery2Driver {

    static String progName = "ShortQuery2Driver";

    /* Graph. */
    static String graphName;
    static GraphDatabaseService db;

    static long personId;

    public static void main(final String[] argv) {

        parseArgs(argv);

        db = new GraphDatabaseFactory().newEmbeddedDatabase(new File(graphName));

        List<LdbcShortQuery2PersonPostsResult> r = ShortQuery2.query(db, personId, 10);

        print(r);
    }

    /**
     * Pretty print the short query 2 results.
     * @param results  Short query 2 results
     */
    static void print(List<LdbcShortQuery2PersonPostsResult> results) {
        int size = results.size();
        if (size == 0) {
            System.out.println("  no matches");
            return;
        }

        SimpleDateFormat dateTimeFmt = new SimpleDateFormat("EEE MMM dd HH:mm:ss yyyy");
        dateTimeFmt.setTimeZone(TimeZone.getTimeZone("UTC"));

        int i = 0;
        for (LdbcShortQuery2PersonPostsResult result : results) {
            System.out.println("  " + result.messageId());
            System.out.println("  " + result.messageContent());
            System.out.println("  " + dateTimeFmt.format(new Date(result.messageCreationDate())));
            System.out.println("  " + result.originalPostId());
            System.out.println("  " + result.originalPostAuthorId());
            System.out.println("  " + result.originalPostAuthorFirstName());
            System.out.println("  " + result.originalPostAuthorLastName());
            if (++i != size) System.out.println();
        }
    }

    /**
     * Parse the command line arguments.
     * @param argv  Command line arguments
     */
    static void parseArgs(String[] argv) {
        int argi = 0;
        int argc = argv.length;
        while (argi < argc && argv[argi].charAt(0) == '-') {
            if (argv[argi].compareTo("-h") == 0) {
                printUsage(System.out);
                System.exit(0);
            }
            else {
                System.err.println(progName + ": " + argv[argi] + ": Unrecognized option");
                printUsage(System.err);
                System.exit(1);
            }
            argi += 1;
        }

        if (argi + 1 > argc) {
            System.err.println(progName + ": Missing input GRAPH");
            printUsage(System.err);
            System.exit(1);
        }
        graphName = argv[argi];
        argi++;
        if (argi + 1 > argc) {
            System.err.println(progName + ": Missing PERSONID");
            printUsage(System.err);
            System.exit(1);
        }
        try {
            personId = Long.parseLong(argv[argi]);
        }
        catch (NumberFormatException e) {
            System.err.println(progName + ": PERSONID must be an integer");
            System.exit(1);
        }
    }

    /**
     * Print information about how to invoke this program.
     */
    static void printUsage(PrintStream stream) {
        stream.println("Usage: " + progName + " [OPTION]... GRAPH PERSONID");
        stream.println("Execute short query 2 of LDBC SNB on GRAPH with PERSONID.");
        stream.println("  -h  print this help and exit");
    }
}
