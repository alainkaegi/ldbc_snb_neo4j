/**
 * Driver for short read query 3.
 *
 * Copyright © 2016 Alain Kägi
 */

package ldbc.drivers;

import java.io.PrintStream;

import java.util.List;

import java.util.Date;
import java.util.TimeZone;
import java.text.SimpleDateFormat;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery3PersonFriendsResult;

import ldbc.queries.ShortQuery3;

public class ShortQuery3Driver {

    static String progName = "ShortQuery3Driver";

    /* Graph. */
    static String graphName;
    static GraphDatabaseService db;

    static long personId;

    public static void main(final String[] argv) {

        parseArgs(argv);

        db = new GraphDatabaseFactory().newEmbeddedDatabase(graphName);

        List<LdbcShortQuery3PersonFriendsResult> r = ShortQuery3.query(db, personId);

        print(r);
    }

    /**
     * Pretty print the short query 3 results.
     * @param results  Short query 3 results
     */
    static void print(List<LdbcShortQuery3PersonFriendsResult> results) {
        int size = results.size();
        if (size == 0) {
            System.out.println("  no matches");
            return;
        }

        SimpleDateFormat dateTimeFmt = new SimpleDateFormat("EEE MMM dd HH:mm:ss yyyy");
        dateTimeFmt.setTimeZone(TimeZone.getTimeZone("UTC"));

        int i = 0;
        for (LdbcShortQuery3PersonFriendsResult result : results) {
            System.out.println("  " + result.personId());
            System.out.println("  " + result.firstName());
            System.out.println("  " + result.lastName());
            System.out.println("  " + dateTimeFmt.format(new Date(result.friendshipCreationDate())));
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
        stream.println("Execute short query 3 of LDBC SNB on GRAPH with PERSONID.");
        stream.println("  -h  print this help and exit");
    }
}
