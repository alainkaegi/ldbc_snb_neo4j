/**
 * Driver for short read query 6.
 *
 * Copyright © 2016 Alain Kägi
 */

package ldbc.drivers;

import java.io.PrintStream;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery6MessageForumResult;

import ldbc.queries.ShortQuery6;

public class ShortQuery6Driver {

    static String progName = "ShortQuery6Driver";

    /* Graph. */
    static String graphName;
    static GraphDatabaseService db;

    static long messageId;

    public static void main(final String[] argv) {

        parseArgs(argv);

        db = new GraphDatabaseFactory().newEmbeddedDatabase(graphName);

        LdbcShortQuery6MessageForumResult r = ShortQuery6.query(db, messageId);

        print(r);
    }

    /**
     * Pretty print the short query 6 result.
     * @param result  Short query 6 result
     */
    static void print(LdbcShortQuery6MessageForumResult result) {
        if (result == null) {
            System.out.println("  no match");
            return;
        }

        System.out.println("  " + result.forumId());
        System.out.println("  " + result.forumTitle());
        System.out.println("  " + result.moderatorId());
        System.out.println("  " + result.moderatorFirstName());
        System.out.println("  " + result.moderatorLastName());
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
            System.err.println(progName + ": Missing MESSAGEID");
            printUsage(System.err);
            System.exit(1);
        }
        try {
            messageId = Long.parseLong(argv[argi]);
        }
        catch (NumberFormatException e) {
            System.err.println(progName + ": MESSAGEID must be an integer");
            System.exit(1);
        }
    }

    /**
     * Print information about how to invoke this program.
     */
    static void printUsage(PrintStream stream) {
        stream.println("Usage: " + progName + " [OPTION]... GRAPH MESSAGEID");
        stream.println("Execute short query 6 of LDBC SNB on GRAPH with MESSAGEID.");
        stream.println("  -h  print this help and exit");
    }
}
