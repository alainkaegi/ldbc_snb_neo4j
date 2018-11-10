/**
 * Driver for short read query 4.
 *
 * Copyright © 2016 Alain Kägi
 */

package ldbc.drivers;

import java.io.PrintStream;
import java.io.File;

import java.util.Date;
import java.util.TimeZone;
import java.text.SimpleDateFormat;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery4MessageContentResult;

import ldbc.queries.ShortQuery4;

public class ShortQuery4Driver {

    static String progName = "ShortQuery4Driver";

    /* Graph. */
    static String graphName;
    static GraphDatabaseService db;

    static long messageId;

    public static void main(final String[] argv) {

        parseArgs(argv);

        db = new GraphDatabaseFactory().newEmbeddedDatabase(new File(graphName));

        LdbcShortQuery4MessageContentResult r = ShortQuery4.query(db, messageId);

        print(r);
    }

    /**
     * Pretty print the short query 4 result.
     * @param result  Short query 4 result
     */
    static void print(LdbcShortQuery4MessageContentResult result) {
        if (result == null) {
            System.out.println("  no match");
            return;
        }

        SimpleDateFormat dateTimeFmt = new SimpleDateFormat("EEE MMM dd HH:mm:ss yyyy");
        dateTimeFmt.setTimeZone(TimeZone.getTimeZone("UTC"));

        System.out.println("  " + result.messageContent());
        System.out.println("  " + dateTimeFmt.format(new Date(result.messageCreationDate())));
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
        stream.println("Execute short query 4 of LDBC SNB on GRAPH with MESSAGEID.");
        stream.println("  -h  print this help and exit");
    }
}
