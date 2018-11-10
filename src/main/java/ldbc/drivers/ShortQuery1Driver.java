/**
 * Driver for short read query 1.
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

import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery1PersonProfileResult;

import ldbc.queries.ShortQuery1;

public class ShortQuery1Driver {

    static String progName = "ShortQuery1Driver";

    /* Graph. */
    static String graphName;
    static GraphDatabaseService db;

    static long personId;

    public static void main(final String[] argv) {

        parseArgs(argv);

        db = new GraphDatabaseFactory().newEmbeddedDatabase(new File(graphName));

        LdbcShortQuery1PersonProfileResult r = ShortQuery1.query(db, personId);

        print(r);
    }

    /**
     * Pretty print the short query 1 result.
     * @param result  Short query 1 result
     */
    static void print(LdbcShortQuery1PersonProfileResult result) {
        if (result == null) {
            System.out.println("  no match");
            return;
        }

        SimpleDateFormat dateFmt = new SimpleDateFormat("yyyy-MM-dd");
        dateFmt.setTimeZone(TimeZone.getTimeZone("UTC"));
        SimpleDateFormat dateTimeFmt = new SimpleDateFormat("EEE MMM dd HH:mm:ss yyyy");
        dateTimeFmt.setTimeZone(TimeZone.getTimeZone("UTC"));

        System.out.println("  " + result.firstName());
        System.out.println("  " + result.lastName());
        System.out.println("  " + dateFmt.format(new Date(result.birthday())));
        System.out.println("  " + result.locationIp());
        System.out.println("  " + result.browserUsed());
        System.out.println("  " + result.cityId());
        System.out.println("  " + result.gender());
        System.out.println("  " + dateTimeFmt.format(new Date(result.creationDate())));
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
        stream.println("Execute short query 1 of LDBC SNB on GRAPH with PERSONID.");
        stream.println("  -h  print this help and exit");
    }
}
