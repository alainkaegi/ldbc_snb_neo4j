/**
 * Driver for complex read query 11.
 *
 * Copyright © 2016 Alain Kägi
 */

package ldbc.drivers;

import java.io.PrintStream;
import java.io.File;
import java.io.FileNotFoundException;

import java.util.List;
import java.util.ArrayList;

import java.util.Scanner;
import java.util.regex.Pattern;
import java.util.regex.MatchResult;

import java.lang.management.*;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery11Result;

import ldbc.queries.Query11;

public class Query11Driver {

    static String progName = "Query11Driver";

    /* Behavior controls. */
    static boolean timing = false;
    static boolean verbose = false;

    /* Substitution pattern file. */
    static String parameterFilename;

    /* Graph. */
    static String graphName;
    static GraphDatabaseService db;

    public static void main(final String[] argv) {

        try {
            // Parse the command line arguments.
            parseArgs(argv);

            // Open the database.
            db = new GraphDatabaseFactory().newEmbeddedDatabase(graphName);

            // Open the parameter file, skip the header, and compile a
            // regular expression pattern describing an input line.
            // Ignoring the header, each line of the substitution
            // parameter file consists of one number, one string (no
            // spaces), and one number all separated by '|'.
            File file = new File(parameterFilename);
            Scanner scanner = new Scanner(file);
            scanner.nextLine();
            Pattern pattern = Pattern.compile("(\\d+)\\|(.+)\\|(\\d+)");

            // Time sampling.
            ThreadMXBean bean = ManagementFactory.getThreadMXBean();
            long startTime = System.nanoTime();
            long threadUserStartTime = bean.getCurrentThreadUserTime();
            long threadTotalStartTime = bean.getCurrentThreadCpuTime();

            // Execute the queries.
            while (scanner.findInLine(pattern) != null) {
                MatchResult match = scanner.match();
                long personId = Long.valueOf(match.group(1));
                String countryName = match.group(2);
                int year = Integer.valueOf(match.group(3));

                List<LdbcQuery11Result> r = Query11.query(db, personId,
                                                          countryName, year, 10);

                if (verbose)
                    print(personId, countryName, year, r);

                if (scanner.hasNextLine())
                    scanner.nextLine();
            }

            // Time sampling.
            long stopTime = System.nanoTime();
            long threadUserStopTime = bean.getCurrentThreadUserTime();
            long threadTotalStopTime = bean.getCurrentThreadCpuTime();

            // Print timing information if requested.
            if (timing) {
                long elapsedTime = (stopTime - startTime)/1000;
                long threadUserTime = (threadUserStopTime - threadUserStartTime)/1000;
                long threadTotalTime = (threadTotalStopTime - threadTotalStartTime)/1000;
                long threadSysTime = threadTotalTime - threadUserTime;
                float tpercent = 100 * (threadTotalTime / (float)elapsedTime);
                float upercent = 100 * (threadUserTime / (float)elapsedTime);
                float spercent = 100 * (threadSysTime / (float)elapsedTime);
                System.out.println("Elapsed time is " + elapsedTime + " microseconds");
                System.out.println("Thread total time is " + threadTotalTime + " microseconds (" + tpercent + "%)");
                System.out.println("Thread user time is " + threadUserTime + " microseconds (" + upercent + "%)");
                System.out.println("Thread system time is " + threadSysTime + " microseconds (" + spercent + "%)");
            }
        }
        catch (FileNotFoundException e) {
            System.err.println(progName + ": " + parameterFilename + ": No such file");
            System.exit(1);
        }
    }

    /**
     * Pretty print the query 11 results.
     * @param personId     Query 11 parameter 1
     * @param countryName  Query 11 parameter 2
     * @param year         Query 11 parameter 3
     * @param results      Query 11 results
     */
    static void print(long personId, String countryName, int year, List<LdbcQuery11Result> results) {
        System.out.println(personId + " " + countryName + " " + year);

        if (results.size() == 0) {
            System.out.println("  no matches");
            System.out.println("");
            return;
        }

        for (LdbcQuery11Result result : results) {
            System.out.println("  " + result.personId()
                               + ", " + result.personFirstName()
                               + " " + result.personLastName()
                               + ", " + result.organizationName()
                               + ", " + result.organizationWorkFromYear());
        }

        System.out.println("");
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
            else if (argv[argi].compareTo("-t") == 0) {
                timing = true;
            }
            else if (argv[argi].compareTo("-v") == 0) {
                verbose = true;
            }
            else {
                System.err.println(progName + ": " + argv[argi] + ": Unrecognized option");
                printUsage(System.err);
                System.exit(1);
            }
            argi += 1;
        }
        if (argi + 1 > argc) {
            System.err.println(progName + ": Missing input graph");
            printUsage(System.err);
            System.exit(1);
        }
        graphName = argv[argi];
        argi++;
        if (argi + 1 > argc) {
            System.err.println(progName + ": Missing substitution parameter filename");
            printUsage(System.err);
            System.exit(1);
        }
        parameterFilename = argv[argi];
    }

    /**
     * Print information about how to invoke this program.
     */
    static void printUsage(PrintStream stream) {
        stream.println("Usage: " + progName + " [OPTION]... GRAPH INPUT-FILE");
        stream.println("Execute query 11 of LDBC SNB on GRAPH one time per line of INPUT-FILE");
        stream.println("");
        stream.println("  -h  print this help and exit");
        stream.println("  -t  time the execution");
        stream.println("  -v  print the result of the query");
    }
}
