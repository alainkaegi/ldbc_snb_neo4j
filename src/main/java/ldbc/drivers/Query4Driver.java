/**
 * Driver for complex read query 4.
 *
 * Copyright © 2016 Alain Kägi
 */

package ldbc.drivers;

import java.io.PrintStream;
import java.io.File;
import java.io.FileNotFoundException;

import java.util.List;
import java.util.ArrayList;

import java.util.Date;
import java.util.TimeZone;
import java.text.SimpleDateFormat;

import java.util.Scanner;
import java.util.regex.Pattern;
import java.util.regex.MatchResult;

import java.lang.management.*;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery4Result;

import ldbc.queries.Query4;

public class Query4Driver {

    static String progName = "Query4Driver";

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
            // parameter file consists of three numbers separated by '|'.
            File file = new File(parameterFilename);
            Scanner scanner = new Scanner(file);
            scanner.nextLine();
            Pattern pattern = Pattern.compile("(\\d+)\\|(\\d+)\\|(\\d+)");

            // Time sampling.
            ThreadMXBean bean = ManagementFactory.getThreadMXBean();
            long startTime = bean.getCurrentThreadUserTime();
            long startTotal = bean.getCurrentThreadCpuTime();

            // Execute the queries.
            while (scanner.findInLine(pattern) != null) {
                MatchResult match = scanner.match();
                long personId = Long.valueOf(match.group(1));
                Date startDate = new Date(Long.valueOf(match.group(2)));
                int duration = Integer.valueOf(match.group(3));

                List<LdbcQuery4Result> r = Query4.query(db,
                                                        personId,
                                                        startDate.getTime(),
                                                        duration,
                                                        10);

                if (verbose)
                    print(personId, startDate.getTime(), duration, r);

                if (scanner.hasNextLine())
                    scanner.nextLine();
            }

            // Time sampling.
            long endTime = bean.getCurrentThreadUserTime();
            long endTotal = bean.getCurrentThreadCpuTime();

            // Print timing information if requested.
            if (timing) {
                long userTime = (endTime - startTime)/1000;
                long cpuTime = (endTotal - startTotal)/1000;
                long sysTime = cpuTime - userTime;
                float upercent = 100 * (userTime / (float)cpuTime);
                float spercent = 100 * (sysTime / (float)cpuTime);
                System.out.println("Elapsed time is " + cpuTime + " microseconds");
                System.out.println("User time is " + userTime + " microseconds (" + upercent + "%)");
                System.out.println("System time is " + sysTime + " microseconds (" + spercent + "%)");
            }
        }
        catch (FileNotFoundException e) {
            System.err.println(progName + ": " + parameterFilename + ": No such file");
            System.exit(1);
        }
    }

    /**
     * Pretty print the query 4 results.
     * @param personId   Query 4 parameter 1
     * @param startDate  Query 4 parameter 2
     * @param duration   Query 4 parameter 3
     * @param results    Query 4 results
     */
    static void print(long personId, long startDate, int duration,
                      List<LdbcQuery4Result> results) {
        SimpleDateFormat dateTimeFmt = new SimpleDateFormat("EEE MMM dd HH:mm:ss yyyy");
        dateTimeFmt.setTimeZone(TimeZone.getTimeZone("UTC"));
        System.out.println(personId
                           + " " + dateTimeFmt.format(startDate)
                           + " " + dateTimeFmt.format(startDate + (long)duration * 24 * 60 * 60 * 1000));

        if (results.size() == 0) {
            System.out.println("  no matches");
            System.out.println("");
            return;
        }

        for (LdbcQuery4Result result : results)
            System.out.println("  " + result.tagName()
                               + " " + result.postCount());
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
        stream.println("Execute query 4 of LDBC SNB on GRAPH one time per line of INPUT-FILE");
        stream.println("");
        stream.println("  -h  print this help and exit");
        stream.println("  -t  time the execution");
        stream.println("  -v  print the result of the query");
    }
}
