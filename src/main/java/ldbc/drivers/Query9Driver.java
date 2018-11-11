/**
 * Driver for complex read query 9.
 *
 * Copyright © 2016, 2018 Alain Kägi
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

import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery9Result;

import ldbc.queries.Query9;

public class Query9Driver {

    static String progName = "Query9Driver";

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
            db = new GraphDatabaseFactory().newEmbeddedDatabase(new File(graphName));

            // Open the parameter file, skip the header, and compile a
            // regular expression pattern describing an input line.
            // Ignoring the header, each line of the substitution
            // parameter file consists of two numbers separated by '|'.
            File file = new File(parameterFilename);
            Scanner scanner = new Scanner(file);
            scanner.nextLine();
            Pattern pattern = Pattern.compile("(\\d+)\\|(\\d+)");

            // Time sampling.
            ThreadMXBean bean = ManagementFactory.getThreadMXBean();
            long startTime = System.nanoTime();
            long threadUserStartTime = bean.getCurrentThreadUserTime();
            long threadTotalStartTime = bean.getCurrentThreadCpuTime();

            // Execute the queries.
            while (scanner.findInLine(pattern) != null) {
                MatchResult match = scanner.match();
                long personId = Long.valueOf(match.group(1));
                long date = Long.valueOf(match.group(2));

                List<LdbcQuery9Result> r = Query9.query(db, personId, date, 20);

                if (verbose)
                    print(personId, date, r);

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
     * Pretty print the query 9 results.
     * @param personId  Query 9 parameter 1
     * @param date      Query 9 parameter 2
     * @param results   Query 9 results
     */
    static void print(long personId, long date, List<LdbcQuery9Result> results) {
        SimpleDateFormat dateTimeFmt = new SimpleDateFormat("EEE MMM dd HH:mm:ss yyyy");
        dateTimeFmt.setTimeZone(TimeZone.getTimeZone("UTC"));
        System.out.println(personId + " " + dateTimeFmt.format(date));

        if (results.size() == 0) {
            System.out.println("  no matches");
            System.out.println("");
            return;
        }

        SimpleDateFormat dateTimeZFmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ");
        dateTimeZFmt.setTimeZone(TimeZone.getTimeZone("UTC"));

        for (LdbcQuery9Result result : results) {
            System.out.println("  " + result.personId()
                               + " " + result.personFirstName()
                               + " " + result.personLastName()
                               + " " + result.commentOrPostId()
                               + " " + dateTimeZFmt.format(new Date(result.commentOrPostCreationDate()))
                               + " " + result.commentOrPostContent());
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
        stream.println("Execute query 9 of LDBC SNB on GRAPH one time per line of INPUT-FILE");
        stream.println("");
        stream.println("  -h  print this help and exit");
        stream.println("  -t  time the execution");
        stream.println("  -v  print the result of the query");
    }
}
