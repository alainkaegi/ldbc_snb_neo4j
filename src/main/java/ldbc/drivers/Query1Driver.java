/**
 * Driver for complex read query 1.
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

import java.util.Collections;
import java.util.Comparator;

import java.util.Scanner;
import java.util.regex.Pattern;
import java.util.regex.MatchResult;

import java.lang.management.*;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery1Result;

import ldbc.queries.Query1;

public class Query1Driver {

    static String progName = "Query1Driver";

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
            // parameter file consists of a number and a name
            // separated by '|'.  The name may contain spaces.
            File file = new File(parameterFilename);
            Scanner scanner = new Scanner(file);
            scanner.nextLine();
            Pattern pattern = Pattern.compile("(\\d+)\\|(.+)");

            // Time sampling.
            ThreadMXBean bean = ManagementFactory.getThreadMXBean();
            long startTime = System.nanoTime();
            long threadUserStartTime = bean.getCurrentThreadUserTime();
            long threadTotalStartTime = bean.getCurrentThreadCpuTime();

            // Execute the queries.
            while (scanner.findInLine(pattern) != null) {
                MatchResult match = scanner.match();
                long personId = Long.valueOf(match.group(1));
                String firstName = match.group(2);

                List<LdbcQuery1Result> r = Query1.query(db, personId, firstName, 20);

                if (verbose)
                    print(personId, firstName, r);

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
     * Pretty print the query 1 results.
     * @param personId   Query 1 parameter 1
     * @param firstName  Query 1 parameter 2
     * @param results    Query 1 results
     * Results are sorted beyond what is performed by the query to
     * allow simple textual comparison with other implementations.
     */
    static void print(long personId, String firstName, List<LdbcQuery1Result> results) {
        System.out.println(personId + " " + firstName);

        if (results.size() == 0) {
            System.out.println("  no matches");
            System.out.println("");
            return;
        }

        SimpleDateFormat dateFmt = new SimpleDateFormat("yyyy-MM-dd");
        dateFmt.setTimeZone(TimeZone.getTimeZone("UTC"));
        SimpleDateFormat dateTimeFmt = new SimpleDateFormat("EEE MMM dd HH:mm:ss yyyy");
        dateTimeFmt.setTimeZone(TimeZone.getTimeZone("UTC"));

        for (LdbcQuery1Result result : results) {
            System.out.println("  " + result.distanceFromPerson()
                               + ", " + result.friendId()
                               + ", " + result.friendLastName()
                               + ", " + dateFmt.format(new Date(result.friendBirthday()))
                               + ", " + dateTimeFmt.format(new Date(result.friendCreationDate()))
                               + ", " + result.friendGender()
                               + ", " + result.friendBrowserUsed()
                               + ", " + result.friendLocationIp());
            for (String email : result.friendEmails())
                System.out.println("    " + email);
            int size = 0;
            List<String> languages = new ArrayList<>();
            for (String language : result.friendLanguages()) {
                languages.add(language);
                size++;
            }
            Collections.sort(languages);
            int count = 0;
            System.out.print("    ");
            for (String language : languages) {
                count++;
                if (count != size)
                    System.out.print(language + ", ");
                else
                    System.out.println(language);
            }
            System.out.println("    " + result.friendCityName());
            List<List<Object>> schools = new ArrayList<>();
            for (List<Object> school : result.friendUniversities())
                schools.add(school);
            Collections.sort(
                schools,
                new Comparator<Object>() {
                    @Override
                    public int compare(Object o1, Object o2) {
                        String name1 = o1.toString();
                        String name2 = o2.toString();
                        return name1.compareTo(name2);
                    }
                });
            for (List<Object> school : schools)
                System.out.println("    " + school.get(0)
                                   + ", " + school.get(1)
                                   + ", " + school.get(2));
            List<List<Object>> organizations = new ArrayList<>();
            for (List<Object> organization : result.friendCompanies())
                organizations.add(organization);
            Collections.sort(
                organizations,
                new Comparator<Object>() {
                    @Override
                    public int compare(Object o1, Object o2) {
                        String name1 = o1.toString();
                        String name2 = o2.toString();
                        return name1.compareTo(name2);
                    }
                });
            for (List<Object> organization : organizations)
                System.out.println("    " + organization.get(0)
                                   + ", " + organization.get(1)
                                   + ", " + organization.get(2));
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
        stream.println("Execute query 1 of LDBC SNB on GRAPH one time per line of INPUT-FILE.");
        stream.println("  -h  print this help and exit");
        stream.println("  -t  time the execution");
        stream.println("  -v  print the results of the queries");
    }
}
