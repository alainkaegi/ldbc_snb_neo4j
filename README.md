This set of source files represents a Neo4j-based implementation of
the LDBC Social Network Benchmark's Interactive Workload.  This
implementation is completely distinct from the implementation
mentioned in the LDBC report specifying the benchmark [1].

Manifest
========

- README.md: this file
- COPYING: license
- ERRATA: known bugs
- build.gradle: build/run script
- src: Java source files
- scripts: scripts for conditioning input and run tests

Expectations
============

This document assumes the reader is familiar with the LDBC Social
Network Benchmark.  Good sources of information include the LDBC
Social Network Benchmark specification [1] and the LDBC SNB Data
Generator project hosted at GitHub, Inc. [2].

Running modes
=============

This distribution supports two running modes.  First and foremost you
can run the LDBC benchmark with its provided driver [3].  However, you
can also run some of the queries individually or in what I call
standalone mode.  That mode might be useful for debugging or
microbenchmarking.  In that mode a query runs on a single thread and
processes input arguments as quickly as possible.  Currently only the
read queries (both complex and short) can be run that way.  To run a
complex query in standalone mode require a substitution parameter-like
file (the same as the ones produced by the LDBC data generator with
the PARAM_GENERATION environment variable set to 1).  Parameters for
the short read queries are passed on the command line.  See below for
details.

Important Parameters
====================

Two key parameters controls the benchmark's behavior.

- Version of Neo4j

  A database created with one version of Neo4j may not be read by
  queries linked against another version.

  When does this parameter come into play?
  - When configuring the data importer
  - When configuring the gradle build file

- Scale factor

  The LDBC Socal Network Benchmark defines several scale factors
  covering a range of system's costs and sizes.

  When does this parameter come into play?
  - When configuring the data generator
  - When configuring a benchmark run

Requirements
============

- gradle version 2.10
- java 1.7
- My version of the Neo4j Data Importer for the LDBC Social Network
  Benchmark [5]
- maven 3.0.5
- hadoop 2.6.0

Instructions
============

I tested these instructions on a Mac mini running Yosemite (version
10.10.5) and Ubuntu running Wily Werewolf (15.10) except for the data
generation phase which I have only performed on the system running
Ubuntu.

These instructions assume that you cloned the various source packages
mentioned here (data generator, benchmark driver, data importer, this
project) in the same directory.  They also assume that in most cases
each project generates data in their own subdirectories.  These
assumptions not need to be the case; it should be straightforward to
choose a different naming discipline.

Edit build.gradle
-----------------

Edit build.gradle to change the version of Neo4j you want to use and
paths to the database and substitution files when running the
individual query drivers.

Install Neo4j
-------------

You should not have to install Neo4j.  Gradle will pick up the package
from maven central.

Generate data
-------------

Generate a dataset using the LDBC SNB data generator [2] with the
scale factor of your choice, the PARAM_GENERATION environment variable
set to 1, and most other parameters left to their default values.  In
the end the goal is to have ../ldbc_snb_datagen/social_network/
holding the CSV files to seed the benchmark graph database and
../ldbc_snb_datagen/substitution_parameters/ containing files used as
input to the LDBC benchmark or to the complex queries run in
standalone mode.  You may have to issue a command like bin/hdfs dfs
-get social_network/ ../ldbc_snb_datagen/social_network/ to retrieve
your data.

Load the database
-----------------

1. Merge the CSV files.  The data generator mentioned in the previous
   section may split entities across multiple files.  Use the provided
   concatenation script to merge the results (e.g., ./scripts/cat.sh
   ../ldbc_snb_datagen/social_network/
   ../ldbc_snb_datagen/social_network_merged/).

2. Prepare the loader.  I use a loader available elsewhere [4] with my
   own set of modifications available in the branch of a fork [5].
   Clone that repository.  Check out the fixes-and-additions branch.
   Edit ../ldbc_socialnet_bm_neo4j/pom.xml to change the key
   'neo4j-version' with the version of Neo4j you are using.  Edit
   ../ldbc_socialnet_bm_neo4j/src/main/resources/ldbc_neo4j.properties
   to change 'data_dir' with the directory containing the merged CSV
   files produced in the previous step (e.g.,
   "../ldbc_snb_datagen/social_network_merged/") and 'db_dir' to
   "../ldbc_socialnet_bm_neo4j/db/".

3. Build the loader (e.g., cd ../ldbc_socialnet_bm_neo4j/;
   ./build.sh).  Note that you must rebuild the project after every
   change to its properties.

4. Load the database (e.g., cd ../ldbc_socialnet_bm_neo4j/;
   mvn exec:java -Dexec.mainClass=com.ldbc.socialnet.workload.neo4j.load.LdbcSocialNeworkNeo4jImporter).
   The database will be located in ../ldbc_socialnet_bm_neo4j/db/.

Install ldbc_driver
-------------------

$ git clone https://github.com/ldbc/ldbc_driver

$ cd ../ldbc_driver/

$ git checkout 0.2

$ mvn clean package -DskipTests

$ mvn install -DskipTests

Build our queries
-----------------

$ gradle build

Prepare a benchmark run
-----------------------

Copy the property file corresponding to the chosen scale factor and
name it ldbc.properties (e.g., cp
../ldbc_driver/workloads/ldbc/snb/interactive/ldbc_snb_interactive_SF-0001.properties
./ldbc.properties).  Copy the update stream property file produced by
the data generation process (e.g, cp
../ldbc_data_gen/social_network/updateStream.properties .).  Edit
./ldbc.properties to configure your benchmark run.  For instance,

- set 'database' to "ldbc.glue.Neo4jDb"

- add 'url' to point to the correct database (e.g.,
  "../ldbc_socialnet_bm_neo4j/db/")

- add 'operation_count' and set it to the desired value

- set 'ldbc.snb.interactive.parameters_dir' to the query parameter
  files produced by the data generation phase (e.g.,
  "../ldbc_snb_datagen/substitution_parameters/")

- set 'ldbc.snb.interactive.updates_dir' to the directory containing
  the update stream files produced by the data generation phase (e.g.,
  "../ldbc_snb_datagen/social_network/")

Run the benchmark
-----------------

$ gradle -q ldbc

Run complex query X
-------------------

$ gradle -q queryX

Run short read query Y
----------------------

where Y is 1, 2, or 3:

$ gradle -q -PpersonId=ID shortqueryY

where Y is 4, 5, 6, or 7:

$ gradle -q -PmessageId=ID shortqueryY

Validate the implementation
---------------------------

Follow the instructions that come with the LDBC SNB interactive
validation project [6].

1. Clone the project in ../ldbc_snb_interactive_validation/.

2. Untar the content of
   ldbc_snb_interactive_validation/neo4j/neo4j--validation_set.tar.gz
   in, say, ../ldbc_snb_interactive_validation/neo4j/e/.

3. Concatenate CSV content of
   ldbc_snb_interactive_validation/neo4j/e/social_network/string_date/
   into ../ldbc_snb_interactive_validation/neo4j/e/social_network/string_date_merged/
   (e.g., ./scripts/cat.sh ../ldbc_snb_interactive_validation/neo4j/e/social_network/string_date/ ../ldbc_snb_interactive_validation/neo4j/e/social_network/string_date_merged/).

4. Load CSV files into a database using my version of the Neo4j Data
   Importer [5] into ../ldbc_socialnet_bm_neo4j/validation_db/.

5. Copy the property file provided with that project and name it
   validation.properties (e.g., cp
   ../ldbc_snb_interactive_validation/neo4j/readwrite_neo4j--ldbc_driver_config--db_validation.properties
   ./validation.properties).  Edit the LDBC driver configuration section
   of validation.properties.  Set 'database' to 'ldbc.glue.Neo4jDb',
   'url' to '../ldbc_socialnet_bm_neo4j/validation_db/',
   'validate_database' to
   '../ldbc_snb_interactive_validation/neo4j/e/validation_params.csv',
   and 'ldbc.snb.interactive.parameters_dir' to
   '../ldbc_snb_interactive_validation/neo4j/e/substitution_parameters/'.

6. Issue the command 'gradle validate'.

7. Important: must reload the validation database from scratch (step
   4) every time you run validation.

References
==========

[1] Arnau Prat (Editor).  LDBC Social Network Benchmark (SNB), v0.2.2
    First Public Draft, release 0.2.2.  Retrieved December 17, 2015.

[2] LDBC SNB Data Generator.
    https://github.com/ldbc/ldbc_snb_datagen, last retrieved March
    18th, 2016, with, then, latest commit 2645cc0.

[3] LDBC SNB Benchmark Driver.  https://github.com/ldbc/ldbc_driver,
    last retrieved March 30th, 2016, version 0.2, commit 55f7ac0.

[4] Neo4j Data Importer & Workload Runner for the LDBC Social Network
    Benchmark.  https://github.com/hy/ldbc_socialnet_bm_neo4j, last
    retrieved March 18th, 2016, with, then, latest commit d75addf.

[5] Alain Kägi's fork of the Neo4j Data Importer & Workload Runner for
    the LDBC Social Network Benchmark.
    https://github.com/alainkaegi/ldbc_socialnet_bm_neo4j, last
    retrieved April 11th, 2016, with, then, latest commit 259be60 on
    branch fixes-and-additions.

[6] LDBC SNB Interactive Validation Project.
    https://github.com/ldbc/ldbc_snb_interactive_validation, last
    retrieved April 5th, 2016, with then, the latest commit 03c34c0.
