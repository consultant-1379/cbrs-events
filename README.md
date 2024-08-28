# README

This README covers three main topics:

1. **Version Control**
   How to manually control versions for this repository.

2. **Unit Tests**
   Running and adding unit tests when introducing a new event. Ensure that every event has a corresponding unit test.

3. **Creating a Tar File**
   How to create a tar archive for the `cbrs_events` script and schemas, enabling its use on a server. (This is a temporary measure until automation is implemented as part of the build process.)

# Version control

There is no version control on this project yet.
Please update the version of the pom.xml in this script and in all the pom.xmls via this simple command from the cbrs-events folder
find .  -name pom.xml | xargs sed -i 's/1.0.1-SNAPSHOT/1.0.2-SNAPSHOT/g'

# Running Unit Tests
 - **Note: Prerequisites of having python in your build environment**

1. Navigate to the Python test folder by using the command: `cd ERICDomainProxyEvents_CXP111111/src/main/test/`.

2. Execute the tests using the command: `python3 -m unittest`.

# Adding a new event to the schema

1. **Add the new event to the corresponding schema file**
    - Locate the schema file with the corresponding version in the folder /resourse/schemas
    - Create unit test for the new Event, similar to `test_that_can_parse_schema_2023_10_for_EV_SC_RECONFIG`
   
2. **Check if you are introducing new headers**:
   - Execute the test `TestEventSchemaParser.test_that_can_load_schemas_csv`.
     Ex:`cd ERICDomainProxyEvents_CXP111111/src/main/test/`
         `python3 -m unittest test_event_schema_parser.TestEventSchemaParser.test_that_can_load_schemas_csv`
   - If the test fails, it indicates the introduction of new headers.

3. **Identify New Headers**:
   - Identify the new headers that caused the test to fail (e.g., `CBSD_ID`).

4. **Update the CSV Schema File**:
   - Locate the appropriate CSV schema file, such as `cbrs_event_csv-schema_XXXX_XX.csv`.
   - Update this file with the new headers identified in step 2. Ensure that the schema file matches the schema of the data being tested.

5. **Update the Test**:
   - Update the test `TestEventSchemaParser.test_that_can_load_schemas_csv` with the new headers from the schema file you modified in step 3.
   - This update should align the test with the current schema.

6. **Update Total Events for the Schema Version**:
   - Update the test `TestEventSchemaParser.test_that_can_load_schemas_csv` with the new number of events for the schema version (e.g., `TOTAL_EVENTS_XXXX_XX`).

7. **Update Total Headers for the Schema Version**:
   - Update the test `TestEventSchemaParser.test_that_can_parse_csv_schema_2023_10` with the new total headers for the schema version (e.g., `TOTAL_HEADERS_XXXX_XX`).

8. **Verify Unit Tests**:
   - Run all unit tests to ensure that the changes you've made do not break existing tests.
   - Confirm that all unit tests pass.

# Creating a Tar Archive (Temporary solution)

1. From the project's root folder, grant execute permission to the build script using `chmod 777 build.sh`, and then run it with `./build.sh`

2. If all tests pass successfully, you will find the `cbrs_event.tar` archive in the `target` folder.
