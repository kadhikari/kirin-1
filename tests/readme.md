# Run
to run the tests, just run py.test in the kirin dir
 
# Test with a database
To be able to correctly test kirin, a database is needed

To have a brand new database, a docker with a db is set up once for each test session

The db scheme is reseted once per module in tests/integration. 
The scheme is upgraded/downgraded for each module to test the migration scripts.

The db is cleaned up before each tests in tests/integration, so each tests are completly independant
