# stairway
Stairway is a library that provides a framework for running "saga" transactions. 
The goal is to make a sequence of operations, often over disparate resources,
run as a transaction; either complete successfully or make no change. A typical
example in our environment would be an operation where we maintain metadata about
an object via a write to a SQL database and create several cloud-platform objects
to instantiate that object. If any of those operations fail, we want to clean up so
the state is as if the operation had never occurred.

Stairway provides a way for you to organize your code so that such operations are:
<ul>
<li>Atomic - they either happen or don't happen</li>
<li>Resiliant - a failure of the service or system does not lose state.
It allows the operation to proceed or rollback.</li>
</ul>

## Flights and Steps

## Retrying

## Exception Handling



## Testing

The Stairway project has unit tests. Running the unit tests requires some configuration:
1. Install and run a local version of Postgres; for example,  [Postgres App for Mac](https://postgresapp.com/)
2. Create a test database for Stairway testing. An example SQL script can be found in
`test/resources/create-stairwaylib-db.sql`
3. Set environment variables used by the tests to find the database. The default values
match the values in the sql file above:
    - STAIRWAY_USERNAME - default is `stairwayuser`
    - STAIRWAY_PASSWORD - default is `stairwaypw`
    - STAIRWAY_URI - default is `jdbc:postgresql://127.0.0.1:5432/stairwaylib`
4. Run the tests. For example, `./gradlew test`    
 