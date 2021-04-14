# Developing Stairway

## Branching and Versioning
The initial design of Stairway branches was that we would use two main branches in github.

The **master** branch ws intended to be used for distributing code
for other components to consume for alpha, staging, and production. It is published to the `libs-release-local`
repository inside of artifactory. It always has simple semantic version numbers: _major_._minor_._patch_

The **develop** branch is used for developing code. The results need to be published in order to properly test
Stairway, should not be used for any production purposes. Is published to the `libs-snapshot-local` repository 
inside of artifactory. It has a semantic version number and the word snapshot:  _major_._minor_._patch_-SNAPSHOT

The current practice is that we are releasing from the **develop** branch and that is
being consumed by other components. As things stabilize, we will have to decide if we want
to continue with direct release or move to indirect release.

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

## Deploying to Artifactory

For Broad-Verily development, you can publish the stairway library to Broad's Artifactory instance
using the artifactoryPublish task. For that to work, define the following environment variables:
- ARTIFACTORY_USER
- ARTIFACTORY_PASSWORD

## Future Enhancements

Right now, the collection of future work is held in a Jira instance run by the Broad and
is not publicly available.
