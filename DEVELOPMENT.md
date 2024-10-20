# Developing Stairway

## Branching and Versioning
The initial design of Stairway branches was that we would use two main branches in github.

The **master** branch is intended to be used for distributing code
for other components to consume for alpha, staging, and production. It is published to the `libs-release-local`
repository inside of artifactory. It always has simple semantic version numbers: _major_._minor_._patch_

The **develop** branch is used for developing code. The results need to be published in order to properly test
Stairway, should not be used for any production purposes. Is published to the `libs-snapshot-local` repository 
inside of artifactory. It has a semantic version number and the word snapshot:  _major_._minor_._patch_-SNAPSHOT

The current practice is that we are releasing from the **develop** branch and that is
being consumed by other components. As things stabilize, we will have to decide if we want
to continue with direct release or move to indirect release.

## Requirements

Java 17

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

For folks working on Terra, the Stairway configuration is embedded within the component
configuration, so these steps are included in component developer setup.

## Local Publishing
When working on this library, it is often helpful to be able to quickly test out changes
in the context of a service repo (e.g. `terra-workspace-manager` or `terra-resource-buffer`)
running a local server.

Gradle makes this easy with a `mavenLocal` target for publishing and loading packages:

1. Publish from Stairway to your machine's local Maven cache.

   ```
   ./gradlew publishToMavenLocal
   ```

   Your package will be in `~/.m2/repository`.
2. From the service repo, add `mavenLocal()` to the _first_ repository location
   build.gradle file (e.g. before `mavenCentral()`).

   ```
   # terra-workspace-manager/build.gradle

   // If true, search local repository (~/.m2/repository/) first for dependencies.
   def useMavenLocal = true
   repositories {
      if (useMavenLocal) {
          mavenLocal() // must be listed first to take effect
      }
      mavenCentral()
      ...
   ```

That's it! Your service should pick up locally-published changes. If your changes involved bumping
this library's version, be careful to update version numbers accordingly.

## SourceClear

[SourceClear](https://srcclr.github.io) is a static analysis tool that scans a project's Java
dependencies for known vulnerabilities. If you are working on addressing dependency vulnerabilities
in response to a SourceClear finding, you may want to run a scan off of a feature branch and/or local code.

### Github Action

You can trigger Stairway's SCA scan on demand via its
[Github Action](https://github.com/broadinstitute/dsp-appsec-sourceclear-github-actions/actions/workflows/z-manual-stairway.yml),
and optionally specify a Github ref (branch, tag, or SHA) to check out from the repo to scan.  By default,
the scan is run off of Stairway's `develop` branch.

High-level results are outputted in the Github Actions run.

### Running Locally

You will need to get the API token from Vault before running the Gradle `srcclr` task.

```sh
export SRCCLR_API_TOKEN=$(vault read -field=api_token secret/secops/ci/srcclr/gradle-agent)
./gradlew srcclr
```

High-level results are outputted to the terminal.

### Veracode

Full results including dependency graphs are uploaded to
[Veracode](https://sca.analysiscenter.veracode.com/workspaces/jppForw/projects/904886/issues)
(if running off of a feature branch, navigate to Project Details > Selected Branch > Change to select your feature branch).
You can request a Veracode account to view full results from #dsp-infosec-champions.

## Deploying to Artifactory

For Broad-Verily development, you can publish the stairway library to Broad's Artifactory instance
using the artifactoryPublish task. For that to work, define the following environment variables:
- ARTIFACTORY_USER
- ARTIFACTORY_PASSWORD

## Future Enhancements

Right now, the collection of future work is held in a Jira instance run by the Broad and
is not publicly available.
