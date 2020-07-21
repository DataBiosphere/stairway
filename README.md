# Stairway
Stairway is a library that provides a framework for running "saga" transactions. 
The goal is to make a sequence of operations, often over disparate resources,
run as a transaction; either complete successfully or make no change. A typical
example in our environment would be an operation where we maintain metadata about
an object via a write to a SQL database and create several cloud-platform objects
to instantiate that object. If any of those operations fail, we want to clean up so
the state is as if the operation had never occurred.

Stairway provides a way for you to organize your code so that such operations are:
<ul>
<li>Atomic - they either happen or don't happen. If something goes wrong, the operation
undone.</li>
<li>Recoverable - a failure of the service or system does not lose state. When the
system recovers, the operation can proceed or rollback.</li>
</ul>

With Stairway, you still have to write code with some care to make sure that it results
in idempotent behavior. The benefit is that Stairway provides a common idiom, and handles
persistent bookkeeping, retrying, logging, and recovery after failure.

## Overview
### Flights and Steps
A `Step` contains a single operation. It provides two methods: `do` and `undo`. The `do` method performs the
operation. The `undo` method removes the effects of the operation. Your step classes are derived from the `Step`
base class.

A `Flight` a set of Steps (of course). The `Flight` provides overall atomicity by running the `do` and `undo`
methods, moving from step to step. Your flight classes are derived from the `Flight` base class.

A `do` method can return three result states:
- Success: all is well and we can continue forward.
- Fatal: all is broken. We need to rollback.
- Retry: a retryable error happened. Invoke the retry logic associated with the step.  

Each Flight has two key-value maps that are accessible to each Step:
- input parameters - an immutable `Map<String, Object>` containing input parameters specified when the flight
was launched
- working parameters - a mutable `Map<String, Object>` containing data developed and used during execution of
the flight.

Working parameters are the way that steps communicate with each other or remember information needed to undo
their work. For example, a step allocating a resource might store the resource id in a parameter. The undo
code would lookup that id in the working parameters and use it to delete the resource.

When the flight completes, the working parameters can be retrieved. The result of the flight can be gathered
from the working parameters. For example, TDR uses the convention that Flights store their results in an object
with the key "result". That object is serialized out as the response to the REST API endpoint.

### Recovery
The Stairway system maintains persistent state about where it is in the Flight, so that a failure of
the system can be recovered. We use a SQL database for the persistent store.

The Flight stores its input parameters and working map at each step boundary. Stairway also persistently stores
errors, result states, and whether the flight is going forward or rolling back.

On recovery, Stairway can re-create the Flight object using its stored input parameters, re-create the execution
context using the working map and other information, and re-launch the Flight. It starts running the flight at
the step it was on when the system failed.

That has a ramification for writing Step methods: they need to be written so that on a re-run,
they do the right thing. That is usually trickier for `do` methods.

### Retrying
Each step can be annotated with a RetryRule. A retryable error will cause an undo of the step.
It may sleep for a bit. Then the Flight will retry the `do` method.
Stairway comes with four built-in retry rules:
- None - retry is not done. Retryable errors are treated as Fatal.
- FixedInterval - retry is done K times with a fixed time interval between attempts
- RandomBackoff - retry is done K times with a random time interval between attempts
- ExponentialBackoff - retry is done K times with exponentially growing time interval between attempts

You can create your own derivation of the `RetryRule` class and implement your own retry algorithm.

### Concurrency
Stairway is designed to provide atomic operations for one instance of one service. It does not coordinate any
global or cross-service state. Therefore, it is up to the application or service to implement concurrency control on 
its objects.

For example, in TDR data ingest jobs have a `load-tag`. The tag can be reused serially to rerun failed ingests.
Only one ingest job is allowed to use a `load-tag` at a time. Tag concurrency is controlled by "locking" the
tag at the start of an ingest job and "unlocking" it at the end. The lock is locked and unlocked by
updating a row in a database. The row contains two columns:
- boolean - locked or unlocked
- flightId - UUID of the flight that has the lock
The flightId is needed to make sure that a second call by the same flight to lock the tag (as would happen
during recovery) will notice that this flight already holds the lock and will not generate a CONFLICT error.

The `do` method in first step of a data ingest flight locks the tag. The `undo`
method unlocks the tag. The `do` method of the last step of the ingest flight unlocks the tag.
Because Stairway guarantees recovery, the Flight author knows that the resource will be locked during the
flight and unlocked regardless of whether the flight succeeds or fails.

### Examples
Terra Data Repository (Jade) uses Stairway and has many examples of its use.
See [jade-data-repo](https://github.com/DataBiosphere/jade-data-repo)

## Details
### Stairway Startup
There are three steps to starting up a Stairway instance in your application, described in the next
sections.

#### Stairway Object Construction
Use the Stairway.Builder to create a Stairway object. There are a number of parameters you can set to control
how Stairway functions. You can check the javadoc for most. The ones that affect Stairway startup are:
- stairwayClusterName - the name of a group of Stairway instances running in a Kubernetes cluster. This name
is used to find or create a shared work queue for the instances. In a standalone environment, you can allow the
name to default to a unique random name. 
- stairwayName - the name of this particular Stairway among the instance running. In Kubernetes, assuming a pod
contains exactly one Stairway instance, it is handy to use the pod name as the stairwayName. The important property
is that it be unique among instances. If you don't supply a name, a random one is provided. It may be 
- projectId - if you supply a projectId, then Stairway will find or create a work 
queue in that GCP project. (Other clouds to be named later.) Otherwise, there will be no work queue.

The object construction itself just records the inputs. It does not take action on them until the next step.
That behavior works better in frameworks like Spring where all of the components get created and wired before
the application starts running.

#### Stairway Initialization
Stairway initialization sets up the connection to the database. If run in a cluster, all Stairway instances
must share one database in order to share the state of flights.

The Stairway initialization method provides two booleans that are useful in controlling the database:
- migrateUpgrade - if true, Stairway performs a liquibase migration on its database.
- forceCleanStart - if true, Stairway empties the database and the work queue. That is not useful in a
production system, but handy for test environments.

Stairway initialization allocates internal resources, like thread pools, and the shared work queue.

The last step of initialization is to collect a list of the names of Stairway instances currently recorded in the
database. Those are returned as the result of the call. Note that this newly created instance is *not* added
to the list. If the same name is on the list, it should be recovered as described next.

#### Stairway Recover and Start
Recover-and-start accepts a list of obsolete Stairway instances that should be recovered. It is up to the application
to determine which instances are obsolete. In the standalone case, all instances found should be recovered.

In the Kubernetes cluster case, the application needs to check if any of the currently recorded instances are running
in other pods or if they are no longer active. By using pod name for Stairway name, it is easy to match the
result of a pod list with the recorded instance list returned from initialization.  

Stairway recover-and-start performs recovery on the obsolete instances. It then records this new instance
and starts accepting flights from the Stairway API and from the Work Queue, if enabled.

### Execution Model
Stairway provides a set of library calls for flight submission, status check, and waiting for completion.
These calls all run on the calling thread.


_LIES, LIES and MORE LIES - this needs to be rewritten_

The application provides a thread pool to Stairway on construction. The application to decide the thread
allocation model. When a flight is submitted, it is run using a thread from the provided pool.
That may mean that the flight will queue until a thread is available from the pool. The flight runs
each step on its thread. Stairway does not provide infrastructure for parallelism within a flight. 
Of course, a Step can do what it pleases.

### Database 
Each Stairway instance needs a separate database for managing its flights. If the application instance fails,
If the application instance fails, we expect that a replacement instance will be launched with the same
Stairway parameters and will perform recovery of its flights. By partitioning the work this way, we
minimize contention in the database and have an easy rule for who is supposed to recover a given flight.
It does not require any global state maintenance within Stairway.

## Submission
The code for a flight is a class that is an extension to the Flight base class. That Flight subclass
must supply a constructor that accepts the input parameters as a `FlightMap` map of key-value pairs. The
constructor creates the list of steps. Each step is an implementation of the Step interface.
The input parameters can guide the construction of that list.

The Stairway library performs the actual construction of the `Flight` subclass. That is necessary,
because the Stairway library needs to be able to reconstruct the flight during recovery as well.
A flight constructor must build exactly the same set of steps when given the same input parameters.

The Stairway library is called to start a flight. It is called with the Class object of flight and
the input parameters. The submit code locates the constructor and calls it with the input parameters.
Stairway code writes the information about the flight to the database and schedules the flight to run
on the thread pool.

The client provides the flight id for a submission. Stairway requires that the flight ids be unique
within the scope of one Stairway object. Stairway provides a default id generator that generates
UUID-based ids.

The caller uses the flight id to poll for status or wait on completion of the flight. The flight id is
also used to collect the resulting working map.

### Exception Handling
There are two styles of exception handling you can use.
- handle your own: use try-catch blocks in your code and explicitly return a `StepResult` object from your step.
The `StepResult` is one of success, failure, or retry.
- rely on Stairway to handle exceptions.

Stairway maintains a try-catch block around the execution of a Step method. If the exception is derived from
Stairway's `RetryException`, then we will treat it as if you had returned a `STEP_RESULT_FAILURE_RETRY`. Otherwise,
it is treated as if you had returned a `STEP_RESULT_FAILURE_FATAL`. 

# Developing Stairway

## Branching and Versioning
There are two branches of interest in the stairway github. The **master** branch is used for distributing code
for other components to consume for alpha, staging, and production. It is published to the `libs-release-local`
repository inside of artifactory. It always has simple semantic version numbers: _major_._minor_._patch_

The **develop** branch is used for developing code. The results need to be published in order to properly test
Stairway, should not be used for any production purposes. Is published to the `libs-snapshot-local` repository 
inside of artifactory. It has a semantic version number and the word snapshot:  _major_._minor_._patch_-SNAPSHOT

The versions are related by convention. The develop version is typically a minor or major increment ahead
of the master version.

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

Here is a list of planned enhancements for Stairway:
- scale out and scale back - it is simple to scale out, adding new Stairway instances. There is no convenient
support in Stairway for scaling back; that is taking an instance out of service. We need to add a way to
quiesce the service so all current operations complete and no new ones are started. That would allow an
application instance to come to a quiet point where it could be taken out of service.
- read-only flights - enable the (re-)use of steps, the thread pool and retries, but does no logging.
The motivation is to use Stairway as a consistent way of running async REST API requests, without the
logging overhead.
- fault insertion - it is difficult to test the atomicity, idempotency and recoverability of steps. The idea is
to provide hooks to allow a failure at any step boundary, and targeted recovery, to ensure that those properties
are achieved.
- multiple thread pools - Stairway runs with on thread pool with one set of properties. It _might_ be useful to
have different flavors of pools and submit flights to specific pools.

These enhancements are discussed in
[Stairway Enhancements](https://docs.google.com/document/d/1BMo6e8uJb1fLpQdbeAujenV2YurNGajBzDwey-bVa_4/edit?pli=1#heading=h.mwr27m9insmm)
  
