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


_LIES and MORE LIES - this needs to be rewritten_

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
