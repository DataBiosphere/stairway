[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=DataBiosphere_stairway&metric=alert_status)](https://sonarcloud.io/summary/new_code?id=DataBiosphere_stairway)
# Stairway
Stairway is a library that provides a framework for running _saga transactions_. Saga
transactions, introduced by Hector Garcia-Molina in 1987, use _compensating operations_ to
rollback the transaction rather than having a service that intermediates activity so is
able to perform the rollback. The goal is to make a sequence of operations, often over disparate resources,
run as a transaction; either complete successfully or make no change.

A typical example in our environment is an operation where we maintain metadata about
an object via a write to a SQL database and create several cloud-platform objects
to instantiate that object. If any of those operations fail, we want to clean up so
the state is as if the operation had never occurred.

Stairway provides a way for you to organize your code so that such operations are:
 * **Atomic** - they either happen or don't happen. If something goes wrong, the operation
   is undone.
 * **Recoverable** - a failure of the service or system does not lose state. When the system recovers, the operation can proceed or rollback.

Stairway does not provide as strong a transaction guarantee as a database system. Most database systems are able to
provide:
 * **Isolation** -  one transaction runs as if it is the only transaction running
 * **Consistenty** - a transaction sees a consistent state of the underlying data

Stairway cannot intermediate access to underlying data and objects, so it cannot control
the view of those objects in the course of running the saga transaction.

With Stairway, you still have to write code with some care to make sure that it results
in idempotent behavior. The benefit is that Stairway provides a common idiom, and handles
persistent bookkeeping, retrying, logging, and recovery after failure.

## References
For more information on developing flights in Stairway see: [Stairway Flight Developer Guide](FLIGHT_DEVELOPER_GUIDE.md)

For information on developing Stairway see [Developing Stairway](DEVELOPMENT.md)

For information on the StairCtl debug/recovery tool, see [StairCtl](STAIRCTL.md)

## Overview
This section provides an overview of Stairway concepts.

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
their work.

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

### Context Awareness and Logs
Stairway leverages the underlying logging system's mapped diagnostic context (MDC) if available.

MDC manages contextual information on a per-thread basis: as Stairway submits a Flight for processing,
it **passes along the MDC of the calling thread** so that context isn't lost.  It also further populates
the MDC with **flight-specific context** for the duration of the flight's execution on the thread,
and **step-specific context** for the duration of each step's execution.

For more information on MDC, please see [Logback's MDC manual](https://logback.qos.ch/manual/mdc.html).

# TODOs
* Add a section on clusters, queuing, failure, and recovery
* Add a section - perhaps in DEVELOPMENT.md describing the schema
* Add a link to a flight example. Maybe https://github.com/DataBiosphere/terra-workspace-manager/blob/dev/src/main/java/bio/terra/workspace/service/resource/controlled/flight/create/CreateControlledResourceFlight.java
* Best practices on serdes of FlightMap parameters
* Guildance on developer testing of Stairway internal code and flights
