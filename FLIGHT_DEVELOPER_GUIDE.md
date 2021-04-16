# Stairway Flight Developer Guide

Date | Status | Notes
-----|--------|------
2021-04-14 | Created | First draft

This document provides guidance on how to write Stairway flights. It assumes you are
familiar with the concepts of Stairway from the README.md

The Stairway implementation is based on the concept of _saga transactions_. It uses
principles familiar in the world of database systems, but perhaps unfamiliar to
programmers with different backgrounds. The transactional approach - that the work of a
flight is either all successfully completed or is all cleaned up - is what makes Stairway
useful. It provides a structure for building more reliable software. It also requires a
particular way of programming to make it work.


## Table of Contents

 1. [Logging](#Logging)
 1. [Using the Working Map](#workingmap)
    1. [Persisting at Step Boundaries](#persistatstep)
    1. [Persisting on Transition from Doing to Undoing](#persistatundo)
    1. [Working Map Best Practices](#workingmapbest)
 1. [Idempotent Steps](#idempotentsteps)
    1. [Database Transactions](#databasetransactions)
    1. [Existence Checking](#existencechecking)
 1. [Retrying Steps in a Flight](#retrying)
    1. [Retry Object State and Reuse](#retryreuse)
    1. [How to Retry](#retryhow)
    1. [Retry After Restart](#retryafterrestart)
 1. [Flight Construction](#flightconstruction)
    1. [Constructors Should...](#constructorsshould)
    1. [Constructors Should NOT...](#constructorsshouldnot)
    1. [Spring Patterns](#springpatterns)
 1. [Dismal Failures](#dismalfailures)
 1. [Using FlightDebugInfo](#flightdebuginfo)
    1. [Restart Every Step](#restarteverystep)
    1. [Fail at Specific Steps](#failsteps)

The ToC is manually maintained. Apologies in advance...

## Logging <a name="logging"></a>

Stairway provides failure recovery by writing state into a SQL database. The database,
possibly shared across a cluster of cooperating Stairway instances, provides the
persistent record of a flight.

The Stairway database is written to:
 * When the flight is initially submitted
 * At the end of each step of the flight
 * During flight state changes (more on that later)

When a step is restarted after a failure, Stairway **guarantees** that the flight
state is exactly what it was when the step originally started. Flight state refers only
to the state Stairway controls; not the state of external resources.

## Using the Working Map <a name="workingmap"></a>

Every executing flight has a working map. It is a `Map<String, Object>` that allows steps
to save state and communicate that state to later steps in the flight. It is important to
understand when the working map is written back to the database. Simply putting something
into the map does not put it in the database.

### Persisting at Step Boudaries <a name="persistatstep"></a>

The first **key principle** of the working map is that it is persisted at step
boundaries. To do otherwise would violate the guarantee that a step starts with a
consistent initial state across failure/recovery.

The table below shows the state of the key "ABC" in the 

Time | Step | Operation | In memory "ABC" | Database "ABC" | Notes
-----|------|-----------|-----------------|----------------|-------
 t0 | 0 | start step | null | null | nothing stored yet
 t1 | 0 | `put("ABC", "foo")` | "foo" | null | in-memory working map set
 t2 |   |          |       |       | failure point
 t3 | 0 | end step | "foo" | "foo" | step state logged to database
 t4 |   |          |       |       | failure point
 t5 | 1 | start step | "foo" | "foo" |
 t6 | 1 | `put("ABC", "bar")` | "bar" | "foo" | in-memory state updated
 t7 | 1 | end step | "bar" | "bar" | step state logged to database

Suppose there is a server failure at time t2; after the working map is updated,
but before the step state is logged. When the flight is recovered and the step is
restarted, the state will be exactly what it was at t0; at the start of step 0.

If there is a server failure at time 4, step 0 will be considered complete. When
the flight is recovered, work would continue at step 1.

### Persisting on Transition From Doing to Undoing <a name="persistatuno"></a>

The second **key principle** of the working map is that it is persisted when a step
transitions from going forward (doing) to going backwards (undoing) as the result of an error.

Time | Step | Operation | In memory "key" | Database "key" | Notes
-----|------|-----------|-----------------|----------------|-------
 t0 | 0 | start step | null | null | nothing stored yet
 t1 | 0 | `put("key", "foo")` | "foo" | null | in-memory working map set
 t2 | 0 | error happens | "foo" | null |
 t3 | 0 | switch to undo | "foo" | "foo" | step state logged to database
 t4 | 0 | start undo  | "foo" | "foo" | initial state of undo
 t5 | 0 | `put("key", "xyz")` | "xyz" | "foo" | in-memory working map set
 t6 | 0 | end step | "xyz" | "xyz" | step state logged to database

As you can see from the table, the switch from do to undo records state in the Stairway
database. Clearly it must; on server failure/recovery Stairway has to know that it is
performing an undo. 

### Working Map Best Practices <a name="workingmapbest"></a>

A useful rule of thumb for the working map is: only use the working map to communicate
data to later steps.

Do not try to use the working map to communicate between the `do()`
and the `undo()` of a step. While technically possible, it is difficult to make it
reliable across server failure/recover.


## Idempotent Steps <a name="idempotentsteps"></a>
Stairway can resume flights after server failure. A failure might be the server process
crashing or Kubernetes shutting down the containing pod. On recovery, the step that was
running at the point of failure is restarted. 

As a result, the `do()` and `undo()` methods of a step must be written to be
idempotent; that is, they may be run multiple times and must have the same effect each
time. Meeting that requirement often uses a different approach than writing a sequential program.

### Database Transactions <a name="databasetransactions"></a>
A step that consists of a database transaction is simple to write. If there is a failure,
the underlying database system provides the rollback of the transaction. However, it is
still possible for a database transaction step to fail in the time between when the
database transaction commits and when Stairway logs the completion of the step.

You still have to make the step idempotent, so you have to handle that condition. Let's
say you are inserting a new object into your database. This is best done by giving your
target table a primary key column that is a UUID. Then you can run a sequence of steps
something like this:
 1. Step that generates a random UUID and stores it in the working map; _OR_ hand a random
 UUID into the flight as an input parameter.
 1. Step that runs a database transaction. The database transaction does:
    1. Read the table to see if a row with the UUID already exists. If so, return
    success. A previous run of the step inserted that row.
    1. Insert the row into the table.
    If there is a failure and this step is re-run, it will either find that the database
    transaction rolled back and re-insert the row; or it will find that the database
    transaction committed and the insert already happened successfully.

A variation on this approach is for the step to trap the duplicate key exception from the
database system and consider that a success.

### Existence Checking <a name="existencechecking"></a>
The database transaction case brings up the general problem of coordinating existence
checking and idempotent steps. There are a few common pitfalls.

#### Pitfall 1: deleting someone else's object
Imagine that you want to create a thing named "foo". The thing might be a file, a cloud
object, a database row, whatever. You write a step like this:

```
Step: Create a thing
 do():
   create "foo"
 undo():
   delete "foo"
```
But what if "foo" already exists when this step is run? The execution will go:
 1. Stairway runs `do()` and call create "foo". That results in a "foo already exists"
 exception.
 1. Stairway catches the exception and decides the flight is failed. It
 transitions to undoing.
 1. Stairway runs `undo()` and calls delete "foo". It deletes the pre-existing object.

#### Pitfall 2: failing when you already succeeded

```
Step: Create a thing - take 2
 do():
   if "foo" exists {
     write "foo already existed" to the working map
     throw "foo exists"
   }
   create "foo"
 undo():
   if the working map says "foo already existed" {
     then return success
   }
   delete "foo"
```
That is better. When Stairway catches the exception and transitions to undoing, it writes
the flight state, so the working map gets written. The undo can skip the delete. Yay!

The problem is that the `do()` is no longer idempotent. Suppose there is a server failure right
after the "foo" thing is created, but before Stairway records the step result. On
recovery, Stairway will rerun the `do()` step. It will say, "foo already existed", and the
flight will fail.

In general, best practice is _NOT_ to use the working map to communicate state between
your `do()` and `undo()` methods. The `undo()` method gets called regardless of
where in the `do()` step the working map is set. It makes the `undo()` more complex 
if it has to handle all of the possible states of the working map.

#### Pitfall 3: concurrent operation

A more reliable way to accomplish the existence check is to do it in a separate step. So
now you have this pseudo-code:
```
Step 1: Check thing existence
 do():
   if "foo" exists {
     error out with "foo already exists"
   }
 undo():
   no action

Step 2: Create thing - we only get to this step if "foo" does not exist
 do():
   create "foo" - ignore "foo" already exists error
 undo():
   delete "foo"
```
This works pretty well, but there is still a problem. Suppose there are two flights
running in parallel that are both trying to create "foo". They will both make it through
Step 1 and decide "foo" doesn't exist. Then they will both try to create "foo". One will
successfully create _their version_ of "foo". The other will get a "foo already
exists" error, ignore it, and think they have succeeded.

#### A Working Approach

The typical case in our system is that we are creating a cloud resource and recording that
resource in a database. We can use the database state to address the pitfalls. We setup
the database so that the UUID and the name columns both have unique constraints. The general
approach is:
 1. Create or provide as flight input a random UUID
 1. Write a row in the database with the UUID and the name. If there is a name
    collision, we fail in this step and do not try to create anything. If there is a UUID
    collision, we know that it was this flight that previously wrote the row and we can proceed to the next step.
    If all concurrent creators check the database first, they will conflict on the name
    before doing any creating.
 1. Create the target object
 1. If needed, update the database row to record details of the created object.

These pitfalls are presented in the context of object creation, but they appear in related
forms in other kinds of operations.

## Retrying Steps in a Flight <a name="retrying"></a>

### Retry Object State and Reuse <a name="retryreuse"></a>
It is important to understand the scope and state of a RetryRule object and only share
RetryRules objects where they will work properly. It is safe to re-use the same RetryRule
object *within* a flight instance. It is not safe to share the same RetryRule object *between*
flight instances.

The reason that you cannot share a RetryRule object between flights is that a RetryRule
object is stateful: it holds the state of retrying. For example, a rule that retries 3
times needs to hold a counter for the number of retries that have been done. If that
object is used across flights, each flight would be updating the number of retries.

The reason that you can share a RetryRule object within a flight is that RetryRules have
an `initialize()` method that resets the state. The Flight object always initializes the
RetryRule object at the start of executing the `do()` and `undo()` methods of a Step.

Every step has a RetryRule object associated with it. If the rule is not specified, the
`RetryRuleNone` is used. RetryRuleNone has no state and does no retries. It always
returns: "do not retry".

### How to Retry <a name="retryhow"></a>
There are two ways to request that the step be retried from within the step.
 1. Throw an exception that derives from `RetryException`. When flight execution catches
 that exception, it will attempt a retry.
 1. Return a StepResult of `STEP_RESULT_FAILURE_RETRY`.

It is our experience that having the step perform its own exception handling and return
the explicit `STEP_RESULT_FAILURE_RETRY` is more readable. It also keeps service
exceptions separate from Stairway exceptions.

Remember that simply requesting a retry and actually retrying are two separate
things. Requesting retry asks Stairway to check the RetryRule to see if there are any more
retries left. If the rule says, "no more retries", then the exception returned in the
StepResult is used as the failure result exception of the flight.

### Retry After Restart <a name="retryafterrestart"></a>
Retrying is only in-memory. RetryRule state is not retained across a failure/recovery.

Imagine that Stairway is running a step of a flight that uses a retry rule. The rule will
try 10 times and then give up. Stairway has retried 9 times and then the server fails.
When the flight is recovered, the step will be restarted and the retry rule counter will
start at 0 once again.

There is no limit to the number of times a flight can be recovered.

## Flight Construction <a name="flightconstruction"></a>

The Stairway caller never constructs a Flight object. When you call Stairway to run a flight,
you provide the input parameters and the class name.
```java
  String jobId = UUID.randomUUID().toString();
  FlightMap inputs = new FlightMap();
  inputs.put("ABC","foo);
  stairway.submit(jobid, MyFlight.class, inputs);
```

Stairway does the actual construction. Stairway must be able to re-construct the flight
after a failure/recovery from the data stored in the Stairway database. Constructing it in
all cases helps ensure that the class has the right constructor.

### Constructors Should... <a name="constructorsshould"></a>

Your flight constructor code should construct and configure the steps and retry rules for
the flight. The constructor might include:
 - adding different steps based on input parameters
 - extracting input parameters and providing them as constructor parameters to the steps

### Constructors Should NOT... <a name="constructorsshouldnot"></a>

The constructor **should not**:
 - do database lookups
 - access external services

There are two reasons for those constraints. First, flight construction is done in
Stairway on the recovery path. If a flight cannot be reconstructed, Stairway will
fail to initialize. That can leave your service in a failed state.

Second, errors in flight construction can cause the flight to be marked as a dismal
failure. It is better to keep the construction simpler and do the work within steps where
you have control over the error handling, retrying and recovery.

### Spring Patterns <a name="springpatterns"></a>

Stairway does not use Spring, but it can be used by services that use Spring.

When you construct a Stairway object, you can provide it with an _application
context_. It is an `Object` that Stairway passes into flight constructors so they can fill in
information about their service. It is easily confused with the Spring
`ApplicationContext` object, but they are not the same thing.

In our experience using Stairway and Spring, the typical use of the Stairway _application
context_ has been to give flights access to injected (`@Autowired`) singleton
objects. There are two styles in use described in the next two sections.

#### Stairway Application Context set to Spring Application Context

Suppose you have an autowired class called` FooService`. How can you resolve FooService in
your flight?

You can set the Stairway application context to be the Spring Application Context like this:
```java
@Autowired
ApplicationContext applicationContext; // Spring application context

Stairway.newBuilder(
  .applicationContext(applicationContext)
  ...
  ).build();
```

Then your flight constructors need code like this:
```java
public YourFlight(FlightMap inputParameters, Object applicationContext) {
  super(inputParameters, applicationContext);

  ApplicationContext springAppContext = (ApplicationContext)applicationContext;
  FooService fooService = springAppContext.getBean(FooService.class);
}
```

#### Resolve Beans in a "Bean Bag"

An alternative to pulling beans from the Spring application context is to make a so-called
"Bean Bag" class that resolves the beans and provides getters for use in flight
constructors. The bean bag is used as the application context. It goes like this:

Set up the bean bag class:

```java
@Component
public class BeanBag {
  private final FooService fooService;

  @Autowired
  public BeanBag(FooService fooService) {
    this.fooService = fooService;
  }

  public FooService getFooService() {
    return fooService;
  }
}
```

Use the bean bag class as the application context:

```java
@Autowired
BeanBag beanBag;

Stairway.newBuilder(
  .applicationContext(beanBag)
  ...
  ).build();
```

Then your flight constructors look like this:

```java
  public YourFlight(FlightMap inputParameters, Object applicationContext) {
    super(inputParameters, applicationContext);

  BeanBag beanBag = (BeanBag)applicationContext;
  FooService fooService = beanBag.getFooService();
```

## Dismal Failures <a name="dismalfailures"></a>

Stairway uses the term _dismal failure_ to refer to a flight that has an error and then
fails to successfully undo. It is the case where the saga transaction is not
all-or-nothing. This is a serious issue, because the system is left in an unintended,
potentially corrupt state. It probably requires human intervention to repair the problem.

When this happens, Stairway writes a log message including the text
`DISMAL FAILURE` that can be used for alerting. The flight is finished with a `FATAL`
status. (A flight with a normal failure that gets undon fails with an `ERROR` status.

In our experience, there are two main causes of dismal failures:
 1. Expired retries
 1. Untested undo paths

## Using FlightDebugInfo <a name="flightdebuginfo"></a>

Stairway provides some help test flights. In a test environment, you can pass a
`FlightDebugInfo` object in when you submit your flight. There are two types of behavior
you can request.

### Restart Every Step  <a name="restarteverystep"></a>

You can request that Stairway restart your flight at each step boundary. That tests
several things:
 * Your flight is not depending on any in-memory object state
 * Your flight is not holding transient external state
 * Your flight object can be reconstituted from the Stairway database

Restarting each step does not change the flow of your flight. It will make it a bit slower
as the flight object is reconstructed and the flight is queued on to the thread pool.

### Fail at Specific Steps <a name="failsteps"></a>

You can request that Stairway force a failure at any step boundary. This is a useful way
to exercise the undo path. You specify the failure`StepStatus` you want to return, so you
can test retry as well as failures that cause undo.

Note that this testing **only** happens at step boundaries, it does not exercise some of
the more complex cases where the partial execution of a step is either not idempotent or
causes a failure during undo. Step-specific fault insertion is one way to exercise
those cases, but it it not built into Stairway.

