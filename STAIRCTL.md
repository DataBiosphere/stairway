# StairCtl - The Stairway Debugging and Recovery Tool
StairCtl is a command line interface intended for debugging and recovering Stairway flights.
It works by initializing, but not starting, a Stairway instance connected to a Stairway database.

Stairway provides a `Control` object that provides methods for the querying and state transitions.
At this point, all operations are done with database changes.

No flights can be run, because the StairCtl application does not have any of the implementation
classes for flights. For the same reason, no data can be deserialized.

## How to Install

TODO: The plan is to make a release action and have the distribution in github.
In the short-term, you can:
- clone Stairway repo: `git clone git@github.com:DataBiosphere/stairway.git`
- build StairCtl: `./gradlew :stairctl:build`
- run StairCtl: `java -jar stairctl/build/libs/stairctl-VERSION.jar`

## Command Summary
You can use the `help` command to get a list of the
commands and use `help "<command>"` to get more detailed help.

```
Built-In Commands
        clear: Clear the shell screen.
        exit, quit: Exit the shell.
        help: Display help about available commands.
        history: Display or save the history of previously run commands
        script: Read and execute commands from a file.
        stacktrace: Display the full stacktrace of the last error.

Connect Commands
        connect: Connect to a Stairway database
        disconnect: Disconnect from a Stairway database
        show connection: Show the current connection

Flight Commands
        count flights: Count flights
        count owned: Count owned flights
        force fatal: Force a flight to FATAL state (dismal failure)
        force ready: Force a flight to the READY state (disown it)
        get flight: Get one flight
        list dismal: List dismal failure flights
        list flights: List flights
        list owned: List owned flights

Stairway Commands
        list stairways: List stairway instances
```

### Using Spring Shell
StairCtl is built using [Spring Shell](https://docs.spring.io/spring-shell/reference/index.html), 
so it can be used as an interactive shell or to run a single command.
As an interactive shell, it supports history operations and tab completion.

## Connecting to a Stairway
The most complex part of using StairCtl is connecting to a Stairway; that is, providing the
information for connecting to a Stairway database.

### `connect` command
The full syntax or the `connect` command is:
```
connect [--username string]  [--password string]  [--dbname string]  [--host string]  [--port string]  
```

This table shows the precedence and defaults for each part of the input. Switches override
environment variables, which override the default. If you've run the stairway tests locally,
the default settings will connect to the local stairway database used by these tests.

| **Option**           | **Environment Variable** | **Default** |
|:---------------------|:---|:---|
| `-u, -U, --username` | STAIRCTL_USERNAME | stairwayuser |
| `-w, --password`     | STAIRCTL_PASSWORD | starwaypw |
| `-H, --host`         | STAIRCTL_HOST | 127.0.0.1 |
| `-p, --port`         | STAIRCTL_PORT | 5432 |
| `-d, --dbname`       | STAIRCTL_DBNAME | stairwaylib |

### `disconnect` command
You can disconnect from a Stairway with the `disconnect` command. It takes no options.
Connecting to a new Stairway database will automatically disconnect you from the current one.

### `show connection` command
Shows information for the current connection. It does not display the password.

## Querying Flights
One of the initial goals for StairCtl is to make it easier to look at flights, see what
their state is, and see how they were executed.

### `list` commands
There are three list commands. They each provide **paging** of the list. You can specify
`-o` or `--offset` for the start of the page and `-l` or `--limit` for the number of flights
to list.

The list is always presented in descending submit-time order, so the most recent flights
are shown first.

#### `list flights` command 
You can provide an optional `-s` or `--status` option to filter for flights in
specific states. For example, `list flights --status ERROR`.

Examples:
```text
stairctl> list flights --status ERROR

Offset FlightId                             Class                          Submitted                   Completed                   Status       StairwayId                    
------ ------------------------------------ ------------------------------ --------------------------- --------------------------- ------------ ------------------------------
     0 458e9d61-1bdd-46c6-bdb4-ac85fea7bebc ...ateControlledResourceFlight 2021-06-11T18:08:26.305134Z 2021-06-11T18:08:40.604532Z ERROR                                      
     1 99cd5909-dbd9-49bf-b857-5b9fcddced86 ...ateControlledResourceFlight 2021-06-11T18:08:24.582197Z 2021-06-11T18:08:24.595765Z ERROR                                      
     2 9caa152b-026c-453c-addf-fe9cb1bef54c ...ateControlledResourceFlight 2021-06-11T18:04:16.011679Z 2021-06-11T18:06:34.123187Z ERROR                                      
```
The `...` in the `Class` column indicates that the name has been truncated from the left. 

```text
stairctl> list flights -o 5 -l 5

Offset FlightId                             Class                          Submitted                   Completed                   Status       StairwayId                    
------ ------------------------------------ ------------------------------ --------------------------- --------------------------- ------------ ------------------------------
     5 750aec00-f414-4d2d-ab49-afb8a1207d42 ...eteControlledResourceFlight 2021-06-11T18:09:15.677952Z 2021-06-11T18:11:49.876130Z SUCCESS                                    
     6 b5c6203d-9a42-4b96-9f98-c0e737f7e0bf ...ateControlledResourceFlight 2021-06-11T18:08:42.396530Z 2021-06-11T18:09:14.433163Z SUCCESS                                    
     7 458e9d61-1bdd-46c6-bdb4-ac85fea7bebc ...ateControlledResourceFlight 2021-06-11T18:08:26.305134Z 2021-06-11T18:08:40.604532Z ERROR                                      
     8 99cd5909-dbd9-49bf-b857-5b9fcddced86 ...ateControlledResourceFlight 2021-06-11T18:08:24.582197Z 2021-06-11T18:08:24.595765Z ERROR                                      
     9 ada63c52-2ab3-403c-ab7a-d5d6cc1c02e0 ...ateControlledResourceFlight 2021-06-11T18:06:42.442001Z 2021-06-11T18:08:06.734071Z SUCCESS                                    
```

####`list dismal` command
This command is a shortcut for `list flights --status FATAL`; providing a list
of flights that require manual intervention for recovery.

#### `list owned` command
Shows flights that are currently owned by a Stairway instance. Those might
be actually running on that instance, or might be awaiting recovery.

### `count` commands
List commands do not provide the total number of flights in their category. Since that can be
an expensive query (although recently release grooming should help that), we do not compute the
count on every list command. Instead, we provide count commands.

#### `count flights` command
This command provides an optional `-s` or `--status` option corresponding to the
`list flights` command.

Examples:
```text
stairctl> count flights
Found 31 flights
```

```text
stairctl> count flights --status ERROR
Found 3 flights with status ERROR
```
#### `count owned` command
Counts currently owned flights that would be listed by the `list owned` command.

### `get flight` command
By default, the `get flight` command provides a summary of the flight. It takes a flight id.
You can get more data by setting switches:
* `--input` will display the input paramters
* `--log` will display a tabular summary of the log
* `--logmap` will display details of the log including the working map at each point
For example, `get flight --input --logmap --flight-id a63fc80f-f8a7-4cbc-be58-6cebc98ac21f`
will provide the maximum, noisiest retrieval.

Examples:

The summary form of `get flight`:
```text
stairctl> get flight 458e9d61-1bdd-46c6-bdb4-ac85fea7bebc 
Flight: 458e9d61-1bdd-46c6-bdb4-ac85fea7bebc
  class     : bio.terra.workspace.service.resource.controlled.flight.create.CreateControlledResourceFlight
  submitted : 2021-06-11T18:08:26.305134Z
  completed : 2021-06-11T18:08:40.604532Z
  status    : ERROR
  exception : 
  stairwayId: 
```

The form with input parameters:
```text
 get flight --input 458e9d61-1bdd-46c6-bdb4-ac85fea7bebc 

Flight: 458e9d61-1bdd-46c6-bdb4-ac85fea7bebc
  class     : bio.terra.workspace.service.resource.controlled.flight.create.CreateControlledResourceFlight
  submitted : 2021-06-11T18:08:26.305134Z
  completed : 2021-06-11T18:08:40.604532Z
  status    : ERROR
  exception : 
  stairwayId: 
  inputMap  :
    auth_user_info              : ["bio.terra.workspace.service.iam.AuthenticatedUserRequest",{"email":"elijah.thunderlord@test.firecloud.org","subjectId":null,"token":"<<TOKEN>>","reqId":"522a7e88-1444-4288-957c-5e8500f71299"}]
    creationParameters          : ["bio.terra.workspace.generated.model.ApiGcpBigQueryDatasetCreationParameters",{"datasetId":"my_test_dataset_8246","location":"us-central1"}]
    description                 : "Create controlled resource BIG_QUERY_DATASET; id e099d016-1562-4772-9391-04a490677146; name test_dataset_2d2bbc59_9343_4183_8567_651995b53527"
    iamRoles                    : ["java.util.Collections$EmptyList",[]]
    mdcKey                      : "null"
    opencensusTracingSpanContext: "AAAAAAAAAAAAAAAAAAAAAAAAAQAAAAAAAAAAAgA="
    request                     : ["bio.terra.workspace.service.resource.controlled.ControlledBigQueryDatasetResource",{"workspaceId":"372a0326-ca56-41ca-8beb-3a89a3385edf","resourceId":"e099d016-1562-4772-9391-04a490677146","name":"test_dataset_2d2bbc59_9343_4183_8567_651995b53527","description":"how much data could a dataset set if a dataset could set data?","cloningInstructions":"COPY_REFERENCE","assignedUser":null,"accessScope":"ACCESS_SCOPE_SHARED","managedBy":"MANAGED_BY_USER","datasetName":"my_test_dataset_8246","resourceType":"BIG_QUERY_DATASET","stewardshipType":"CONTROLLED","category":"USER_SHARED"}]
    resultPath                  : null
    subjectId                   : null
```

The form showing the log summary:
```text
stairctl> get flight --log 458e9d61-1bdd-46c6-bdb4-ac85fea7bebc 

Flight: 458e9d61-1bdd-46c6-bdb4-ac85fea7bebc
  class     : bio.terra.workspace.service.resource.controlled.flight.create.CreateControlledResourceFlight
  submitted : 2021-06-11T18:08:26.305134Z
  completed : 2021-06-11T18:08:40.604532Z
  status    : ERROR
  exception : 
  stairwayId: 
  flight log:
      Step Direction Log Time                         Duration Rerun Exception
    ------ --------- --------------------------- ------------- ----- --------------------------------------------------
         0 DO        2021-06-11T18:08:26.311883Z   0:00:00.487 false 
         1 DO        2021-06-11T18:08:26.799597Z   0:00:01.606 false 
         2 DO        2021-06-11T18:08:28.406211Z   0:00:00.963 false 
         3 DO        2021-06-11T18:08:29.369389Z   0:00:00.004 false 
         4 SWITCH    2021-06-11T18:08:29.373599Z   0:00:00.002 false 
         4 UNDO      2021-06-11T18:08:29.376491Z   0:00:10.904 false 
         3 UNDO      2021-06-11T18:08:40.281023Z   0:00:00.002 false 
         2 UNDO      2021-06-11T18:08:40.283257Z   0:00:00.316 false 
         1 UNDO      2021-06-11T18:08:40.599976Z   0:00:00.003 false 
```
Note how the step does not show the name of the step class. That information is not held in the
database. It only exists when the flight object is constructed; therefore, it cannot be reported
out here. In the future, it might be worth adding that as an informational field to the database
for debugging purposes.

A section of the full log map form:
```text
stairctl> get flight --logmap 458e9d61-1bdd-46c6-bdb4-ac85fea7bebc 

Flight: 458e9d61-1bdd-46c6-bdb4-ac85fea7bebc
  class     : bio.terra.workspace.service.resource.controlled.flight.create.CreateControlledResourceFlight
  submitted : 2021-06-11T18:08:26.305134Z
  completed : 2021-06-11T18:08:40.604532Z
  status    : ERROR
  exception : 
  stairwayId: 
  flight log:
    Log Entry:
      stepIndex: 0
      direction: DO
      logTime  : 2021-06-11T18:08:26.311883Z
      duration : 0:00:00.487
      rerun    : false
      exception: 
      workingMap:
    Log Entry:
      stepIndex: 1
      direction: DO
      logTime  : 2021-06-11T18:08:26.799597Z
      duration : 0:00:01.606
      rerun    : false
      exception: 
      workingMap:
    Log Entry:
      stepIndex: 2
      direction: DO
      logTime  : 2021-06-11T18:08:28.406211Z
      duration : 0:00:00.963
      rerun    : false
      exception: 
      workingMap:
        iamGroupEmailMap: ["java.util.HashMap",{"READER":"policy-ee04c63c-52ea-4c65-b2fa-081865456076@dev.test.firecloud.org","OWNER":"policy-ba716d3c-33a0-40c0-aa99-34f2a84e8b3c@dev.test.firecloud.org","WRITER":"policy-81cd0840-0173-426e-9d7e-2f5384cf4c3f@dev.test.firecloud.org","APPLICATION":"policy-5be527ac-68b5-4ff9-af52-336105d5e47c@dev.test.firecloud.org"}]
    Log Entry:
      stepIndex: 3
      direction: DO
      logTime  : 2021-06-11T18:08:29.369389Z
      duration : 0:00:00.004
      rerun    : false
      exception: 
      workingMap:
        iamGroupEmailMap: ["java.util.HashMap",{"READER":"policy-ee04c63c-52ea-4c65-b2fa-081865456076@dev.test.firecloud.org","OWNER":"policy-ba716d3c-33a0-40c0-aa99-34f2a84e8b3c@dev.test.firecloud.org","WRITER":"policy-81cd0840-0173-426e-9d7e-2f5384cf4c3f@dev.test.firecloud.org","APPLICATION":"policy-5be527ac-68b5-4ff9-af52-336105d5e47c@dev.test.firecloud.org"}]
```

## Flight Use Cases
This section provides a few capabilities of StairCtl.

### Unrecoverable Flight
We have had cases where a service is unable to start, because a Stairway
flight is not recoverable. The Stairway `recoverAndStart` call fails and
the service is unable to start.

This problem can be addressed by locating the stuck flight, perhaps using the `list flights` and
`get flight` commands. Then forcing the flight into the "dismal failure" state, so that it will
not be subject to recovery. That is done with the `force fatal` command.

### Recover Timed-Out Flight
We have seen cases where a flight fails because a dependent service is unavailable. The operation
exhausts all retries and fails. Usually, that is a normal error and is handled by performing an 
undo of the flight steps. However, if the timeout failure happens _during undo_, the flight will
be a "dismal failure" and set to FATAL state. You can see that in this case there is nothing
wrong with the flight itself.

Such flights can be recovered manually using StairCtl. When the dependent service is available,
you can force the flight into the READY state using the `force ready` command. The next time
a pod performs `recoverAndStart`, Stairway will recover the flight. Recovery in this case means
continuing to undo the flight steps.

## Future Ideas
There are many useful operations that could be done with StairCtl. Some current ideas are:
* Trigger a groom operation
* Delete a flight - remove it from the system (very carefully)
* Cancel a flight - exactly how to do this is not clear, because there is no simple way to poke a
specific Stairway instance to cancel.
* Edit input and working map of a flight (very carefully)
* Improve the display of flight maps
* List flights owned by a specific stairway
* List unowned flights
* Add step class name as informational data to the Stairway database log
* Provide shell-script-friendly mode that would execute one command and exit. And if we did that,
it would make sense to provide the option of JSON output to allow easy consumption of the command
result.

If you have suggestions, you can email `ddietterich@verily.com` or
add a ticket to the Jira epic [Stairway Debug Tooling]( https://broadworkbench.atlassian.net/browse/PF-678).
