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
StairCtl will be getting new capabilities frequently, so this document may not be an up-to-date
reference of the available commands. You can use the `help` command to get a list of the
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

## Connecting to a Stairway
The most complex part of using StairCtl is connecting to a Stairway; that is, providing the
information for connecting to a Stairway database.

### `connect` command
The full syntax or the `connect` command is:
```
connect [--username string]  [--password string]  [--dbname string]  [--host string]  [--port string]  
```

This table shows the precedence and defaults for each part of the input. Switches override
environment variables, which override the default. 

| **Option** | **Environment Variable** | **Default** |
|:---|:---|:---|
| `-u, -U, --username`| STAIRCTL_USERNAME | stairwayuser |
| `-w, --password` | STAIRCTL_PASSWORD | starwaypw |
| `-h, --host` | STAIRCTL_HOST | 127.0.0.1 |
| `-p, --port` | STAIRCTL_PORT | 5432 |
| `-d, --dbname` | STAIRCTL_DBNAME | stairwaylib |

### `disconnect` command
You can disconnect from a Stairway with the `disconnect` command. It takes no options.
Connecting to a new Stairway database will automatically disconnect you from the current one.

### `show connection` command
Shows information for the current connection. It does not display the password.

## Querying Flights
One of the initial goals for StairCtl is to make it easier to look at flights, see what
their state is, and see how they were executed.

### `list` and `count` commands
There are three list commands. They each provide **paging** of the list. You can specify
`-o` or `--offset` for the start of the page and `-l` or `--limit` for the number of flights
to list.

* `list flights` command provides an optional `-s` or `--status` option to filter for flights in
specific states. For example, `list flights --status QUEUED`.

* `list dismal` command is a shortcut for `list flights --status FATAL`; providing a list
of flights that require manual intervention for recovery.

* `list owned` command shows flights that are currently owned by a Stairway instance. Those might
be actually running on that instance, or might be awaiting recovery.

List commands do not provide the total number of flights in their category. Since that can be
an expensive query (although recently release grooming should help that), we do not compute the
count on every list command. Instead, we provide count commands.

* `count flights` command provides an optional `-s` or `--status` option corresponding to the
`list flights` command.

* `count owned` command counts currently owned flights that would be listed by the `list owned`
command.

### `get flight` command
By default, the `get flight` command provides a summary of the flight.


## Flight Use Cases
This section provides a few capabilities of StairCtl.

### Unrecoverable Flight
We have had cases where a service is unable to start, because a Stairway
flight is not recoverable. The Stairway `recoverAndStart` call fails and
the service is unable start.

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

If you have suggestions, you can email `ddietterich@verily.com` or
add a ticket to the Jira epic [Stairway Debug Tooling]( https://broadworkbench.atlassian.net/browse/PF-678).
