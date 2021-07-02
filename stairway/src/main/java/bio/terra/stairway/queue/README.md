# Queue Implementation Package
This package contains the work queue message formats and implementation used in Stairway.
It is **not** for client use, but until we get around to using the new Java module stuff,
the classes used by the Stairway class are public.

## Message Structure
The message structure is set up to provide several message types, identified in QueueMessageEnum.
At this time, there is only one message type {@code READY}.

The messsage structure is versioned. Each message must have a {@link QueueMessageType} that includes
the version number and the message type enum.

When a message arrives from the queue, it is deserialized into the {@link QueueMessageNoFields} form
that only includes the QueueMessageType. The version and enum are inspected, and if valid, the
message is deserialized into the appropriate message format.

This is probably over-engineered for the single message we have, but there it is.



