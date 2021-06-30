
# Stairway Schema Notes

## Using stairwayName as the stairwayId
In the current state, the `stairwayinstance` table maps from the client-provided stairway name to
an internal shortUUID. On the one hand, using an internal id provides a uniform shape of ids
and saves a few bytes of space in the database. However,
it means that an id to name lookup needs to be done in order to provide logging information.

Instead, we will convert to using the `stairwayName` as the `stairwayId` and eventually remove
the `stairwayName` column. There may be stairway databases with old ids in them, so we have
to do this in a two steps.

### Step 1: name and id are identical
This is upward compatible from the current state. The code continues to lookup by id, so both
old and new ids will work. This change is being made as part of PF-311.

### Step 2: remove the name column
After enough time has passed that we think there are no more old id columns, we can take the next
step. In our current state, we have no guarantees about enough time. We might want to wait until we
have implemented old flight cleanup and then waited for that time to elapse.

We should remove the column that is easiest. I think that is the `stairwayName` column.
Removal means:
* Make `stairwayId` the primary key
* Drop the `stairwayName` column
* No name to id lookups are needed anymore, so that interface can be removed
* Existence and state checking is needed for recovery, but that can be conveyed with a
boolean, rather than returning the stairwayName (which is also the input param).
