package bio.terra.stairway;

public enum FlightStatus {
    RUNNING,  // flight is queued to run or is running
    SUCCESS,  // flight finished successfully
    ERROR,    // flight finished with an error and cleaned up
    FATAL,    // flight had errors and was unable to clean up
    WAITING,  // flight was yielded and is waiting to be resumed
    READY,    // flight is unowned and ready to execute
    QUEUED    // flight is in the work queue awaiting execution
}
