package bio.terra.stairway;

public enum Direction {
  START, // Start at the 0th step
  DO, // Running steps forward
  UNDO, // Running steps backwards
  SWITCH; // Switching from forward to backward
}
