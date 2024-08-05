package bio.terra.stairctl.commands;

import bio.terra.stairctl.StairwayService;
import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.shell.Availability;
import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;
import org.springframework.shell.standard.ShellMethodAvailability;

@ShellComponent
public class StairwayCommands {
  private final StairwayService stairwayService;
  private final Output output;

  @Autowired
  public StairwayCommands(StairwayService stairwayService, Output output) {
    this.stairwayService = stairwayService;
    this.output = output;
  }

  // None of the stairway command work if we are not connected, so list all commands here
  // so they will not be available.
  @ShellMethodAvailability({"list stairways"})
  public Availability availabilityCheck() {
    return stairwayService.isConnected()
        ? Availability.available()
        : Availability.unavailable("you are not connected");
  }

  @ShellMethod(value = "List stairway instances", key = "list stairways")
  public void listStairways() {
    try {
      List<String> stairwayList = stairwayService.getControl().listStairways();
      output.stairwayList(stairwayList);
    } catch (Exception ex) {
      output.error("List stairways failed: ", ex);
    }
  }
}
