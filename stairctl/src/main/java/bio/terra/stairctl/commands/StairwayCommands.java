package bio.terra.stairctl.commands;

import bio.terra.stairctl.StairwayService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;

import java.util.List;

@ShellComponent
public class StairwayCommands {
  private final StairwayService stairwayService;

  @Autowired
  public StairwayCommands(StairwayService stairwayService) {
    this.stairwayService = stairwayService;
  }

  @ShellMethod(value = "List stairway instances", key = "list stairways")
  public void listStairways() throws Exception {

    try {
      List<String> stairwayList = stairwayService.get().listStairways();
      Output.stairwayList(stairwayList);
    } catch (Exception ex) {
      System.err.println("List flights failed: " + ex.getMessage());
      ex.printStackTrace();
    }
  }
}
