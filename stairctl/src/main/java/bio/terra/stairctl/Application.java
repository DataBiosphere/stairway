package bio.terra.stairctl;

import org.jline.utils.AttributedString;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.shell.jline.PromptProvider;

@SpringBootApplication
public class Application {

  public static void main(String[] args) {
    SpringApplication.run(Application.class, args);
  }

  @Bean
  public PromptProvider stairtoolPromptProvider() {
    return () -> new AttributedString("stairctl> ");
  }
}
