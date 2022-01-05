package bio.terra.stairway.impl;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import bio.terra.stairway.exception.InvalidPageToken;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("unit")
public class PageTokenTest {

  @Test
  public void pageTokenTest() throws Exception {
    Instant testInstant = Instant.now();

    // Make sure we translate properly
    PageToken token = new PageToken(testInstant);
    String externalToken = token.makeToken();
    PageToken validate = new PageToken(externalToken);
    assertThat(validate.getTimestamp(), equalTo(token.getTimestamp()));

    // Bad version
    String badVersion =
        "vxx" + StringUtils.removeStart(externalToken, PageToken.PAGE_TOKEN_VERSION);
    Assertions.assertThrows(InvalidPageToken.class, () -> new PageToken(badVersion));

    // Bad URL string
    String badUrl = PageToken.PAGE_TOKEN_VERSION + "     \\";
    Assertions.assertThrows(InvalidPageToken.class, () -> new PageToken(badUrl));

    // Bad timestamp string
    String badInstant =
        PageToken.PAGE_TOKEN_VERSION
            + URLEncoder.encode("2022-01-01x01:02:03Z", StandardCharsets.UTF_8);
    Assertions.assertThrows(InvalidPageToken.class, () -> new PageToken(badInstant));
  }
}
