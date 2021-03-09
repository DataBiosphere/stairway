package bio.terra.stairway;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class FlightMapUpgradeView {
  JsonNode root;

  public FlightMapUpgradeView(JsonNode root) {
    this.root = root;
  }

  public String getType(String key) {
    return getMap().get(key).get(0).textValue();
  }

  public JsonNode getJson(String key) {
    return getMap().get(key).get(1);
  }

  public void put(String key, Object obj) {
    getMap().putPOJO(key, obj);
  }

  public void remove(String key) {
    getMap().remove(key);
  }

  public void replace(String key, Object obj) {
    remove(key);
    put(key, obj);
  }

  public ObjectNode getMap() {
    return (ObjectNode) root.get(1);
  }
}
