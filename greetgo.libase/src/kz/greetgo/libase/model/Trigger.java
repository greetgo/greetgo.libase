package kz.greetgo.libase.model;

import java.util.Objects;

public class Trigger {
  public final String name, tableName;
  public String eventManipulation, actionOrientation, actionTiming;
  public String actionStatement;
  
  public Trigger(String name, String tableName) {
    this.name = name;
    this.tableName = tableName;
  }
  
  public boolean fullEquals(Trigger one) {
    if (one == null) return false;
    if (!equals(one)) return false;
    
    if (!Objects.equals(eventManipulation, one.eventManipulation)) return false;
    if (!Objects.equals(actionOrientation, one.actionOrientation)) return false;
    if (!Objects.equals(actionTiming, one.actionTiming)) return false;
    if (!Objects.equals(actionStatement, one.actionStatement)) return false;
    
    return true;
  }
  
  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((name == null) ? 0 :name.hashCode());
    result = prime * result + ((tableName == null) ? 0 :tableName.hashCode());
    return result;
  }
  
  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null) return false;
    if (getClass() != obj.getClass()) return false;
    Trigger other = (Trigger)obj;
    if (name == null) {
      if (other.name != null) return false;
    } else if (!name.equals(other.name)) return false;
    if (tableName == null) {
      if (other.tableName != null) return false;
    } else if (!tableName.equals(other.tableName)) return false;
    return true;
  }
  
  @Override
  public String toString() {
    return "create trigger " + name + "\n    " + actionTiming + " " + eventManipulation + " on "
        + tableName + "\n    for each " + actionOrientation + "\n    " + actionStatement + ";;";
  }
}
