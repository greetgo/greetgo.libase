package kz.greetgo.libase.strureader;

public class TriggerRow {
  public String name, tableName;
  public String eventManipulation, actionOrientation, actionTiming;
  public String actionStatement;
  
  @Override
  public String toString() {
    return "create trigger " + name + "\n    " + actionTiming + " " + eventManipulation + " on "
        + tableName + "\n    for each " + actionOrientation + "\n    " + actionStatement + ";;";
  }
}
