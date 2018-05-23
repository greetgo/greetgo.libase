package kz.greetgo.libase.strureader;

public class ColumnRow {
  public String tableName;
  public String name;
  public String type;
  public String defaultValue;
  public boolean nullable;
  
  @Override
  public String toString() {
    return "ColumnRow [tableName=" + tableName + ", name=" + name + ", type=" + type
        + ", defaultValue=" + defaultValue + ", nullable=" + nullable + "]";
  }
}
