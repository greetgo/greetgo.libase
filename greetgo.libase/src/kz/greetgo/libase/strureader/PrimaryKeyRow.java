package kz.greetgo.libase.strureader;

import java.util.ArrayList;
import java.util.List;

public class PrimaryKeyRow {
  public String tableName;
  public final List<String> keyFieldNames = new ArrayList<>();
  
  public PrimaryKeyRow(String tableName) {
    this.tableName = tableName;
  }
  
  @Override
  public String toString() {
    return "PrimaryKey [tableName=" + tableName + ", keyFieldNames=" + keyFieldNames + "]";
  }
}
