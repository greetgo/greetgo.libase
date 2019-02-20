package kz.greetgo.libase.strureader;

import java.util.ArrayList;
import java.util.List;

public class ForeignKeyRow {
  public final String name;
  public String fromTable, toTable;
  public final List<String> fromColumns = new ArrayList<>();
  public final List<String> toColumns = new ArrayList<>();
  
  public ForeignKeyRow(String name) {
    this.name = name;
  }
  
  @Override
  public String toString() {
    return "ForeignKeyRow [name=" + name + ", fromTable=" + fromTable + ", toTable=" + toTable
        + ", fromColumns=" + fromColumns + ", toColumns=" + toColumns + "]";
  }
}
