package kz.greetgo.libase.changes;

import kz.greetgo.libase.model.Table;

public class TableComment extends Comment {
  public final Table table;
  
  public TableComment(Table table) {
    this.table = table;
  }
}
