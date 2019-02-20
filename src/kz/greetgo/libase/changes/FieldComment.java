package kz.greetgo.libase.changes;

import kz.greetgo.libase.model.Field;
import kz.greetgo.libase.model.Table;

public class FieldComment extends Comment {
  public final Field field;
  public final Table table;

  public FieldComment(Table table, Field field) {
    this.table = table;
    this.field = field;
  }

  @Override
  public String toString() {
    return "FieldComment : " + table.name + "." + field.name
      + " -> " + (field.comment == null ? "<NULL>" : "[[" + field.comment + "]]");
  }
}
