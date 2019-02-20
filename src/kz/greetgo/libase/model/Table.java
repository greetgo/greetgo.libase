package kz.greetgo.libase.model;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class Table extends Relation {
  public final List<Field> allFields = new ArrayList<>();
  public final List<Field> keyFields = new ArrayList<>();
  public final Set<ForeignKey> foreignKeys = new HashSet<>();
  
  public String comment;
  
  public Table() {}
  
  public Table(String name) {
    this.name = name;
  }
  
  public Field field(String name) {
    for (Field field : allFields) {
      if (field.name.equals(name)) return field;
    }
    return null;
  }
  
  @Override
  public String toString() {
    return "Table [name=" + name + ", allFields=" + allFields + "\n, keyFields=" + keyFields
        + "\n, foreignKeys=" + foreignKeys + "]";
  }
  
  @Override
  public String relationName() {
    return "table " + name;
  }
  
  public String trimComment() {
    if (comment == null) return "";
    return comment.trim();
  }
}
