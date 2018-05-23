package kz.greetgo.libase.changes;

import kz.greetgo.libase.model.Field;

public class AddTableField extends Change {
  public final Field field;
  
  public AddTableField(Field field) {
    this.field = field;
  }
}
