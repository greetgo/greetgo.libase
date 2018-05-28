package kz.greetgo.libase.changes;

import kz.greetgo.libase.model.Field;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

public class AlterField extends Change {
  public final Field field;
  public final Field oldField;
  public final Set<AlterPartPart> alters = new HashSet<>();

  public AlterField(Field field, Field oldField, Collection<AlterPartPart> alters) {
    this.field = field;
    this.oldField = oldField;
    this.alters.addAll(alters);
  }

  public AlterField(Field field, Field oldField, AlterPartPart... alters) {
    this.field = field;
    this.oldField = oldField;
    this.alters.addAll(Arrays.asList(alters));
  }

  @Override
  public String toString() {
    return ("AlterField " + field.owner.name + " : " + oldField + " --> " + field + " " + alters
      + ", typeLenInc=" + typeLenInc()).replaceAll("\\s+", " ");
  }

  public int typeLenInc() {
    return field.typeLen - oldField.typeLen;
  }
}
