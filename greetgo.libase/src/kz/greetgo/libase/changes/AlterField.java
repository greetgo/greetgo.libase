package kz.greetgo.libase.changes;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import kz.greetgo.libase.model.Field;

public class AlterField extends Change {
  public final Field field;
  public final Set<AlterPartPart> alters = new HashSet<>();
  
  public AlterField(Field field, Collection<AlterPartPart> alters) {
    this.field = field;
    this.alters.addAll(alters);
  }
  
  public AlterField(Field field, AlterPartPart... alters) {
    this.field = field;
    for (AlterPartPart alter : alters) {
      this.alters.add(alter);
    }
  }
}
