package kz.greetgo.libase.changes;

import kz.greetgo.libase.model.ForeignKey;

public class CreateForeignKey extends Change {
  public final ForeignKey foreignKey;
  
  public CreateForeignKey(ForeignKey foreignKey) {
    this.foreignKey = foreignKey;
  }
}
