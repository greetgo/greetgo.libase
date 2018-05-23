package kz.greetgo.libase.changes;

import kz.greetgo.libase.model.Relation;

public class CreateRelation extends Change {
  public final Relation relation;
  
  public CreateRelation(Relation relation) {
    this.relation = relation;
  }
}
