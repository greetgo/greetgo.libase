package kz.greetgo.libase.changes;

import kz.greetgo.libase.model.Sequence;

public class CreateSequence extends Change {
  public final Sequence sequence;
  
  public CreateSequence(Sequence sequence) {
    this.sequence = sequence;
  }
}
