package kz.greetgo.libase.changes;

import kz.greetgo.libase.model.Trigger;

public class DropCreateTrigger extends Change {
  public final Trigger trigger;
  
  public DropCreateTrigger(Trigger trigger) {
    this.trigger = trigger;
  }
}
