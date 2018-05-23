package kz.greetgo.libase.changes;

import kz.greetgo.libase.model.Trigger;

public class CreateTrigger extends Change {
  public final Trigger trigger;
  
  public CreateTrigger(Trigger trigger) {
    this.trigger = trigger;
  }
}
