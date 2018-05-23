package kz.greetgo.libase.changes;

import kz.greetgo.libase.model.StoreFunc;

public class CreateOrReplaceFunc extends Change {
  public final StoreFunc storeFunc;
  
  public CreateOrReplaceFunc(StoreFunc storeFunc) {
    this.storeFunc = storeFunc;
  }
}
