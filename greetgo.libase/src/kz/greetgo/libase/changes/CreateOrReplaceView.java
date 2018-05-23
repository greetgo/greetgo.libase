package kz.greetgo.libase.changes;

import kz.greetgo.libase.model.View;

public class CreateOrReplaceView extends Change {
  public final View view;
  
  public CreateOrReplaceView(View view) {
    this.view = view;
  }
}
