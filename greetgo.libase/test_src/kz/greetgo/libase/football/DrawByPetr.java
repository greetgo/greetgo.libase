package kz.greetgo.libase.football;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class DrawByPetr {
  public static void main(String[] args) {
    new DrawByPetr().run();
  }
  
  private void run() {
    String[] men = new String[] { "Агын", "Петя", "Женя", "Олег", "Костя", "Дима" };
    
    List<String> list = new ArrayList<>();
    for (String man : men) {
      list.add(man);
    }
    
    Collections.shuffle(list);
    
    for (int i = 0; i < men.length / 2; i++) {
      System.out.println("pair " + (i + 1) + " => " + list.get(i * 2) + ", " + list.get(i * 2 + 1));
    }
  }
}
