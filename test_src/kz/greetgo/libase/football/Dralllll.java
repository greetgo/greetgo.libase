package kz.greetgo.libase.football;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

public class Dralllll {
  public static void main(String[] args) {
    new Dralllll().run();
  }
  
  private void run() {
    String[] men = new String[] { "Агын", "Петя", "Женя", "Олег", "Костя", "Дима" };
    
    List<String> list = new ArrayList<>();
    for (String man : men) {
      list.add(man);
    }
    
    Collections.shuffle(list);
    
    List<String> list2 = new ArrayList<>();
    list2.addAll(list);
    
    List<String> pairs = new ArrayList<>();
    
    while (list.size() >= 2) {
      String one = list.remove(0);
      String two = list.remove(0);
      pairs.add(one + ", " + two);
    }
    
    if (list.size() > 0) {
      Random rnd = new Random();
      String one = list.remove(0);
      list2.remove(one);
      
      String two = list2.get(rnd.nextInt(list2.size()));
      pairs.add(one + ", " + two);
    }
    
    Collections.shuffle(pairs);
    
    int i = 1;
    for (String pair : pairs) {
      System.out.println("pair " + i++ + "  -> " + pair);
    }
  }
}
