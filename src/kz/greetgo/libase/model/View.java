package kz.greetgo.libase.model;

import java.util.HashSet;
import java.util.Set;

public class View extends Relation {
  public String content;
  public final Set<Relation> dependences = new HashSet<>();
  
  public View(String name, String content) {
    this.name = name;
    this.content = content;
  }
  
  @Override
  public String toString() {
    StringBuilder ret = new StringBuilder("view " + name + " has ");
    for (Relation r : dependences) {
      ret.append(r.name).append(' ');
    }
    ret.append('[').append(content).append(']');
    return ret.toString();
  }
  
  @Override
  public String relationName() {
    return "view " + name;
  }
}
