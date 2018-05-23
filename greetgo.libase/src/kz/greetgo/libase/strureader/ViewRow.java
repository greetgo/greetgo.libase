package kz.greetgo.libase.strureader;

import java.util.HashSet;
import java.util.Set;

public class ViewRow {
  public String name, content;
  
  public ViewRow(String name, String content) {
    this.name = name;
    this.content = content;
  }
  
  public Set<String> dependenses = new HashSet<>();
  
  @Override
  public String toString() {
    return "ViewRow [name=" + name + ", dependenses=" + dependenses + ", content=" + content + "]";
  }
}
