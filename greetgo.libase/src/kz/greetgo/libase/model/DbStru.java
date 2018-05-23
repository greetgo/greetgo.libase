package kz.greetgo.libase.model;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class DbStru {
  public final Map<String, Relation> relations = new HashMap<>();
  
  public final Set<ForeignKey> foreignKeys = new HashSet<>();
  
  public final Set<Sequence> sequences = new HashSet<>();
  
  public final Map<StoreFunc, StoreFunc> funcs = new HashMap<>();
  
  public final Map<Trigger, Trigger> triggers = new HashMap<>();
  
  public Table table(String name) {
    return (Table)relations.get(name);
  }
  
  public View view(String name) {
    return (View)relations.get(name);
  }
}
