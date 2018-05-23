package kz.greetgo.libase.model;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class StoreFunc {
  public final String name;
  public final List<String> argTypes = new ArrayList<>();
  public final List<String> argNames = new ArrayList<>();
  public String returns;
  public String source, language;
  
  public StoreFunc(String name, List<String> argTypes) {
    this.name = name;
    this.argTypes.addAll(argTypes);
  }
  
  @Override
  public String toString() {
    return name + " " + argTypes + " > " + argNames + "\n    returns " + returns
        + "\n    sources {" + source + "} language " + language;
  }
  
  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((argTypes == null) ? 0 :argTypes.hashCode());
    result = prime * result + ((name == null) ? 0 :name.hashCode());
    return result;
  }
  
  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null) return false;
    if (!(obj instanceof StoreFunc)) return false;
    StoreFunc other = (StoreFunc)obj;
    if (argTypes == null) {
      if (other.argTypes != null) return false;
    } else if (!argTypes.equals(other.argTypes)) return false;
    if (name == null) {
      if (other.name != null) return false;
    } else if (!name.equals(other.name)) return false;
    return true;
  }
  
  public boolean fullEquals(StoreFunc func) {
    if (func == null) return false;
    if (!equals(func)) return false;
    
    if (!Objects.equals(returns, func.returns)) return false;
    if (!argTypes.equals(func.argTypes)) return false;
    
    return Objects.equals(source, func.source);
  }
}
