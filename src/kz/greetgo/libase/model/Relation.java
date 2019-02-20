package kz.greetgo.libase.model;

public abstract class Relation {
  public String name;
  
  public abstract String relationName();
  
  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + getClass().hashCode();
    result = prime * result + ((name == null) ? 0 :name.hashCode());
    return result;
  }
  
  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null) return false;
    
    if (!getClass().equals(obj.getClass())) return false;
    
    Relation other = (Relation)obj;
    
    if (name == null) {
      if (other.name != null) return false;
    } else if (!name.equals(other.name)) {
      return false;
    }
    
    return true;
  }
  
}
