package kz.greetgo.libase.model;

public class Field {
  public Table owner;
  public String name, type;
  public boolean nullable = true;
  public String defaultValue;
  
  public String comment;
  
  public Field() {}
  
  public Field(Table owner, String name, String type, boolean nullable, String defaultValue) {
    this.owner = owner;
    this.name = name;
    this.type = type;
    this.nullable = nullable;
    this.defaultValue = defaultValue;
  }
  
  public Field(Table owner, String name, String type) {
    this.owner = owner;
    this.name = name;
    this.type = type;
  }
  
  @Override
  public String toString() {
    return "\n\t\t" + name + ' ' + type + (nullable ? "" :" NOT NULL")
        + (defaultValue == null ? "" :" DEFAULT " + defaultValue);
  }
  
  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((name == null) ? 0 :name.hashCode());
    result = prime * result + ((owner == null) ? 0 :owner.hashCode());
    return result;
  }
  
  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (!(obj instanceof Field)) {
      return false;
    }
    Field other = (Field)obj;
    if (name == null) {
      if (other.name != null) {
        return false;
      }
    } else if (!name.equals(other.name)) {
      return false;
    }
    if (owner == null) {
      if (other.owner != null) {
        return false;
      }
    } else if (!owner.equals(other.owner)) {
      return false;
    }
    return true;
  }
  
  public String trimComment() {
    if (comment == null) return "";
    return comment.trim();
  }
}
