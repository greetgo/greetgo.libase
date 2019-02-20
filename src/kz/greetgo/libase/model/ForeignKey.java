package kz.greetgo.libase.model;

import java.util.ArrayList;
import java.util.List;

public class ForeignKey {
  public Table from, to;
  public final List<FieldVector> vectors = new ArrayList<>();
  
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("\n\t" + from.name + "(");
    for (FieldVector fv : vectors) {
      sb.append(fv.from.name).append(", ");
    }
    sb.setLength(sb.length() - 2);
    sb.append(") --> ").append(to.name).append("(");
    for (FieldVector fv : vectors) {
      sb.append(fv.to.name).append(", ");
    }
    sb.setLength(sb.length() - 2);
    sb.append(")");
    return sb.toString();
  }
  
  public ForeignKey(Table from, Table to) {
    this.from = from;
    this.to = to;
  }
  
  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((from == null) ? 0 : from.hashCode());
    result = prime * result + ((to == null) ? 0 : to.hashCode());
    result = prime * result + ((vectors == null) ? 0 : vectors.hashCode());
    return result;
  }
  
  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null) return false;
    if (!(obj instanceof ForeignKey)) return false;
    ForeignKey other = (ForeignKey)obj;
    if (from == null) {
      if (other.from != null) return false;
    } else if (!from.equals(other.from)) return false;
    if (to == null) {
      if (other.to != null) return false;
    } else if (!to.equals(other.to)) return false;
    if (vectors == null) {
      if (other.vectors != null) return false;
    } else if (!vectors.equals(other.vectors)) return false;
    return true;
  }
}
