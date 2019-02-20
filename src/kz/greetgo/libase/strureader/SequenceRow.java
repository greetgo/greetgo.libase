package kz.greetgo.libase.strureader;

public class SequenceRow {
  public String name;
  public long startFrom;
  
  public SequenceRow(String name, long startFrom) {
    this.name = name;
    this.startFrom = startFrom;
  }
  
  @Override
  public String toString() {
    return "SequenceRow [name=" + name + ", startFrom=" + startFrom + "]";
  }
}
