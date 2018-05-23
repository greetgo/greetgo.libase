package kz.greetgo.libase.model;

public class Creator {
  public static void addTable(DbStru stru, String instru) {
    String[] instrus = instru.split(",");
    
    String[] firsts = instrus[0].trim().split("\\s+");
    Table table = new Table(firsts[0]);
    addField(table, firsts, 1);
    
    for (int i = 1, C = instrus.length; i < C; i++) {
      addField(table, instrus[i].trim().split("\\s+"), 0);
    }
    
    stru.relations.put(table.name, table);
  }
  
  private static void addField(Table table, String[] fieldInfo, int offset) {
    String name = fieldInfo[offset + 0];
    String type = fieldInfo[offset + 1];
    boolean isKey = name.startsWith("*");
    if (isKey) name = name.substring(1);
    Field field = new Field(table, name, type);
    table.allFields.add(field);
    if (isKey) table.keyFields.add(field);
    for (int i = offset + 2, C = fieldInfo.length; i < C; i++) {
      String part = fieldInfo[i];
      if ("notnull".equals(part)) {
        field.nullable = false;
        continue;
      }
      if (part.startsWith("def")) {
        field.defaultValue = part.substring(3);
        continue;
      }
    }
  }
  
  public static void addView(DbStru stru, String instru) {
    String[] pair = instru.split("\\|");
    String[] left = pair[0].trim().split("\\s+");
    View view = new View(left[0].trim(), pair[1].trim());
    for (int i = 1, C = left.length; i < C; i++) {
      Relation relation = stru.relations.get(left[i].trim());
      if (relation == null) throw new NullPointerException("No relation with name " + left[i]);
      view.dependences.add(relation);
    }
    stru.relations.put(view.name, view);
  }
}
