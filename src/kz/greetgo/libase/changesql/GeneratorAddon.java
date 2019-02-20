package kz.greetgo.libase.changesql;

import kz.greetgo.libase.model.Field;
import kz.greetgo.libase.model.Table;

public class GeneratorAddon {
  
  public static String generateTableComment(Table table) {
    return "COMMENT ON TABLE " + table.name + " IS '" + table.comment + "'";
  }
  
  public static String generateFieldComment(Table table, Field field) {
    return "comment on column " + table.name + '.' + field.name + " is '" + field.comment + "'";
  }
  
}
