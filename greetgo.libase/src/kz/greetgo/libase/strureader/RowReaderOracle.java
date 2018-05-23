package kz.greetgo.libase.strureader;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class RowReaderOracle implements RowReader {
  
  private Connection connection;
  
  public RowReaderOracle(Connection connection) {
    this.connection = connection;
  }
  
  @Override
  public List<ColumnRow> readAllTableColumns() throws Exception {
    
    PreparedStatement ps = connection.prepareStatement("select * from all_tab_columns"
        + " where owner = sys_context('USERENV','SESSION_SCHEMA')"
        + " and table_name not in (select view_name from all_views"
        + " where owner = sys_context('USERENV','SESSION_SCHEMA'))"
        + " order by table_name, column_id");
    try {
      ResultSet rs = ps.executeQuery();
      try {
        List<ColumnRow> ret = new ArrayList<>();
        while (rs.next()) {
          ret.add(readColumnRow(rs));
        }
        return ret;
      } finally {
        rs.close();
      }
    } finally {
      ps.close();
    }
  }
  
  private static final Set<String> NUMS = new HashSet<>();
  private static final Set<String> WITH_DATALEN = new HashSet<>();
  static {
    NUMS.add("NUMBER");
    NUMS.add("FLOAT");
    
    WITH_DATALEN.add("VARCHAR2");
    WITH_DATALEN.add("VARCHAR");
  }
  
  private ColumnRow readColumnRow(ResultSet rs) throws Exception {
    ColumnRow ret = new ColumnRow();
    ret.tableName = rs.getString("table_name");
    ret.name = rs.getString("column_name");
    ret.defaultValue = rs.getString("data_default");
    if (ret.defaultValue != null) ret.defaultValue = ret.defaultValue.trim();
    ret.nullable = "Y".equals(rs.getString("nullable"));
    
    int dataLen = rs.getInt("data_length");
    int dataPrec = rs.getInt("data_precision");
    int dataScale = rs.getInt("data_scale");
    
    String dataType = rs.getString("data_type");
    String dataTypeU = dataType.toUpperCase();
    
    if ("TIMESTAMP(6)".equals(dataTypeU)) {
      ret.type = dataType;
    } else if (NUMS.contains(dataTypeU)) {
      ret.type = dataType + sizeToStr(dataPrec, dataScale);
    } else if (WITH_DATALEN.contains(dataTypeU)) {
      ret.type = dataType + "(" + dataLen + ")";
    } else {
      ret.type = dataType;
    }
    return ret;
  }
  
  private String sizeToStr(int size, int scale) {
    if (size <= 0) return "";
    if (scale <= 0) return "(" + size + ")";
    return "(" + size + ", " + scale + ")";
  }
  
  @Override
  public Map<String, PrimaryKeyRow> readAllTablePrimaryKeys() throws Exception {
    PreparedStatement ps = connection.prepareStatement("SELECT \n"
        + "    cols.table_name, cols.column_name, cols.position,\n"
        + "    cons.status, cons.owner\n" //
        + "FROM all_constraints cons, all_cons_columns cols\n"
        + "WHERE cons.constraint_type = 'P'\n"
        + "AND cons.constraint_name = cols.constraint_name AND cons.owner = cols.owner\n"
        + "and cons.owner = sys_context('USERENV','SESSION_SCHEMA')\n" //
        + "and cols.table_name in (select table_name from all_tables \n"
        + "      where owner = sys_context('USERENV','SESSION_SCHEMA')\n" //
        + "  )\n" //
        + "ORDER BY cols.table_name, cols.position");
    try {
      ResultSet rs = ps.executeQuery();
      try {
        Map<String, PrimaryKeyRow> ret = new HashMap<>();
        
        while (rs.next()) {
          String tableName = rs.getString("table_name");
          PrimaryKeyRow primaryKey = ret.get(tableName);
          if (primaryKey == null) ret.put(tableName, primaryKey = new PrimaryKeyRow(tableName));
          primaryKey.keyFieldNames.add(rs.getString("column_name"));
        }
        
        return ret;
      } finally {
        rs.close();
      }
    } finally {
      ps.close();
    }
  }
  
  @Override
  public Map<String, ForeignKeyRow> readAllForeignKeys() throws Exception {
    PreparedStatement ps = connection
        .prepareStatement("SELECT a.constraint_name as fk, b.position as i,\n"
            + "   a.table_name as fromtable, a.column_name as fromcol, \n"
            + "   c_pk.table_name as totable,  b.column_name as tocol\n"
            + "  FROM all_cons_columns a\n" //
            + "  JOIN all_constraints c ON a.owner = c.owner\n"
            + "       AND a.constraint_name = c.constraint_name\n"
            + "  JOIN all_constraints c_pk ON c.r_owner = c_pk.owner\n"
            + "       AND c.r_constraint_name = c_pk.constraint_name\n"
            + "  JOIN all_cons_columns b ON C_PK.owner = b.owner\n"
            + "       AND  C_PK.CONSTRAINT_NAME = b.constraint_name AND b.POSITION = a.POSITION\n"
            + " WHERE c.constraint_type = 'R'\n"
            + " AND a.owner = sys_context('USERENV','SESSION_SCHEMA')\n"//
            + " order by fromtable, i");
    try {
      ResultSet rs = ps.executeQuery();
      try {
        Map<String, ForeignKeyRow> ret = new HashMap<>();
        
        while (rs.next()) {
          String name = "FK" + rs.getString("fk");
          ForeignKeyRow fk = ret.get(name);
          if (fk == null) ret.put(name, fk = new ForeignKeyRow(name));
          fk.fromTable = rs.getString("fromTable");
          fk.toTable = rs.getString("toTable");
          fk.fromColumns.add(rs.getString("fromCol"));
          fk.toColumns.add(rs.getString("toCol"));
        }
        
        return ret;
      } finally {
        rs.close();
      }
    } finally {
      ps.close();
    }
  }
  
  @Override
  public Map<String, SequenceRow> readAllSequences() throws Exception {
    PreparedStatement ps = connection.prepareStatement("select * from all_sequences"
        + " where sequence_owner = sys_context('USERENV','SESSION_SCHEMA')");
    try {
      ResultSet rs = ps.executeQuery();
      try {
        Map<String, SequenceRow> ret = new HashMap<>();
        
        while (rs.next()) {
          SequenceRow s = new SequenceRow(rs.getString("sequence_name"), rs.getLong("last_number"));
          ret.put(s.name, s);
        }
        
        return ret;
      } finally {
        rs.close();
      }
    } finally {
      ps.close();
    }
  }
  
  @Override
  public Map<String, ViewRow> readAllViews() throws Exception {
    Map<String, ViewRow> ret = readViews();
    
    addDependences(ret);
    
    return ret;
  }
  
  private void addDependences(Map<String, ViewRow> ret) throws SQLException {
    PreparedStatement ps = connection.prepareStatement("select * from all_dependencies \n"
        + "where owner = sys_context('USERENV','SESSION_SCHEMA')\n" //
        + "and type = 'VIEW'\n"//
        + "order by name");
    try {
      ResultSet rs = ps.executeQuery();
      try {
        
        while (rs.next()) {
          String name = rs.getString("name");
          ViewRow view = ret.get(name);
          if (view == null) throw new NullPointerException("No view " + name);
          view.dependenses.add(rs.getString("referenced_name"));
        }
        
      } finally {
        rs.close();
      }
    } finally {
      ps.close();
    }
  }
  
  private Map<String, ViewRow> readViews() throws SQLException {
    PreparedStatement ps = connection
        .prepareStatement("select * from all_views where owner = sys_context('USERENV','SESSION_SCHEMA')");
    try {
      ResultSet rs = ps.executeQuery();
      try {
        Map<String, ViewRow> ret = new HashMap<>();
        
        while (rs.next()) {
          ViewRow s = new ViewRow(rs.getString("view_name"),
              killSemicolonInEnd(rs.getString("text")));
          ret.put(s.name, s);
        }
        
        return ret;
      } finally {
        rs.close();
      }
    } finally {
      ps.close();
    }
  }
  
  private static String killSemicolonInEnd(String str) {
    if (str == null) return null;
    str = str.trim();
    if (str.endsWith(";")) return str.substring(0, str.length() - 1).trim();
    return str;
  }
  
  @Override
  public List<StoreFuncRow> readAllFuncs() throws Exception {
    PreparedStatement ps = connection.prepareStatement("select * from all_source"
        + " where owner = sys_context('USERENV','SESSION_SCHEMA')" //
        + " order by name, line");
    try {
      ResultSet rs = ps.executeQuery();
      try {
        Map<String, StringBuilder> lines = new HashMap<>();
        
        while (rs.next()) {
          
          String name = rs.getString("name");
          String line = rs.getString("text");
          
          StringBuilder sb = lines.get(name);
          if (sb == null) {
            lines.put(name, sb = new StringBuilder());
          }
          
          sb.append(line);
          
        }
        
        List<StoreFuncRow> ret = new ArrayList<>();
        
        for (Entry<String, StringBuilder> e : lines.entrySet()) {
          StoreFuncRow row = new StoreFuncRow();
          row.name = e.getKey();
          row.source = e.getValue().toString();
          ret.add(row);
        }
        
        return ret;
      } finally {
        rs.close();
      }
    } finally {
      ps.close();
    }
  }
  
  @Override
  public Map<String, TriggerRow> readAllTriggers() throws Exception {
    PreparedStatement ps = connection.prepareStatement("select * from all_triggers"
        + " where owner = sys_context('USERENV','SESSION_SCHEMA')");
    try {
      ResultSet rs = ps.executeQuery();
      try {
        Map<String, TriggerRow> ret = new HashMap<>();
        while (rs.next()) {
          TriggerRow x = new TriggerRow();
          
          x.name = rs.getString("trigger_name");
          x.tableName = rs.getString("table_name");
          x.eventManipulation = rs.getString("description");
          x.actionStatement = rs.getString("trigger_body");
          
          ret.put(x.name, x);
        }
        
        return ret;
      } finally {
        rs.close();
      }
    } finally {
      ps.close();
    }
  }
  
  @Override
  public Map<String, String> readTableComments() throws Exception {
    StringBuilder s = new StringBuilder();
    s.append("select * from all_tab_comments");
    s.append("  where owner = sys_context('USERENV','SESSION_SCHEMA')");
    s.append("  and comments is not null");
    
    PreparedStatement ps = connection.prepareStatement(s.toString());
    
    try {
      ResultSet rs = ps.executeQuery();
      try {
        
        Map<String, String> ret = new HashMap<>();
        while (rs.next()) {
          ret.put(rs.getString("TABLE_NAME"), rs.getString("COMMENTS"));
        }
        return ret;
        
      } finally {
        rs.close();
      }
      
    } finally {
      ps.close();
    }
    
  }
  
  @Override
  public Map<String, String> readColumnComments() throws Exception {
    StringBuilder s = new StringBuilder();
    s.append("select * from all_col_comments");
    s.append("  where owner = sys_context('USERENV','SESSION_SCHEMA')");
    s.append("  and comments is not null");
    
    PreparedStatement ps = connection.prepareStatement(s.toString());
    
    try {
      ResultSet rs = ps.executeQuery();
      try {
        
        Map<String, String> ret = new HashMap<>();
        while (rs.next()) {
          ret.put(rs.getString("TABLE_NAME") + '.' + rs.getString("COLUMN_NAME"),
              rs.getString("COMMENTS"));
        }
        return ret;
        
      } finally {
        rs.close();
      }
      
    } finally {
      ps.close();
    }
  }
}
