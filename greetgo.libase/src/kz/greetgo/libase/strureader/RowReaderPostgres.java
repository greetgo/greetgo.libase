package kz.greetgo.libase.strureader;

import static kz.greetgo.libase.util.StrUtil.def;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class RowReaderPostgres implements RowReader {
  
  private Connection connection;
  
  public RowReaderPostgres(Connection connection) {
    this.connection = connection;
  }
  
  @Override
  public List<ColumnRow> readAllTableColumns() throws Exception {
    
    PreparedStatement ps = connection.prepareStatement("select * from information_schema.columns "
        + " where table_schema = 'public' and table_name not in "
        + " (select table_name from information_schema.views where table_schema = 'public')"
        + " order by table_name, ordinal_position");
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
  
  private static final Set<String> NOSIZE_COLS = new HashSet<>();
  static {
    NOSIZE_COLS.add("BIGINT");
    NOSIZE_COLS.add("INTEGER");
    NOSIZE_COLS.add("DOUBLE PRECISION");
  }
  
  private ColumnRow readColumnRow(ResultSet rs) throws Exception {
    ColumnRow ret = new ColumnRow();
    ret.tableName = rs.getString("table_name");
    ret.name = rs.getString("column_name");
    ret.defaultValue = rs.getString("column_default");
    ret.nullable = "YES".equals(rs.getString("is_nullable"));
    
    int charLen = rs.getInt("character_maximum_length");
    int numPrec = rs.getInt("numeric_precision");
    int numScale = rs.getInt("numeric_scale");
    
    String dataType = rs.getString("data_type");
    
    if (NOSIZE_COLS.contains(dataType.toUpperCase())) {
      ret.type = dataType;
    } else {
      ret.type = dataType + sizeToStr(charLen + numPrec, numScale);
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
    PreparedStatement ps = connection
        .prepareStatement("select * from information_schema.key_column_usage"
            + " where constraint_name in ("
            + "   select constraint_name from information_schema.table_constraints"
            + "   where constraint_schema = 'public' and constraint_type = 'PRIMARY KEY')"
            + " order by table_name, ordinal_position");
    try {
      Map<String, PrimaryKeyRow> ret = new HashMap<>();
      ResultSet rs = ps.executeQuery();
      try {
        
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
    PreparedStatement ps = connection.prepareStatement("select fk, i,\n"
        + "  conrelid ::regclass as fromTable,  a.attname as fromCol,\n"
        + "  confrelid::regclass as   toTable, af.attname as   toCol\n"
        + "from pg_attribute af, pg_attribute a,\n"
        + "  (select fk, conrelid,confrelid,conkey[i] as conkey, confkey[i] as confkey, i\n"
        + "   from (select oid as fk, conrelid,confrelid,conkey,confkey,\n"
        + "                generate_series(1,array_upper(conkey,1)) as i\n"
        + "         from pg_constraint where contype = 'f') ss) ss2\n"
        + "where af.attnum = confkey and af.attrelid = confrelid and\n"
        + "      a.attnum = conkey and a.attrelid = conrelid order by fk, i");
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
    PreparedStatement ps = connection
        .prepareStatement("select * from information_schema.sequences");
    try {
      ResultSet rs = ps.executeQuery();
      try {
        Map<String, SequenceRow> ret = new HashMap<>();
        
        while (rs.next()) {
          SequenceRow s = new SequenceRow(rs.getString("sequence_name"), rs.getLong("start_value"));
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
    PreparedStatement ps = connection.prepareStatement("select distinct view_name, table_name"
        + " from information_schema.view_column_usage order by view_name, table_name");
    try {
      ResultSet rs = ps.executeQuery();
      try {
        
        while (rs.next()) {
          String name = rs.getString("view_name");
          ViewRow view = ret.get(name);
          if (view == null) throw new NullPointerException("No view " + name);
          view.dependenses.add(rs.getString("table_name"));
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
        .prepareStatement("select * from information_schema.views where table_schema = 'public'");
    try {
      ResultSet rs = ps.executeQuery();
      try {
        Map<String, ViewRow> ret = new HashMap<>();
        
        while (rs.next()) {
          ViewRow s = new ViewRow(rs.getString("table_name"),
              killSemicolonInEnd(rs.getString("view_definition")));
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
    return fillMain(readFuncsTop(), new Cache());
  }
  
  private List<StoreFuncRow> readFuncsTop() throws SQLException {
    PreparedStatement ps = connection.prepareStatement("SELECT\n"
        + "  p.prorettype as returnType, p.proname as name, \n"
        + "  array_to_string(p.proargtypes, ';') as argTypes, \n"
        + "  array_to_string(p.proargnames, ';') as argNames,  p.prolang, p.prosrc \n"
        + "FROM    pg_catalog.pg_namespace n JOIN    pg_catalog.pg_proc p "
        + "ON      pronamespace = n.oid WHERE nspname = 'public'");
    try {
      ResultSet rs = ps.executeQuery();
      try {
        List<StoreFuncRow> ret = new ArrayList<>();
        
        while (rs.next()) {
          StoreFuncRow x = new StoreFuncRow();
          
          x.name = rs.getString("name");
          x.__argTypesStr = rs.getString("argTypes");
          x.__argNamesStr = rs.getString("argNames");
          x.__returns = rs.getString("returnType");
          x.__langId = rs.getString("prolang");
          
          x.source = rs.getString("prosrc");
          
          ret.add(x);
        }
        
        return ret;
      } finally {
        rs.close();
      }
    } finally {
      ps.close();
    }
  }
  
  private class Cache {
    
    final Map<String, String> types = new HashMap<>();
    
    public String getType(String typeId) throws Exception {
      String type = types.get(typeId);
      if (type == null) types.put(typeId, type = loadType(typeId));
      return type;
    }
    
    private String loadType(String typeId) throws Exception {
      PreparedStatement ps = connection
          .prepareStatement("select typname from pg_type where oid = ?");
      try {
        ps.setLong(1, Long.parseLong(typeId));
        ResultSet rs = ps.executeQuery();
        try {
          if (!rs.next()) throw new IllegalArgumentException("No typeId = " + typeId);
          return rs.getString(1);
        } finally {
          rs.close();
        }
      } finally {
        ps.close();
      }
    }
    
    final Map<String, String> languages = new HashMap<>();
    
    public String getLanguage(String langId) throws Exception {
      String lang = languages.get(langId);
      if (lang == null) languages.put(langId, lang = loadLanguage(langId));
      return lang;
    }
    
    private String loadLanguage(String langId) throws Exception {
      PreparedStatement ps = connection
          .prepareStatement("select lanname from pg_language where oid = ?");
      try {
        ps.setLong(1, Long.parseLong(langId));
        ResultSet rs = ps.executeQuery();
        try {
          if (!rs.next()) throw new IllegalArgumentException("No langId = " + langId);
          return rs.getString(1);
        } finally {
          rs.close();
        }
      } finally {
        ps.close();
      }
    }
    
  }
  
  private List<StoreFuncRow> fillMain(List<StoreFuncRow> funcs, Cache cache) throws Exception {
    for (StoreFuncRow sfr : funcs) {
      if (def(sfr.__argNamesStr)) for (String argName : sfr.__argNamesStr.split(";")) {
        sfr.argNames.add(argName);
      }
      if (def(sfr.__argTypesStr)) for (String argTypeId : sfr.__argTypesStr.split(";")) {
        sfr.argTypes.add(cache.getType(argTypeId));
      }
      sfr.returns = cache.getType(sfr.__returns);
      sfr.language = cache.getLanguage(sfr.__langId);
    }
    return funcs;
  }
  
  @Override
  public Map<String, TriggerRow> readAllTriggers() throws Exception {
    PreparedStatement ps = connection.prepareStatement("select * from information_schema.triggers"
        + " where trigger_schema = 'public' and event_object_schema = 'public'");
    try {
      ResultSet rs = ps.executeQuery();
      try {
        Map<String, TriggerRow> ret = new HashMap<>();
        while (rs.next()) {
          TriggerRow x = new TriggerRow();
          
          x.name = rs.getString("trigger_name");
          x.tableName = rs.getString("event_object_table");
          x.eventManipulation = rs.getString("event_manipulation");
          x.actionOrientation = rs.getString("action_orientation");
          x.actionTiming = rs.getString("action_timing");
          x.actionStatement = rs.getString("action_statement");
          
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
    s.append("with tt as (");
    s.append("  select tt.table_name from information_schema.tables tt");
    s.append("  where tt.table_schema = 'public' and table_name not in");
    s.append("  (select table_name from information_schema.views where table_schema = 'public')");
    s.append("), res as (");
    s.append("  select tt.table_name, pg_catalog.obj_description(c.oid) cmmnt");
    s.append("  from tt, pg_catalog.pg_class c");
    s.append("  where tt.table_name = c.relname");
    s.append(")");
    s.append("");
    s.append("select * from res where cmmnt is not null");
    
    PreparedStatement ps = connection.prepareStatement(s.toString());
    try {
      ResultSet rs = ps.executeQuery();
      
      try {
        Map<String, String> ret = new HashMap<>();
        while (rs.next()) {
          ret.put(rs.getString("table_name"), rs.getString("cmmnt"));
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
    s.append("with res as (");
    s.append("  select cols.table_name, cols.column_name, (");
    s.append("    select pg_catalog.col_description(oid,cols.ordinal_position::int)");
    s.append("    from pg_catalog.pg_class c where c.relname=cols.table_name");
    s.append("  ) as column_comment");
    s.append("  from information_schema.columns cols");
    s.append("  where cols.table_schema='public'");
    s.append(")");
    s.append("");
    s.append("select * from res where column_comment is not null");
    
    PreparedStatement ps = connection.prepareStatement(s.toString());
    try {
      ResultSet rs = ps.executeQuery();
      try {
        Map<String, String> ret = new HashMap<>();
        
        while (rs.next()) {
          ret.put(rs.getString("table_name") + '.' + rs.getString("column_name"),
              rs.getString("column_comment"));
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
