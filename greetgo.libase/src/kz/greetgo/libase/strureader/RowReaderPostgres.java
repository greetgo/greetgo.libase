package kz.greetgo.libase.strureader;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static kz.greetgo.libase.util.StrUtil.def;

public class RowReaderPostgres implements RowReader {

  private Connection connection;

  public RowReaderPostgres(Connection connection) {
    this.connection = connection;
  }

  @Override
  public List<ColumnRow> readAllTableColumns() throws Exception {

    String sql = "select * from (select"
      + " case when table_schema = 'public' then table_name else table_schema||'.'||table_name end full_table_name,"
      + " column_name, column_default, is_nullable,"
      + " character_maximum_length, numeric_precision, numeric_scale, data_type, ordinal_position"
      + " from information_schema.columns "
      + " where table_schema in (" + schemas() + ") and table_name not in "
      + " (select table_name from information_schema.views where table_schema in (" + schemas() + "))) x"
      + " order by full_table_name, ordinal_position";

    //noinspection Duplicates
    try (PreparedStatement ps = connection.prepareStatement(sql)) {
      try (ResultSet rs = ps.executeQuery()) {
        List<ColumnRow> ret = new ArrayList<>();
        while (rs.next()) ret.add(readColumnRow(rs));
        return ret;
      }
    }
  }

  private ColumnRow readColumnRow(ResultSet rs) throws Exception {
    ColumnRow ret = new ColumnRow();
    ret.tableName = rs.getString("full_table_name");
    ret.name = rs.getString("column_name");
    ret.defaultValue = rs.getString("column_default");
    ret.nullable = "YES".equals(rs.getString("is_nullable"));

    int charLen = rs.getInt("character_maximum_length");
    int numPrecision = rs.getInt("numeric_precision");
    int numScale = rs.getInt("numeric_scale");

    String dataType = rs.getString("data_type");

    if (NO_SIZE_COLS.contains(dataType.toUpperCase())) {
      ret.type = dataType;
    } else {
      ret.type = dataType + sizeToStr(charLen + numPrecision, numScale);
    }
    return ret;
  }

  private static final Set<String> NO_SIZE_COLS = new HashSet<>();

  static {
    NO_SIZE_COLS.add("BIGINT");
    NO_SIZE_COLS.add("INTEGER");
    NO_SIZE_COLS.add("DOUBLE PRECISION");
  }


  private String sizeToStr(int size, int scale) {
    if (size <= 0) return "";
    if (scale <= 0) return "(" + size + ")";
    return "(" + size + ", " + scale + ")";
  }

  private final List<String> schemaList = new ArrayList<>();

  @Override
  public RowReader addSchema(String schemaName) {
    schemaList.add(schemaName);
    return this;
  }

  private String schemas() {
    return Stream.concat(schemaList.stream(), Stream.of("public"))
      .map(s -> "'" + s + "'")
      .collect(Collectors.joining(", "));
  }

  @Override
  public Map<String, PrimaryKeyRow> readAllTablePrimaryKeys() throws Exception {
    String sql = "select * from information_schema.key_column_usage"
      + " where constraint_name in ("
      + "   select constraint_name from information_schema.table_constraints"
      + "   where constraint_schema = 'public' and constraint_type = 'PRIMARY KEY')"
      + " order by table_name, ordinal_position";

    try (PreparedStatement ps = connection.prepareStatement(sql)) {
      Map<String, PrimaryKeyRow> ret = new HashMap<>();
      try (ResultSet rs = ps.executeQuery()) {

        //noinspection Duplicates
        while (rs.next()) {
          String tableName = rs.getString("table_name");
          PrimaryKeyRow primaryKey = ret.get(tableName);
          if (primaryKey == null) ret.put(tableName, primaryKey = new PrimaryKeyRow(tableName));
          primaryKey.keyFieldNames.add(rs.getString("column_name"));
        }

        return ret;
      }
    }
  }

  @Override
  public Map<String, ForeignKeyRow> readAllForeignKeys() throws Exception {
    String sql = "select fk, i,\n"
      + "  conrelid ::regclass as fromTable,  a.attname as fromCol,\n"
      + "  confrelid::regclass as   toTable, af.attname as   toCol\n"
      + "from pg_attribute af, pg_attribute a,\n"
      + "  (select fk, conrelid,confrelid,conkey[i] as conkey, confkey[i] as confkey, i\n"
      + "   from (select oid as fk, conrelid,confrelid,conkey,confkey,\n"
      + "                generate_series(1,array_upper(conkey,1)) as i\n"
      + "         from pg_constraint where contype = 'f') ss) ss2\n"
      + "where af.attnum = confkey and af.attrelid = confrelid and\n"
      + "      a.attnum = conkey and a.attrelid = conrelid order by fk, i";

    try (PreparedStatement ps = connection.prepareStatement(sql)) {
      try (ResultSet rs = ps.executeQuery()) {
        Map<String, ForeignKeyRow> ret = new HashMap<>();

        while (rs.next()) {
          String name = "FK" + rs.getString("fk");
          ForeignKeyRow fk = ret.get(name);
          if (fk == null) ret.put(name, fk = new ForeignKeyRow(name));
          fk.toTable = rs.getString("toTable");
          fk.fromTable = rs.getString("fromTable");
          fk.fromColumns.add(rs.getString("fromCol"));
          fk.toColumns.add(rs.getString("toCol"));
        }

        return ret;
      }
    }
  }

  @Override
  public Map<String, SequenceRow> readAllSequences() throws Exception {
    try (PreparedStatement ps = connection
      .prepareStatement("select * from information_schema.sequences")) {
      try (ResultSet rs = ps.executeQuery()) {
        Map<String, SequenceRow> ret = new HashMap<>();

        while (rs.next()) {
          SequenceRow s = new SequenceRow(rs.getString("sequence_name"), rs.getLong("start_value"));
          ret.put(s.name, s);
        }

        return ret;
      }
    }
  }

  @Override
  public Map<String, ViewRow> readAllViews() throws Exception {
    Map<String, ViewRow> ret = readViews();

    addDependencies(ret);

    return ret;
  }

  private void addDependencies(Map<String, ViewRow> ret) throws SQLException {
    String sql = "select distinct view_name, table_name"
      + " from information_schema.view_column_usage order by view_name, table_name";

    try (PreparedStatement ps = connection.prepareStatement(sql)) {
      try (ResultSet rs = ps.executeQuery()) {

        while (rs.next()) {
          String name = rs.getString("view_name");
          ViewRow view = ret.get(name);
          if (view == null) throw new NullPointerException("No view " + name);
          view.dependenses.add(rs.getString("table_name"));
        }

      }
    }
  }

  private Map<String, ViewRow> readViews() throws SQLException {
    String sql = "select * from information_schema.views where table_schema = 'public'";

    try (PreparedStatement ps = connection.prepareStatement(sql)) {
      try (ResultSet rs = ps.executeQuery()) {
        Map<String, ViewRow> ret = new HashMap<>();

        while (rs.next()) {
          ViewRow s = new ViewRow(rs.getString("table_name"),
            killSemicolonInEnd(rs.getString("view_definition")));
          ret.put(s.name, s);
        }

        return ret;
      }
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
    String sql = "SELECT\n"
      + "  p.prorettype as returnType, p.proname as name, \n"
      + "  array_to_string(p.proargtypes, ';') as argTypes, \n"
      + "  array_to_string(p.proargnames, ';') as argNames,  p.prolang, p.prosrc \n"
      + "FROM    pg_catalog.pg_namespace n JOIN    pg_catalog.pg_proc p "
      + "ON      pronamespace = n.oid WHERE nspname = 'public'";

    try (PreparedStatement ps = connection.prepareStatement(sql)) {
      try (ResultSet rs = ps.executeQuery()) {
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
      }
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
      String sql = "select typname from pg_type where oid = ?";

      try (PreparedStatement ps = connection.prepareStatement(sql)) {
        ps.setLong(1, Long.parseLong(typeId));
        try (ResultSet rs = ps.executeQuery()) {
          if (!rs.next()) throw new IllegalArgumentException("No typeId = " + typeId);
          return rs.getString(1);
        }
      }
    }

    final Map<String, String> languages = new HashMap<>();

    public String getLanguage(String langId) throws Exception {
      String lang = languages.get(langId);
      if (lang == null) languages.put(langId, lang = loadLanguage(langId));
      return lang;
    }

    private String loadLanguage(String langId) throws Exception {
      String sql = "select lanname from pg_language where oid = ?";

      try (PreparedStatement ps = connection.prepareStatement(sql)) {
        ps.setLong(1, Long.parseLong(langId));
        try (ResultSet rs = ps.executeQuery()) {
          if (!rs.next()) throw new IllegalArgumentException("No langId = " + langId);
          return rs.getString(1);
        }
      }
    }

  }

  private List<StoreFuncRow> fillMain(List<StoreFuncRow> funcs, Cache cache) throws Exception {
    for (StoreFuncRow sfr : funcs) {
      if (def(sfr.__argNamesStr)) sfr.argNames.addAll(Arrays.asList(sfr.__argNamesStr.split(";")));
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
    String sql = "select * from information_schema.triggers"
      + " where trigger_schema = 'public' and event_object_schema = 'public'";

    try (PreparedStatement ps = connection.prepareStatement(sql)) {
      try (ResultSet rs = ps.executeQuery()) {
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
      }
    }
  }

  @Override
  public Map<String, String> readTableComments() throws Exception {

    String sql = "with tt as (" +
      "  select tt.table_name from information_schema.tables tt" +
      "  where tt.table_schema = 'public' and table_name not in" +
      "  (select table_name from information_schema.views where table_schema = 'public')" +
      "), res as (" +
      "  select tt.table_name, pg_catalog.obj_description(c.oid) cmmnt" +
      "  from tt, pg_catalog.pg_class c" +
      "  where tt.table_name = c.relname" +
      ")" +
      "select * from res where cmmnt is not null";

    try (PreparedStatement ps = connection.prepareStatement(sql)) {

      try (ResultSet rs = ps.executeQuery()) {
        Map<String, String> ret = new HashMap<>();
        while (rs.next()) {
          ret.put(rs.getString("table_name"), rs.getString("cmmnt"));
        }
        return ret;
      }

    }
  }

  @Override
  public Map<String, String> readColumnComments() throws Exception {

    String sql = "with res as (" +
      "  select cols.table_name, cols.column_name, (" +
      "    select pg_catalog.col_description(oid,cols.ordinal_position::int)" +
      "    from pg_catalog.pg_class c where c.relname=cols.table_name" +
      "  ) as column_comment" +
      "  from information_schema.columns cols" +
      "  where cols.table_schema='public'" +
      ")" +
      "select * from res where column_comment is not null";

    try (PreparedStatement ps = connection.prepareStatement(sql)) {
      try (ResultSet rs = ps.executeQuery()) {
        Map<String, String> ret = new HashMap<>();

        while (rs.next()) {
          ret.put(rs.getString("table_name") + '.' + rs.getString("column_name"),
            rs.getString("column_comment"));
        }

        return ret;
      }
    }
  }
}
