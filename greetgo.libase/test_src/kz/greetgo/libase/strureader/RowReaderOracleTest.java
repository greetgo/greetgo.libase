package kz.greetgo.libase.strureader;

import kz.greetgo.libase.model.DbStru;
import kz.greetgo.libase.model.Table;
import kz.greetgo.libase.utils.DbSide;
import kz.greetgo.libase.utils.DbWorker;
import kz.greetgo.libase.utils.DbWorkerOracle;
import org.fest.assertions.data.MapEntry;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static kz.greetgo.libase.utils.TestUtil.exec;
import static org.fest.assertions.api.Assertions.assertThat;

public class RowReaderOracleTest extends RowReaderPostgresTest {

  @Override
  protected DbWorker dbWorker() {
    return new DbWorkerOracle();
  }

  @Override
  protected RowReader createRowReader(Connection con) {
    return new RowReaderOracle(con);
  }

  @Override
  protected String createTableClient(Connection con) {
    exec(con, "" +
      "create table CLIENT (" +
      "  id int primary key," +
      "  name varchar2(100)" +
      ")");
    return "CLIENT";
  }

  @Override
  protected String createTableHouse(Connection con) {
    exec(con, "" +
      "create table house (" +
      "  id int primary key," +
      "  owner_id int references client(id)," +
      "  name varchar(100)" +
      ")");
    return "house";
  }

  @Test
  public void readAllTableColumns() throws Exception {

    DbWorker dbWorker = dbWorker();

    dbWorker.recreateDb(DbSide.FROM);

    try (Connection con = dbWorker.connection(DbSide.FROM)) {
      exec(con, "create table client (id int primary key, name varchar2(100))");
      exec(con, "create table hello (identifier int primary key, door varchar2(100))");

      RowReader rowReader = createRowReader(con);

      //
      //
      List<ColumnRow> columnRowList = rowReader.readAllTableColumns();
      //
      //

      assertThat(columnRowList).hasSize(4);

      assertThat(columnRowList.get(0).tableName).isEqualTo("CLIENT");
      assertThat(columnRowList.get(1).tableName).isEqualTo("CLIENT");
      assertThat(columnRowList.get(2).tableName).isEqualTo("HELLO");
      assertThat(columnRowList.get(3).tableName).isEqualTo("HELLO");

      assertThat(columnRowList.get(0).name).isEqualTo("ID");
      assertThat(columnRowList.get(1).name).isEqualTo("NAME");
      assertThat(columnRowList.get(2).name).isEqualTo("IDENTIFIER");
      assertThat(columnRowList.get(3).name).isEqualTo("DOOR");
    }
  }

  @Override
  public void readAllTablePrimaryKeys() throws Exception {

    DbWorker dbWorker = dbWorker();

    dbWorker.recreateDb(DbSide.FROM);

    try (Connection con = dbWorker.connection(DbSide.FROM)) {
      exec(con, "create table client (" +
        "  id1 int," +
        "  id2 int," +
        "  name varchar(100)," +
        "  primary key(id1, id2)" +
        ")");
      exec(con, "create table hello (" +
        "  identifier1 int," +
        "  identifier2 int," +
        "  identifier3 int," +
        "  door varchar(100)," +
        "  primary key (identifier1, identifier2, identifier3)" +
        ")");

      RowReader rowReader = createRowReader(con);

      //
      //
      Map<String, PrimaryKeyRow> map = rowReader.readAllTablePrimaryKeys();
      //
      //

      assertThat(map).hasSize(2);

      assertThat(map).containsKey("CLIENT");
      assertThat(map).containsKey("HELLO");

      PrimaryKeyRow client = map.get("CLIENT");
      assertThat(client.tableName).isEqualTo("CLIENT");
      assertThat(client.keyFieldNames).hasSize(2);
      assertThat(client.keyFieldNames.get(0)).isEqualTo("ID1");
      assertThat(client.keyFieldNames.get(1)).isEqualTo("ID2");

      PrimaryKeyRow hello = map.get("HELLO");
      assertThat(hello.tableName).isEqualTo("HELLO");
      assertThat(hello.keyFieldNames).hasSize(3);
      assertThat(hello.keyFieldNames.get(0)).isEqualTo("IDENTIFIER1");
      assertThat(hello.keyFieldNames.get(1)).isEqualTo("IDENTIFIER2");
      assertThat(hello.keyFieldNames.get(2)).isEqualTo("IDENTIFIER3");
    }
  }

  @Override
  public void readTableHello() throws Exception {

    DbWorker dbWorker = dbWorker();

    dbWorker.recreateDb(DbSide.FROM);

    try (Connection con = dbWorker.connection(DbSide.FROM)) {
      exec(con, "create table hello (id int primary key, name varchar(100))");

      DbStru struct = StruReader.read(createRowReader(con));

      Table table = struct.table("HELLO");
      assertThat(table).isNotNull();
      assertThat(table.keyFields).hasSize(1);
      assertThat(table.keyFields.get(0).name.toLowerCase()).isEqualTo("id");
    }
  }


  @Override
  protected Consumer<Map<String, ForeignKeyRow>> readAllForeignKeys_createTableClient(Connection con) {
    exec(con, "create table client (" +
      "  id1 int," +
      "  id2 int," +
      "  name varchar2(100)," +
      "  primary key(id1, id2)" +
      ")");
    return (Map<String, ForeignKeyRow> map) -> {};
  }

  @Override
  protected Consumer<Map<String, ForeignKeyRow>> readAllForeignKeys_createTablePhone(Connection con) {
    exec(con, "create table phone (" +
      "  first_id  varchar2(50)," +
      "  second_id number," +
      "  client_id1 int," +
      "  client_id2 int," +
      "  phone_number varchar2(300)," +
      "  constraint k001 foreign key (client_id1, client_id2) references client(id1, id2)," +
      "  primary key(first_id, second_id)" +
      ")");
    return (Map<String, ForeignKeyRow> map) -> {
      assertThat(map).containsKey("FKK001");
      ForeignKeyRow row = map.get("FKK001");
      assertThat(row.fromTable).isEqualTo("PHONE");
      assertThat(row.toTable).isEqualTo("CLIENT");
      assertThat(row.fromColumns).containsExactly("CLIENT_ID1", "CLIENT_ID2");
      assertThat(row.toColumns).containsExactly("ID1", "ID2");
    };
  }

  @Override
  protected Consumer<Map<String, ForeignKeyRow>> readAllForeignKeys_createTablePhoneCallType(Connection con) {
    exec(con, "create table phone_call_type (" +
      "  code varchar2(150)," +
      "  phone_first_id varchar2(50)," +
      "  phone_second_id number," +
      "  description varchar2(100)," +
      "  constraint k002 foreign key (phone_first_id, phone_second_id) references phone(first_id, second_id)," +
      "  primary key(code)" +
      ")");
    return (Map<String, ForeignKeyRow> map) -> {
      assertThat(map).containsKey("FKK002");
      ForeignKeyRow row = map.get("FKK002");
      assertThat(row.fromTable).isEqualTo("PHONE_CALL_TYPE");
      assertThat(row.toTable).isEqualTo("PHONE");
      assertThat(row.fromColumns).containsExactly("PHONE_FIRST_ID", "PHONE_SECOND_ID");
      assertThat(row.toColumns).containsExactly("FIRST_ID", "SECOND_ID");
    };
  }


  @Override
  protected Consumer<Map<String, SequenceRow>> readAllSequences_createSequenceClient(Connection con) {
    exec(con, "create sequence client");
    return map -> {
      assertThat(map).containsKey("CLIENT");
      SequenceRow row = map.get("CLIENT");
      assertThat(row.name).isEqualTo("CLIENT");
      assertThat(row.startFrom).isEqualTo(1);
    };
  }

  @Override
  protected Consumer<Map<String, SequenceRow>> readAllSequences_createSequencePhone(Connection con) {
    exec(con, "create sequence phone start with 12000");
    return map -> {
      assertThat(map).containsKey("PHONE");
      SequenceRow row = map.get("PHONE");
      assertThat(row.name).isEqualTo("PHONE");
      assertThat(row.startFrom).isEqualTo(12000);
    };
  }

  @Override
  protected Consumer<Map<String, SequenceRow>> readAllSequences_createSequenceLeftHello(Connection con) {
    return map -> {};
  }

  @Override
  protected Consumer<Map<String, ViewRow>> readAllViews_createViewClient(Connection con) {
    exec(con, "create table client1 (id int primary key, code int, f1 int)");
    exec(con, "create table client2 (id int primary key, code int, f2 int)");
    exec(con, "create view client as select c1.id as id1, c2.id as id2, c1.code, c1.f1, c2.f2" +
      " from client1 c1, client2 c2 where c1.code = c2.code");
    return map -> {
      assertThat(map).containsKey("CLIENT");
      ViewRow row = map.get("CLIENT");
      assertThat(row.name).isEqualTo("CLIENT");
      assertThat(row.dependenses).containsAll(Arrays.asList("CLIENT1", "CLIENT2"));
      assertThat(row.content.replaceAll("\\s+", " "))
        .isEqualTo("select c1.id as id1, c2.id as id2, c1.code, c1.f1, c2.f2" +
          " from client1 c1, client2 c2 where c1.code = c2.code");
    };
  }

  @Override
  protected Consumer<Map<String, ViewRow>> readAllViews_createViewPhone(Connection con) {
    return map -> {};
  }

  @Override
  protected Consumer<Map<String, ViewRow>> readAllViews_createViewHello(Connection con) {
    return map -> {};
  }

  @Override
  protected Consumer<Map<String, String>> readTableComments_createTableClient(Connection con) {
    exec(con, "create table client(id int)");
    exec(con, "comment on table client is 'Hello Client'");
    return map -> {
      assertThat(map).containsKey("CLIENT");
      assertThat(map.get("CLIENT")).isEqualTo("Hello Client");
    };
  }

  @Override
  protected Consumer<Map<String, String>> readTableComments_createTablePhone(Connection con) {
    exec(con, "create table PHONE(id int)");
    exec(con, "comment on table PHONE is 'Hello Phone'");
    return map -> {
      assertThat(map).containsKey("PHONE");
      assertThat(map.get("PHONE")).isEqualTo("Hello Phone");
    };
  }

  @Override
  protected Consumer<Map<String, String>> readTableComments_createTableHello(Connection con) {
    return map -> {};
  }

  @Override
  protected Consumer<Map<String, String>> readColumnComments_createTableClient(Connection con) {
    exec(con, "create table client (id int, name int)");
    exec(con, "comment on column client.id   is 'Hello client id'  ");
    exec(con, "comment on column client.name is 'Hello client name'");
    return map -> {
      assertThat(map).contains(MapEntry.entry("CLIENT.ID", "Hello client id"));
      assertThat(map).contains(MapEntry.entry("CLIENT.NAME", "Hello client name"));
    };
  }

  @Override
  protected Consumer<Map<String, String>> readColumnComments_createTablePhone(Connection con) {
    exec(con, "create table phone (id int, name int)");
    exec(con, "comment on column phone.id   is 'Hello phone id'  ");
    exec(con, "comment on column phone.name is 'Hello phone name'");
    return map -> {
      assertThat(map).contains(MapEntry.entry("PHONE.ID", "Hello phone id"));
      assertThat(map).contains(MapEntry.entry("PHONE.NAME", "Hello phone name"));
    };
  }

  @Override
  protected Consumer<Map<String, String>> readColumnComments_createTableHello(Connection con) {
    return map -> {};
  }

  @Override
  protected Consumer<Map<String, StoreFuncRow>> readAllFuncs_createFuncHelloPlus(Connection con) {
    exec(con, "" +
      "create function hello_plus(a int, b int) returns int\n" +
      "begin\n" +
      "  return a + b;\n" +
      "end ;\n"
    );
    return map -> {
      assertThat(map).containsKey("HELLO_PLUS");
      StoreFuncRow row = map.get("HELLO_PLUS");
      assertThat(row.name).isEqualTo("HELLO_PLUS");
      assertThat(row.argTypes).isEmpty();
      assertThat(row.argNames).isEmpty();
      assertThat(row.returns).isNull();
      assertThat(row.language).isNull();
      assertThat(row.source.replaceAll("\\s+", " "))
        .isEqualTo("function hello_plus(a int, b int) returns int begin return a + b; end ; ");
    };
  }

  @Override
  protected Consumer<Map<String, StoreFuncRow>> readAllFuncs_createFuncHelloMinus(Connection con) {
    return map -> {};
  }

  @Override
  protected Consumer<Map<String, StoreFuncRow>> readAllFuncs_createFuncHelloMul(Connection con) {
    return map -> {};
  }

  @Override
  protected Consumer<Map<String, TriggerRow>> readAllTriggers_createTriggerSet1(Connection con) {
    exec(con, "create table chair (id int, value int)");
    exec(con, "create trigger chair_set_1 before insert on chair for each row begin :NEW.value := 1; end");
    return map -> {
      assertThat(map).containsKey("CHAIR_SET_1");
      TriggerRow row = map.get("CHAIR_SET_1");
      assertThat(row.tableName).isEqualTo("CHAIR");
      assertThat(row.actionStatement).isEqualTo("begin :NEW.value := 1; end");
      assertThat(row.actionOrientation).isNull();
      assertThat(row.actionTiming).isNull();
      assertThat(row.eventManipulation).isEqualTo("chair_set_1 before insert on chair for each row ");
    };
  }

  @Override
  protected Consumer<Map<String, TriggerRow>> readAllTriggers_createTriggerSet2(Connection con) {
    return map -> {};
  }
}
