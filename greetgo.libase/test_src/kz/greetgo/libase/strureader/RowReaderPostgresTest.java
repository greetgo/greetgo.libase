package kz.greetgo.libase.strureader;

import kz.greetgo.libase.model.DbStru;
import kz.greetgo.libase.model.ForeignKey;
import kz.greetgo.libase.model.Table;
import kz.greetgo.libase.utils.DbSide;
import kz.greetgo.libase.utils.DbWorker;
import kz.greetgo.libase.utils.DbWorkerPostgres;
import org.fest.assertions.data.MapEntry;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static kz.greetgo.libase.utils.TestUtil.exec;
import static org.fest.assertions.api.Assertions.assertThat;

public class RowReaderPostgresTest {
  protected DbWorker dbWorker() {
    return new DbWorkerPostgres();
  }

  protected RowReader createRowReader(Connection con) {
    return new RowReaderPostgres(con).addSchema("moon");
  }

  protected String createTableClient(Connection con) {
    exec(con, "" +
      "create table client (" +
      "  id int primary key," +
      "  name varchar(100)" +
      ")");
    return "client";
  }

  @SuppressWarnings("UnusedReturnValue")
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
  public void readTableClient() throws Exception {
    DbWorker dbWorker = dbWorker();

    dbWorker.recreateDb(DbSide.FROM);
    try (Connection con = dbWorker.connection(DbSide.FROM)) {
      String clientName = createTableClient(con);

      DbStru struct = StruReader.read(createRowReader(con));

      Table client = struct.table(clientName);
      assertThat(client).isNotNull();
      assertThat(client.keyFields).hasSize(1);
      assertThat(client.keyFields.get(0).name.toLowerCase()).isEqualTo("id");
    }
  }

  @Test
  public void foreignKey() throws Exception {
    DbWorker dbWorker = dbWorker();

    dbWorker.recreateDb(DbSide.FROM);
    try (Connection con = dbWorker.connection(DbSide.FROM)) {
      createTableClient(con);
      createTableHouse(con);

      DbStru struct = StruReader.read(createRowReader(con));

      assertThat(struct.foreignKeys).hasSize(1);
      ForeignKey foreignKey = struct.foreignKeys.iterator().next();
      assertThat(foreignKey.from.name.toLowerCase()).isEqualTo("house");
      assertThat(foreignKey.to.name.toLowerCase()).isEqualTo("client");
    }
  }

  @Test
  public void readTableHello() throws Exception {

    DbWorker dbWorker = dbWorker();

    dbWorker.recreateDb(DbSide.FROM);

    try (Connection con = dbWorker.connection(DbSide.FROM)) {
      exec(con, "create table moon.hello (id int primary key, name varchar(100))");

      DbStru struct = StruReader.read(createRowReader(con));

      Table table = struct.table("moon.hello");
      assertThat(table).isNotNull();
      assertThat(table.keyFields).hasSize(1);
      assertThat(table.keyFields.get(0).name.toLowerCase()).isEqualTo("id");
    }
  }

  @Test
  public void readAllTableColumns() throws Exception {

    DbWorker dbWorker = dbWorker();

    dbWorker.recreateDb(DbSide.FROM);

    try (Connection con = dbWorker.connection(DbSide.FROM)) {
      exec(con, "create table client (id int primary key, name varchar(100))");
      exec(con, "create table moon.hello (identifier int primary key, door varchar(100))");

      RowReader rowReader = createRowReader(con);

      //
      //
      List<ColumnRow> columnRowList = rowReader.readAllTableColumns();
      //
      //

      assertThat(columnRowList).hasSize(4);

      assertThat(columnRowList.get(0).tableName).isEqualTo("client");
      assertThat(columnRowList.get(1).tableName).isEqualTo("client");
      assertThat(columnRowList.get(2).tableName).isEqualTo("moon.hello");
      assertThat(columnRowList.get(3).tableName).isEqualTo("moon.hello");

      assertThat(columnRowList.get(0).name).isEqualTo("id");
      assertThat(columnRowList.get(1).name).isEqualTo("name");
      assertThat(columnRowList.get(2).name).isEqualTo("identifier");
      assertThat(columnRowList.get(3).name).isEqualTo("door");
    }
  }

  @Test
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
      exec(con, "create table moon.hello (" +
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

      assertThat(map).containsKey("client");
      assertThat(map).containsKey("moon.hello");

      PrimaryKeyRow client = map.get("client");
      assertThat(client.tableName).isEqualTo("client");
      assertThat(client.keyFieldNames).hasSize(2);
      assertThat(client.keyFieldNames.get(0)).isEqualTo("id1");
      assertThat(client.keyFieldNames.get(1)).isEqualTo("id2");

      PrimaryKeyRow hello = map.get("moon.hello");
      assertThat(hello.tableName).isEqualTo("moon.hello");
      assertThat(hello.keyFieldNames).hasSize(3);
      assertThat(hello.keyFieldNames.get(0)).isEqualTo("identifier1");
      assertThat(hello.keyFieldNames.get(1)).isEqualTo("identifier2");
      assertThat(hello.keyFieldNames.get(2)).isEqualTo("identifier3");
    }
  }

  protected Consumer<Map<String, ForeignKeyRow>> readAllForeignKeys_createTableClient(Connection con) {
    exec(con, "create table client (" +
      "  id1 int," +
      "  id2 int," +
      "  name varchar(100)," +
      "  primary key(id1, id2)" +
      ")");
    return map -> {};
  }

  protected Consumer<Map<String, ForeignKeyRow>> readAllForeignKeys_createTablePhone(Connection con) {
    exec(con, "create table phone (" +
      "  first_id  varchar(50)," +
      "  second_id bigint," +
      "  client_id1 int," +
      "  client_id2 int," +
      "  phone_number varchar(300)," +
      "  constraint k001 foreign key (client_id1, client_id2) references client(id1, id2)," +
      "  primary key(first_id, second_id)" +
      ")");
    return map -> {
      assertThat(map).containsKey("FKk001");
      ForeignKeyRow row = map.get("FKk001");
      assertThat(row.fromTable).isEqualTo("phone");
      assertThat(row.toTable).isEqualTo("client");
      assertThat(row.fromColumns).containsExactly("client_id1", "client_id2");
      assertThat(row.toColumns).containsExactly("id1", "id2");
    };
  }

  protected Consumer<Map<String, ForeignKeyRow>> readAllForeignKeys_createTablePhoneCallType(Connection con) {
    exec(con, "create table moon.phone_call_type (" +
      "  code varchar(150)," +
      "  phone_first_id varchar(50)," +
      "  phone_second_id bigint," +
      "  description varchar(100)," +
      "  constraint k002 foreign key (phone_first_id, phone_second_id) references phone(first_id, second_id)," +
      "  primary key(code)" +
      ")");
    return map -> {
      assertThat(map).containsKey("FKk002");
      ForeignKeyRow row = map.get("FKk002");
      assertThat(row.fromTable).isEqualTo("moon.phone_call_type");
      assertThat(row.toTable).isEqualTo("phone");
      assertThat(row.fromColumns).containsExactly("phone_first_id", "phone_second_id");
      assertThat(row.toColumns).containsExactly("first_id", "second_id");
    };
  }

  @Test
  public void readAllForeignKeys() throws Exception {
    DbWorker dbWorker = dbWorker();

    dbWorker.recreateDb(DbSide.FROM);

    try (Connection con = dbWorker.connection(DbSide.FROM)) {
      Consumer<Map<String, ForeignKeyRow>> c1 = readAllForeignKeys_createTableClient(con);
      Consumer<Map<String, ForeignKeyRow>> c2 = readAllForeignKeys_createTablePhone(con);
      Consumer<Map<String, ForeignKeyRow>> c3 = readAllForeignKeys_createTablePhoneCallType(con);

      RowReader rowReader = createRowReader(con);

      //
      //
      Map<String, ForeignKeyRow> map = rowReader.readAllForeignKeys();
      //
      //

      assertThat(map).isNotNull();

      c1.accept(map);
      c2.accept(map);
      c3.accept(map);
    }
  }

  protected Consumer<Map<String, SequenceRow>> readAllSequences_createSequenceClient(Connection con) {
    exec(con, "create sequence client");
    return map -> {
      assertThat(map).containsKey("client");
      SequenceRow row = map.get("client");
      assertThat(row.name).isEqualTo("client");
      assertThat(row.startFrom).isEqualTo(1);
    };
  }

  protected Consumer<Map<String, SequenceRow>> readAllSequences_createSequencePhone(Connection con) {
    exec(con, "create sequence moon.phone start 12000");
    return map -> {
      assertThat(map).containsKey("moon.phone");
      SequenceRow row = map.get("moon.phone");
      assertThat(row.name).isEqualTo("moon.phone");
      assertThat(row.startFrom).isEqualTo(12000);
    };
  }

  protected Consumer<Map<String, SequenceRow>> readAllSequences_createSequenceLeftHello(Connection con) {
    exec(con, "create sequence boom.hello");
    return map -> assertThat(map).doesNotContainKey("boom.hello");
  }

  @Test
  public void readAllSequences() throws Exception {
    DbWorker dbWorker = dbWorker();

    dbWorker.recreateDb(DbSide.FROM);

    try (Connection con = dbWorker.connection(DbSide.FROM)) {
      Consumer<Map<String, SequenceRow>> c1 = readAllSequences_createSequenceClient(con);
      Consumer<Map<String, SequenceRow>> c2 = readAllSequences_createSequencePhone(con);
      Consumer<Map<String, SequenceRow>> c3 = readAllSequences_createSequenceLeftHello(con);

      RowReader rowReader = createRowReader(con);

      //
      //
      Map<String, SequenceRow> map = rowReader.readAllSequences();
      //
      //

      assertThat(map).isNotNull();

      c1.accept(map);
      c2.accept(map);
      c3.accept(map);
    }
  }

  protected Consumer<Map<String, ViewRow>> readAllViews_createViewClient(Connection con) {
    exec(con, "create table client1 (id int primary key, code int, f1 int)");
    exec(con, "create table client2 (id int primary key, code int, f2 int)");
    exec(con, "create view client as select c1.id as id1, c2.id as id2, c1.code, c1.f1, c2.f2" +
      " from client1 c1, client2 c2 where c1.code = c2.code");
    return map -> {
      assertThat(map).containsKey("client");
      ViewRow row = map.get("client");
      assertThat(row.name).isEqualTo("client");
      assertThat(row.dependenses).containsAll(Arrays.asList("client1", "client2"));
      assertThat(row.content.replaceAll("\\s+", ""))
        .isEqualTo("SELECTc1.idASid1,c2.idASid2,c1.code,c1.f1,c2.f2FROMclient1c1,client2c2WHERE(c1.code=c2.code)");
    };
  }

  protected Consumer<Map<String, ViewRow>> readAllViews_createViewPhone(Connection con) {
    exec(con, "create view moon.phone as select 10 x, 20 y, 30 z");
    return map -> {
      assertThat(map).containsKey("moon.phone");
      ViewRow row = map.get("moon.phone");
      assertThat(row.name).isEqualTo("moon.phone");
    };
  }

  protected Consumer<Map<String, ViewRow>> readAllViews_createViewHello(Connection con) {
    exec(con, "create view boom.hello as select 11 x, 12 y, 13 z");
    return map -> assertThat(map).doesNotContainKey("boom.hello");
  }

  @Test
  public void readAllViews() throws Exception {
    DbWorker dbWorker = dbWorker();

    dbWorker.recreateDb(DbSide.FROM);

    try (Connection con = dbWorker.connection(DbSide.FROM)) {
      Consumer<Map<String, ViewRow>> c1 = readAllViews_createViewClient(con);
      Consumer<Map<String, ViewRow>> c2 = readAllViews_createViewPhone(con);
      Consumer<Map<String, ViewRow>> c3 = readAllViews_createViewHello(con);

      RowReader rowReader = createRowReader(con);

      //
      //
      Map<String, ViewRow> map = rowReader.readAllViews();
      //
      //

      assertThat(map).isNotNull();

      c1.accept(map);
      c2.accept(map);
      c3.accept(map);
    }
  }

  protected Consumer<Map<String, String>> readTableComments_createTableClient(Connection con) {
    exec(con, "create table client(id int)");
    exec(con, "comment on table client is 'Hello Client'");
    return map -> {
      assertThat(map).containsKey("client");
      assertThat(map.get("client")).isEqualTo("Hello Client");
    };
  }

  protected Consumer<Map<String, String>> readTableComments_createTablePhone(Connection con) {
    exec(con, "create table moon.phone(id int)");
    exec(con, "comment on table moon.phone is 'Hello Phone'");
    return map -> {
      assertThat(map).containsKey("moon.phone");
      assertThat(map.get("moon.phone")).isEqualTo("Hello Phone");
    };
  }

  protected Consumer<Map<String, String>> readTableComments_createTableHello(Connection con) {
    exec(con, "create table boom.hello(id int)");
    exec(con, "comment on table boom.hello is 'Hello'");
    return map -> assertThat(map).doesNotContainKey("boom.hello");
  }

  @Test
  public void readTableComments() throws Exception {
    DbWorker dbWorker = dbWorker();

    dbWorker.recreateDb(DbSide.FROM);

    try (Connection con = dbWorker.connection(DbSide.FROM)) {
      Consumer<Map<String, String>> c1 = readTableComments_createTableClient(con);
      Consumer<Map<String, String>> c2 = readTableComments_createTablePhone(con);
      Consumer<Map<String, String>> c3 = readTableComments_createTableHello(con);

      RowReader rowReader = createRowReader(con);

      //
      //
      Map<String, String> map = rowReader.readTableComments();
      //
      //

      assertThat(map).isNotNull();

      c1.accept(map);
      c2.accept(map);
      c3.accept(map);
    }
  }

  protected Consumer<Map<String, String>> readColumnComments_createTableClient(Connection con) {
    exec(con, "create table client (id int, name int)");
    exec(con, "comment on column client.id   is 'Hello client id'  ");
    exec(con, "comment on column client.name is 'Hello client name'");
    return map -> {
      assertThat(map).contains(MapEntry.entry("client.id", "Hello client id"));
      assertThat(map).contains(MapEntry.entry("client.name", "Hello client name"));
    };
  }

  protected Consumer<Map<String, String>> readColumnComments_createTablePhone(Connection con) {
    exec(con, "create table moon.phone (id int, name int)");
    exec(con, "comment on column moon.phone.id   is 'Hello phone id'  ");
    exec(con, "comment on column moon.phone.name is 'Hello phone name'");
    return map -> {
      assertThat(map).contains(MapEntry.entry("moon.phone.id", "Hello phone id"));
      assertThat(map).contains(MapEntry.entry("moon.phone.name", "Hello phone name"));
    };
  }

  protected Consumer<Map<String, String>> readColumnComments_createTableHello(Connection con) {
    exec(con, "create table boom.hello (id int, name int)");
    exec(con, "comment on column boom.hello.id   is 'Hello id'  ");
    exec(con, "comment on column boom.hello.name is 'Hello name'");
    return map -> {
      assertThat(map).doesNotContainKey("boom.hello.id");
      assertThat(map).doesNotContainKey("boom.hello.name");
    };
  }

  @Test
  public void readColumnComments() throws Exception {
    DbWorker dbWorker = dbWorker();

    dbWorker.recreateDb(DbSide.FROM);

    try (Connection con = dbWorker.connection(DbSide.FROM)) {
      Consumer<Map<String, String>> c1 = readColumnComments_createTableClient(con);
      Consumer<Map<String, String>> c2 = readColumnComments_createTablePhone(con);
      Consumer<Map<String, String>> c3 = readColumnComments_createTableHello(con);

      RowReader rowReader = createRowReader(con);

      //
      //
      Map<String, String> map = rowReader.readColumnComments();
      //
      //

      assertThat(map).isNotNull();

      c1.accept(map);
      c2.accept(map);
      c3.accept(map);
    }
  }
}
