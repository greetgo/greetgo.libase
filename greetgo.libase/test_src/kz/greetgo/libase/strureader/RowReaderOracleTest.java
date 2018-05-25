package kz.greetgo.libase.strureader;

import kz.greetgo.libase.model.DbStru;
import kz.greetgo.libase.model.Table;
import kz.greetgo.libase.utils.DbSide;
import kz.greetgo.libase.utils.DbWorker;
import kz.greetgo.libase.utils.DbWorkerOracle;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.util.List;
import java.util.Map;

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
}
