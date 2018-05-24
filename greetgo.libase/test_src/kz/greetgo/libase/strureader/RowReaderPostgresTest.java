package kz.greetgo.libase.strureader;

import kz.greetgo.libase.model.DbStru;
import kz.greetgo.libase.model.ForeignKey;
import kz.greetgo.libase.model.Table;
import kz.greetgo.libase.utils.DbSide;
import kz.greetgo.libase.utils.DbWorker;
import kz.greetgo.libase.utils.DbWorkerPostgres;
import org.testng.SkipException;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.util.List;

import static kz.greetgo.libase.utils.TestUtil.exec;
import static org.fest.assertions.api.Assertions.assertThat;

public class RowReaderPostgresTest {
  protected DbWorker dbWorker() {
    return new DbWorkerPostgres();
  }

  protected RowReader createRowReader(Connection con) {
    return new RowReaderPostgres(con);
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

  protected boolean isOracle() {
    return false;
  }

  @Test
  public void readTableMoonHello() throws Exception {
    if (isOracle()) throw new SkipException("Not supported schemas for Oracle");

    DbWorker dbWorker = dbWorker();

    dbWorker.recreateDb(DbSide.FROM);

    try (Connection con = dbWorker.connection(DbSide.FROM)) {
      exec(con, "create table moon.hello (id int primary key, name varchar(100))");

      DbStru struct = StruReader.read(createRowReader(con).addSchema("moon"));

      Table table = struct.table("moon.hello");
      assertThat(table).isNotNull();
      assertThat(table.keyFields).hasSize(1);
      assertThat(table.keyFields.get(0).name.toLowerCase()).isEqualTo("id");
    }
  }

  @Test
  public void readAllTableColumns() throws Exception {
    if (isOracle()) throw new SkipException("For Oracle not supported schemas");
    DbWorker dbWorker = dbWorker();

    dbWorker.recreateDb(DbSide.FROM);

    try (Connection con = dbWorker.connection(DbSide.FROM)) {
      exec(con, "create table client (id int primary key, name varchar(100))");
      exec(con, "create table moon.hello (identifier int primary key, door varchar(100))");

      RowReader rowReader = createRowReader(con).addSchema("moon");

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
}
