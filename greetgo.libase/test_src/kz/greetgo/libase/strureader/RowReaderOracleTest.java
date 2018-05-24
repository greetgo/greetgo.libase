package kz.greetgo.libase.strureader;

import kz.greetgo.libase.utils.DbSide;
import kz.greetgo.libase.utils.DbWorker;
import kz.greetgo.libase.utils.DbWorkerOracle;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.util.List;

import static kz.greetgo.libase.utils.TestUtil.exec;
import static org.fest.assertions.api.Assertions.assertThat;

public class RowReaderOracleTest extends RowReaderPostgresTest {

  @Override
  protected DbWorker dbWorker() {
    return new DbWorkerOracle();
  }

  @Override
  protected boolean isOracle() {
    return true;
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
}
