package kz.greetgo.libase.strureader;

import kz.greetgo.libase.utils.DbWorker;
import kz.greetgo.libase.utils.DbWorkerOracle;

import java.sql.Connection;

import static kz.greetgo.libase.utils.TestUtil.exec;

public class RowReaderOracleTest extends RowReaderPostgresTest {
  protected DbWorker dbWorker() {
    return new DbWorkerOracle();
  }

  protected RowReader createRowReader(Connection con) {
    return new RowReaderOracle(con);
  }

  protected String createTableClient(Connection con) {
    exec(con, "" +
      "create table CLIENT (" +
      "  id int primary key," +
      "  name varchar2(100)" +
      ")");
    return "CLIENT";
  }
}
