package kz.greetgo.libase.struct_diff;

import kz.greetgo.libase.strureader.RowReader;
import kz.greetgo.libase.strureader.RowReaderOracle;
import kz.greetgo.libase.utils.DbWorker;
import kz.greetgo.libase.utils.DbWorkerOracle;

import java.sql.Connection;

import static kz.greetgo.libase.utils.TestUtil.exec;

public class DiffTableOracleTest extends DiffTablePostgresTest {
  @Override
  protected DbWorker dbWorker() {
    return new DbWorkerOracle();
  }

  @Override
  protected RowReader createRowReader(Connection con) {
    return new RowReaderOracle(con);
  }

  @Override
  protected void addField_createTableClientWith3Fields(Connection con) {
    exec(con, "create table client (id int primary key, field1 int, field2 int)");
  }

  @Override
  protected void addField_createTableClientWith2Fields(Connection con) {
    exec(con, "create table client (id int primary key, field1 int)");
  }

  @Override
  protected void modifyFieldTypeLength_createTableCharm(Connection con, int nameLength) {
    exec(con, "create table charm (id int primary key, name varchar2(" + nameLength + "))");
  }
}
