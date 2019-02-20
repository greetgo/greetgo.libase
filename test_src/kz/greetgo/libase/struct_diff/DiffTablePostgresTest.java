package kz.greetgo.libase.struct_diff;

import kz.greetgo.libase.changes.AddTableField;
import kz.greetgo.libase.changes.AlterField;
import kz.greetgo.libase.changes.Change;
import kz.greetgo.libase.changes.Comparer;
import kz.greetgo.libase.model.DbStru;
import kz.greetgo.libase.strureader.RowReader;
import kz.greetgo.libase.strureader.RowReaderPostgres;
import kz.greetgo.libase.strureader.StruReader;
import kz.greetgo.libase.utils.DbSide;
import kz.greetgo.libase.utils.DbWorker;
import kz.greetgo.libase.utils.DbWorkerPostgres;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.util.List;

import static kz.greetgo.libase.utils.TestUtil.exec;
import static org.fest.assertions.api.Assertions.assertThat;

public class DiffTablePostgresTest {
  protected DbWorker dbWorker() {
    return new DbWorkerPostgres();
  }

  protected RowReader createRowReader(Connection con) {
    return new RowReaderPostgres(con).addSchema("moon");
  }

  protected void addField_createTableClientWith3Fields(Connection con) {
    exec(con, "create table client (id int primary key, field1 int, field2 int)");
  }

  protected void addField_createTableClientWith2Fields(Connection con) {
    exec(con, "create table client (id int primary key, field1 int)");
  }

  protected void modifyFieldTypeLength_createTableCharm(Connection con, int nameLength) {
    exec(con, "create table charm (id int primary key, name varchar(" + nameLength + "))");
  }

  @Test
  public void addField() throws Exception {

    DbWorker dbWorker = dbWorker();

    dbWorker.recreateDb(DbSide.FROM);
    dbWorker.recreateDb(DbSide.TO);

    try (Connection conFrom = dbWorker.connection(DbSide.FROM);
         Connection conTo = dbWorker.connection(DbSide.TO)) {
      addField_createTableClientWith3Fields(conFrom);
      addField_createTableClientWith2Fields(conTo);

      DbStru structFrom = StruReader.read(createRowReader(conFrom));
      DbStru structTo = StruReader.read(createRowReader(conTo));

      List<Change> changes = Comparer.compare(structTo, structFrom);

      assertThat(changes).hasSize(1);
      assertThat(changes.get(0)).isInstanceOf(AddTableField.class);
      AddTableField addTableField = (AddTableField) changes.get(0);
      assertThat(addTableField.field.owner.name.toLowerCase()).isEqualTo("client");
      assertThat(addTableField.field.name.toLowerCase()).isEqualTo("field2");
    }
  }

  @Test
  public void modifyFieldTypeLength_decLen() throws Exception {

    DbWorker dbWorker = dbWorker();

    dbWorker.recreateDb(DbSide.FROM);
    dbWorker.recreateDb(DbSide.TO);

    try (Connection conFrom = dbWorker.connection(DbSide.FROM);
         Connection conTo = dbWorker.connection(DbSide.TO)) {
      modifyFieldTypeLength_createTableCharm(conFrom, 500);
      modifyFieldTypeLength_createTableCharm(conTo, 300);

      DbStru structFrom = StruReader.read(createRowReader(conFrom));
      DbStru structTo = StruReader.read(createRowReader(conTo));

      List<Change> changes = Comparer.compare(structTo, structFrom);

      assertThat(changes).hasSize(1);
      AlterField alterField = (AlterField) changes.get(0);
      assertThat(alterField.typeLenInc()).isEqualTo(-200);

      System.out.println(changes);
    }
  }

  @Test
  public void modifyFieldTypeLength_incLen() throws Exception {

    DbWorker dbWorker = dbWorker();

    dbWorker.recreateDb(DbSide.FROM);
    dbWorker.recreateDb(DbSide.TO);

    try (Connection conFrom = dbWorker.connection(DbSide.FROM);
         Connection conTo = dbWorker.connection(DbSide.TO)) {
      modifyFieldTypeLength_createTableCharm(conFrom, 200);
      modifyFieldTypeLength_createTableCharm(conTo, 350);

      DbStru structFrom = StruReader.read(createRowReader(conFrom));
      DbStru structTo = StruReader.read(createRowReader(conTo));

      List<Change> changes = Comparer.compare(structTo, structFrom);

      assertThat(changes).hasSize(1);
      AlterField alterField = (AlterField) changes.get(0);
      assertThat(alterField.typeLenInc()).isEqualTo(150);

      System.out.println(changes);
    }
  }

}
