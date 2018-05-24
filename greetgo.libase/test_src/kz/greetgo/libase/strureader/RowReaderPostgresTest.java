package kz.greetgo.libase.strureader;

import kz.greetgo.libase.model.DbStru;
import kz.greetgo.libase.model.ForeignKey;
import kz.greetgo.libase.model.Table;
import kz.greetgo.libase.utils.DbSide;
import kz.greetgo.libase.utils.DbWorker;
import kz.greetgo.libase.utils.DbWorkerPostgres;
import org.testng.annotations.Test;

import java.sql.Connection;

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

}
