package kz.greetgo.libase.utils;

public abstract class AbstractDbWorker implements DbWorker {

  protected static class StoredConnectParams implements ConnectParams {

    private final String db;
    private final String url;
    private final String username;
    private final String password;

    public StoredConnectParams(String db, String url, String username, String password) {
      this.db = db;
      this.url = url;
      this.username = username;
      this.password = password;
    }

    @Override
    public String db() {
      return db;
    }

    @Override
    public String url() {
      return url;
    }

    @Override
    public String username() {
      return username;
    }

    @Override
    public String password() {
      return password;
    }
  }
}
