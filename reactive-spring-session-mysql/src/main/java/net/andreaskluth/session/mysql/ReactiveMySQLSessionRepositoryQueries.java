package net.andreaskluth.session.mysql;

import net.andreaskluth.session.core.ReactiveVertxSessionRepositoryQueries;

public class ReactiveMySQLSessionRepositoryQueries
    implements ReactiveVertxSessionRepositoryQueries {

  private static final String INSERT_SQL =
      "INSERT IGNORE INTO session "
          + " ("
          + "   id,"
          + "   session_id,"
          + "   session_data,"
          + "   creation_time,"
          + "   last_accessed_time,"
          + "   expiry_time,"
          + "   max_inactive_interval"
          + " ) "
          + " VALUES "
          + " ("
          + "   ?,"
          + "   ?,"
          + "   ?,"
          + "   ?,"
          + "   ?,"
          + "   ?,"
          + "   ?"
          + " );";

  private static final String UPDATE_SQL =
      "UPDATE session "
          + " SET "
          + "   session_id = ?,"
          + "   session_data = ?,"
          + "   last_accessed_time = ?,"
          + "   expiry_time = ?,"
          + "   max_inactive_interval = ?"
          + " WHERE id = ?;";

  private static final String REDUCED_UPDATE_SQL =
      "UPDATE session "
          + " SET "
          + "   session_id = ?,"
          + "   last_accessed_time = ?,"
          + "   expiry_time = ?,"
          + "   max_inactive_interval = ?"
          + " WHERE id = ?;";

  private static final String SELECT_SQL =
      "SELECT "
          + " id, session_id, session_data, creation_time,"
          + " last_accessed_time, max_inactive_interval "
          + "FROM session WHERE session_id = ?;";

  private static final String DELETE_FROM_SESSION_SQL = "DELETE FROM session WHERE session_id = ?;";

  private static final String DELETE_EXPIRED_SESSIONS_SQL =
      "DELETE FROM session WHERE expiry_time < ? AND max_inactive_interval >= 0;";

  @Override
  public String insertSql() {
    return INSERT_SQL;
  }

  @Override
  public String updateSql() {
    return UPDATE_SQL;
  }

  @Override
  public String reducedUpdateSql() {
    return REDUCED_UPDATE_SQL;
  }

  @Override
  public String selectSql() {
    return SELECT_SQL;
  }

  @Override
  public String deleteFromSessionSql() {
    return DELETE_FROM_SESSION_SQL;
  }

  @Override
  public String deleteExpiredSessionsSql() {
    return DELETE_EXPIRED_SESSIONS_SQL;
  }
}
