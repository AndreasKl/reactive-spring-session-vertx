package net.andreaskluth.session.postgres;

import net.andreaskluth.session.core.ReactiveVertxSessionRepositoryQueries;

public class ReactivePostgresSessionRepositoryQueries implements
    ReactiveVertxSessionRepositoryQueries {

  private static final String INSERT_SQL =
      "INSERT INTO session "
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
          + "   $1,"
          + "   $2,"
          + "   $3,"
          + "   $4,"
          + "   $5,"
          + "   $6,"
          + "   $7"
          + " );";

  private static final String UPDATE_SQL =
      "UPDATE session "
          + " SET "
          + "   session_id = $1,"
          + "   session_data = $2,"
          + "   last_accessed_time = $3,"
          + "   expiry_time = $4,"
          + "   max_inactive_interval = $5"
          + " WHERE id = $6;";

  private static final String REDUCED_UPDATE_SQL =
      "UPDATE session "
          + " SET "
          + "   session_id = $1,"
          + "   last_accessed_time = $2,"
          + "   expiry_time = $3,"
          + "   max_inactive_interval = $4"
          + " WHERE id = $5;";

  private static final String SELECT_SQL =
      "SELECT "
          + " id, session_id, session_data, creation_time,"
          + " last_accessed_time, max_inactive_interval "
          + "FROM session WHERE session_id = $1;";

  private static final String DELETE_FROM_SESSION_SQL = "DELETE FROM session WHERE session_id = $1;";

  private static final String DELETE_EXPIRED_SESSIONS_SQL =
      "DELETE FROM session WHERE expiry_time < $1 AND max_inactive_interval >= 0;";

  @Override public String insertSql() {
    return INSERT_SQL;
  }

  @Override public String updateSql() {
    return UPDATE_SQL;
  }

  @Override public String reducedUpdateSql() {
    return REDUCED_UPDATE_SQL;
  }

  @Override public String selectSql() {
    return SELECT_SQL;
  }

  @Override public String deleteFromSessionSql() {
    return DELETE_FROM_SESSION_SQL;
  }

  @Override public String deleteExpiredSessionsSql() {
    return DELETE_EXPIRED_SESSIONS_SQL;
  }
}
