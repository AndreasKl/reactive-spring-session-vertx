package net.andreaskluth.session.core;

public class ReactivePostgresSessionRepositoryQueries {

  private ReactivePostgresSessionRepositoryQueries() {
    // Construction not permitted.
  }

  public static final String INSERT_SQL =
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

  public static final String UPDATE_SQL =
      "UPDATE session "
          + " SET "
          + "   session_id = $2,"
          + "   session_data = $3,"
          + "   last_accessed_time = $4,"
          + "   expiry_time = $5,"
          + "   max_inactive_interval = $6"
          + " WHERE id = $1;";

  public static final String REDUCED_UPDATE_SQL =
      "UPDATE session "
          + " SET "
          + "   session_id = $2,"
          + "   last_accessed_time = $3,"
          + "   expiry_time = $4,"
          + "   max_inactive_interval = $5"
          + " WHERE id = $1;";

  public static final String SELECT_SQL =
      "SELECT "
          + " id, session_id, session_data, creation_time,"
          + " last_accessed_time, max_inactive_interval "
          + "FROM session WHERE session_id = $1;";

  public static final String DELETE_FROM_SESSION_SQL = "DELETE FROM session WHERE session_id = $1;";

  public static final String DELETE_EXPIRED_SESSIONS_SQL =
      "DELETE FROM session WHERE expiry_time < $1 AND max_inactive_interval >= 0;";
}
