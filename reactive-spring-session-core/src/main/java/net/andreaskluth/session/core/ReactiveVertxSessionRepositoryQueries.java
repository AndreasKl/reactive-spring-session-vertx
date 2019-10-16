package net.andreaskluth.session.core;

public interface ReactiveVertxSessionRepositoryQueries {

  String insertSql();

  String updateSql();

  String reducedUpdateSql();

  String selectSql();

  String deleteFromSessionSql();

  String deleteExpiredSessionsSql();
}
