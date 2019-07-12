package net.andreaskluth.session.postgres;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.opentable.db.postgres.junit.EmbeddedPostgresRules;
import com.opentable.db.postgres.junit.PreparedDbRule;
import io.reactiverse.pgclient.PgClient;
import io.reactiverse.pgclient.PgException;
import io.reactiverse.pgclient.PgPool;
import io.reactiverse.pgclient.PgPoolOptions;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.sql.Connection;
import java.time.Clock;
import java.time.Instant;
import java.util.Objects;
import net.andreaskluth.session.postgres.ReactivePostgresSessionRepository.PostgresSession;
import net.andreaskluth.session.postgres.serializer.JdkSerializationStrategy;
import net.andreaskluth.session.postgres.serializer.SerializationException;
import net.andreaskluth.session.postgres.support.ReactivePostgresSessionSchemaPopulator;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.springframework.util.ReflectionUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class ReactivePostgresSessionRepositoryTest {

  private static final String KEY = "key";
  private static final String VALUE = "value";
  private PgPool pgPool = null;

  @ClassRule
  public static final PreparedDbRule embeddedPostgres =
      EmbeddedPostgresRules.preparedDatabase(
          ds -> {
            try (Connection connection = ds.getConnection()) {
              ReactivePostgresSessionSchemaPopulator.applyDefaultSchema(connection);
            }
          });

  @Before
  public void before() {
    pgPool = pool();
  }

  @After
  public void after() {
    pgPool.close();
  }

  @Test
  public void saveAndLoadWithAttributes() {
    var repo = sessionRepository();
    var session = repo.createSession().block();

    session.setAttribute(KEY, VALUE);

    var sessionId = session.getId();

    repo.save(session).block();

    var loadedSession = repo.findById(sessionId).block();

    assertThat(loadedSession.getId()).isEqualTo(sessionId);
    assertThat(loadedSession.<String>getAttribute(KEY)).isEqualTo(VALUE);
  }

  @Test
  public void duplicateSessionIdsAreNotPermitted() {
    var repo = sessionRepository();
    var session = repo.createSession().block();
    repo.save(session).block();

    var anotherSession = repo.createSession().block();

    setSessionId(anotherSession, session.getId());

    assertThatThrownBy(() -> repo.save(anotherSession).block()).isInstanceOf(PgException.class);
  }

  @Test
  public void saveAndLoadRemovingAttributes() {
    var repo = sessionRepository();

    var session = repo.createSession().block();
    session.setAttribute(KEY, VALUE);

    repo.save(session).block();

    var loadedSession = repo.findById(session.getId()).block();
    loadedSession.removeAttribute(KEY);

    repo.save(loadedSession).block();

    var reloadedSession = repo.findById(session.getId()).block();

    assertThat(reloadedSession.getId()).isEqualTo(session.getId());
    assertThat(loadedSession.<String>getAttribute(KEY)).isNull();
  }

  @Test
  public void findUnknownSessionIdShouldReturnNull() {
    var repo = sessionRepository();
    PostgresSession session = repo.findById("unknown").block();
    assertThat(session).isNull();
  }

  @Test
  public void deleteSessionById() {
    var repo = sessionRepository();
    var session = repo.createSession().block();
    repo.save(session).block();

    repo.deleteById(session.getId()).block();

    var loadedSession = repo.findById(session.getId()).block();
    assertThat(loadedSession).isNull();
  }

  @Test
  public void deleteSessionByIdWithUnknownSessionIdShouldNotCauseAnError() {
    var repo = sessionRepository();
    repo.deleteById("unknown").block();
  }

  @Test
  public void rotateSessionIdChangesSessionId() {
    var repo = sessionRepository();
    var session = repo.createSession().block();
    session.setAttribute(KEY, VALUE);

    var sessionId = session.getId();
    String changedSessionId = session.changeSessionId();

    repo.save(session).block();

    var loadedSession = repo.findById(changedSessionId).block();

    assertThat(sessionId).isNotEqualTo(changedSessionId);
    assertThat(loadedSession.getId()).isEqualTo(changedSessionId);
    assertThat(loadedSession.<String>getAttribute(KEY)).isEqualTo(VALUE);
  }

  @Test
  public void updatingValuesInSession() {
    var repo = sessionRepository();
    var session = repo.createSession().block();

    session.setAttribute(KEY, VALUE);

    repo.save(session).block();

    var reloadedSession = repo.findById(session.getId()).block();

    reloadedSession.setAttribute(KEY, "another value");

    repo.save(reloadedSession).block();

    assertThat(reloadedSession.<String>getAttribute(KEY)).isEqualTo("another value");
  }

  @Test
  public void updatingWithSameValueShouldChangeSession() {
    var repo = sessionRepository();
    var session = repo.createSession().block();

    session.setAttribute(KEY, VALUE);
    session.clearChangeFlags();
    session.setAttribute(KEY, VALUE);

    assertThat(session.isChanged()).isTrue();
  }

  @Test
  public void addingNullValueForNewKeyShouldChangeSession() {
    var repo = sessionRepository();
    var session = repo.createSession().block();

    session.clearChangeFlags();
    session.setAttribute(KEY, null);

    assertThat(session.isChanged()).isTrue();
  }

  @Test
  public void storeComplexObjectsInSession() {
    var repo = sessionRepository();
    var session = repo.createSession().block();

    session.setAttribute(KEY, new Complex(Instant.MAX));

    repo.save(session).block();

    var reloadedSession = repo.findById(session.getId()).block();

    assertThat(reloadedSession.<Complex>getAttribute(KEY)).isEqualTo(new Complex(Instant.MAX));
  }

  @Test
  public void objectsThatAreNotSerializableShouldRaise() {
    var repo = sessionRepository();
    var session = repo.createSession().block();

    session.setAttribute(KEY, new NotSerializable(Instant.MAX));

    Mono<Void> save = repo.save(session);
    assertThatThrownBy(save::block).isInstanceOf(SerializationException.class);
  }

  @Test
  public void savingMultipleTimes() {
    var repo = sessionRepository();
    var session = repo.createSession().block();

    session.setAttribute("keyA", "value A");
    Mono<Void> saveA = repo.save(session);
    saveA.block();

    session.setAttribute("keyB", "value B");
    Mono<Void> saveB = repo.save(session);
    saveB.block();

    var reloadedSession = repo.findById(session.getId()).block();
    assertThat(reloadedSession.<String>getAttribute("keyA")).isEqualTo("value A");
    assertThat(reloadedSession.<String>getAttribute("keyB")).isEqualTo("value B");
  }

  @Test
  public void savingInParallel() {
    var repo = sessionRepository();
    var session = repo.createSession().block();

    session.setAttribute("keyA", "value A");
    var saveA = repo.save(session);

    session.setAttribute("keyB", "value B");
    var saveB = repo.save(session);

    Flux.concat(saveA, saveB).blockLast();

    var reloadedSession = repo.findById(session.getId()).block();
    assertThat(reloadedSession.<String>getAttribute("keyA")).isEqualTo("value A");
    assertThat(reloadedSession.<String>getAttribute("keyB")).isEqualTo("value B");
  }

  @Test
  public void expiredSessionsCanNotBeRetrieved() {
    var repo = sessionRepository();
    repo.setDefaultMaxInactiveInterval(0);

    var session = repo.createSession().block();
    repo.save(session).block();
    assertThat(session.isExpired()).isTrue();

    var loadedSession = repo.findById(session.getId()).block();
    assertThat(loadedSession).isNull();
  }

  @Test
  public void expiredSessionsArePurgedByCleanup() {
    var repo = sessionRepository();
    repo.setDefaultMaxInactiveInterval(0);

    var session = repo.createSession().block();
    repo.save(session).block();

    Integer count = repo.cleanupExpiredSessions().block();

    assertThat(count).isGreaterThan(0);
  }

  private ReactivePostgresSessionRepository sessionRepository() {
    return new ReactivePostgresSessionRepository(
        pgPool, new JdkSerializationStrategy(), Clock.systemDefaultZone());
  }

  private PgPool pool() {
    PgPoolOptions options =
        new PgPoolOptions()
            .setPort(embeddedPostgres.getConnectionInfo().getPort())
            .setHost("localhost")
            .setDatabase("template1")
            .setUser("postgres")
            .setPassword("postgres")
            .setMaxSize(5);
    return PgClient.pool(options);
  }

  private void setSessionId(PostgresSession anotherSession, String sessionId) {
    Field sessionIdField = ReflectionUtils.findField(PostgresSession.class, "sessionId");
    ReflectionUtils.makeAccessible(sessionIdField);
    ReflectionUtils.setField(sessionIdField, anotherSession, sessionId);
  }

  private static class NotSerializable {

    private final Instant instant;

    private NotSerializable(Instant instant) {
      this.instant = instant;
    }
  }

  private static class Complex implements Serializable {

    private static final long serialVersionUID = 1;

    private final Instant instant;

    private Complex(Instant instant) {
      this.instant = instant;
    }

    public Instant getInstant() {
      return instant;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Complex complex = (Complex) o;
      return Objects.equals(instant, complex.instant);
    }

    @Override
    public int hashCode() {
      return Objects.hash(instant);
    }
  }
}
