package net.andreaskluth.session.postgres;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.opentable.db.postgres.junit5.EmbeddedPostgresExtension;
import com.opentable.db.postgres.junit5.PreparedDbExtension;
import io.vertx.core.buffer.Buffer;
import io.vertx.pgclient.PgException;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.Tuple;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.sql.Connection;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import net.andreaskluth.session.core.MonoToVertxHandlerAdapter;
import net.andreaskluth.session.core.ReactiveVertxSessionRepository;
import net.andreaskluth.session.core.ReactiveVertxSessionRepository.ReactiveSession;
import net.andreaskluth.session.core.serializer.DeserializationException;
import net.andreaskluth.session.core.serializer.JdkSerializationStrategy;
import net.andreaskluth.session.core.serializer.SerializationException;
import net.andreaskluth.session.core.support.ReactiveSessionSchemaPopulator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.springframework.util.ReflectionUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

class ReactivePostgresSessionRepositoryTest {

  private static final String KEY = "key";
  private static final String VALUE = "value";
  public static final String UPDATE_SESSION_DATA_QUERY =
      "UPDATE session SET session_data = $1 WHERE session_id = $2;";
  private Pool pool = null;

  @RegisterExtension
  static final PreparedDbExtension embeddedPostgres =
      EmbeddedPostgresExtension.preparedDatabase(
          ds -> {
            try (Connection connection = ds.getConnection()) {
              ReactiveSessionSchemaPopulator.applyDefaultSchema(connection);
            }
          });

  @BeforeEach
  void before() {
    pool = pool();
  }

  @AfterEach
  void after() {
    pool.close();
  }

  @Test
  void saveAndLoadWithAttributes() {
    ReactiveVertxSessionRepository repo = sessionRepository();
    ReactiveSession session = repo.createSession().block();

    session.setAttribute(KEY, VALUE);

    String sessionId = session.getId();

    repo.save(session).block();

    ReactiveSession loadedSession = repo.findById(sessionId).block();

    assertThat(loadedSession.getId()).isEqualTo(sessionId);
    assertThat(loadedSession.<String>getAttribute(KEY)).isEqualTo(VALUE);
  }

  @Test
  void duplicateSessionIdsAreNotPermitted() {
    ReactiveVertxSessionRepository repo = sessionRepository();
    ReactiveSession session = repo.createSession().block();
    repo.save(session).block();

    ReactiveSession anotherSession = repo.createSession().block();

    setSessionId(anotherSession, session.getId());

    assertThatThrownBy(() -> repo.save(anotherSession).block()).isInstanceOf(PgException.class);
  }

  @Test
  void saveAndLoadRemovingAttributes() {
    ReactiveVertxSessionRepository repo = sessionRepository();

    ReactiveSession session = repo.createSession().block();
    session.setAttribute(KEY, VALUE);

    repo.save(session).block();

    ReactiveSession loadedSession = repo.findById(session.getId()).block();
    loadedSession.removeAttribute(KEY);

    repo.save(loadedSession).block();

    ReactiveSession reloadedSession = repo.findById(session.getId()).block();

    assertThat(reloadedSession.getId()).isEqualTo(session.getId());
    assertThat(loadedSession.<String>getAttribute(KEY)).isNull();
  }

  @Test
  void findUnknownSessionIdShouldReturnNull() {
    ReactiveVertxSessionRepository repo = sessionRepository();
    ReactiveSession session = repo.findById("unknown").block();
    assertThat(session).isNull();
  }

  @Test
  void deleteSessionById() {
    ReactiveVertxSessionRepository repo = sessionRepository();
    ReactiveSession session = repo.createSession().block();
    repo.save(session).block();

    repo.deleteById(session.getId()).block();

    ReactiveSession loadedSession = repo.findById(session.getId()).block();
    assertThat(loadedSession).isNull();
  }

  @Test
  void deleteSessionByIdWithUnknownSessionIdShouldNotCauseAnError() {
    ReactiveVertxSessionRepository repo = sessionRepository();
    repo.deleteById("unknown").block();
  }

  @Test
  void rotateSessionIdChangesSessionId() {
    ReactiveVertxSessionRepository repo = sessionRepository();
    ReactiveSession session = repo.createSession().block();
    session.setAttribute(KEY, VALUE);

    String sessionId = session.getId();
    String changedSessionId = session.changeSessionId();

    repo.save(session).block();

    ReactiveSession loadedSession = repo.findById(changedSessionId).block();

    assertThat(sessionId).isNotEqualTo(changedSessionId);
    assertThat(loadedSession.getId()).isEqualTo(changedSessionId);
    assertThat(loadedSession.<String>getAttribute(KEY)).isEqualTo(VALUE);
  }

  @Test
  void updatingValuesInSession() {
    ReactiveVertxSessionRepository repo = sessionRepository();
    ReactiveSession session = repo.createSession().block();

    session.setAttribute(KEY, VALUE);

    repo.save(session).block();

    ReactiveSession reloadedSession = repo.findById(session.getId()).block();

    reloadedSession.setAttribute(KEY, "another value");

    repo.save(reloadedSession).block();

    assertThat(reloadedSession.<String>getAttribute(KEY)).isEqualTo("another value");
  }

  @Test
  void updatingWithSameValueShouldChangeSession() {
    ReactiveVertxSessionRepository repo = sessionRepository();
    ReactiveSession session = repo.createSession().block();

    session.setAttribute(KEY, VALUE);
    session.clearChangeFlags();
    session.setAttribute(KEY, VALUE);

    assertThat(session.isChanged()).isTrue();
  }

  @Test
  void addingNullValueForNewKeyShouldChangeSession() {
    ReactiveVertxSessionRepository repo = sessionRepository();
    ReactiveSession session = repo.createSession().block();

    session.clearChangeFlags();
    session.setAttribute(KEY, null);

    assertThat(session.isChanged()).isTrue();
  }

  @Test
  void storeComplexObjectsInSession() {
    ReactiveVertxSessionRepository repo = sessionRepository();
    ReactiveSession session = repo.createSession().block();

    session.setAttribute(KEY, new Complex(Instant.MAX));

    repo.save(session).block();

    ReactiveSession reloadedSession = repo.findById(session.getId()).block();

    assertThat(reloadedSession.<Complex>getAttribute(KEY)).isEqualTo(new Complex(Instant.MAX));
  }

  @Test
  void objectsThatAreNotSerializableShouldRaise() {
    ReactiveVertxSessionRepository repo = sessionRepository();
    ReactiveSession session = repo.createSession().block();

    session.setAttribute(KEY, new NotSerializable(Instant.MAX));

    Mono<Void> save = repo.save(session);
    assertThatThrownBy(save::block).isInstanceOf(SerializationException.class);
  }

  @Test
  void objectsThatAreNotDeserializableShouldRaise()
      throws ExecutionException, InterruptedException {
    ReactiveVertxSessionRepository repo = sessionRepository();

    ReactiveSession session = repo.createSession().block();
    repo.save(session).block();

    messWithSessionData(session);

    Mono<ReactiveSession> reloadedSession = repo.findById(session.getId());
    assertThatThrownBy(reloadedSession::block).isInstanceOf(DeserializationException.class);
  }

  @Test
  void objectsThatAreNotDeserializableShouldNotRaiseIfConfigured()
      throws ExecutionException, InterruptedException {
    ReactiveVertxSessionRepository repo = sessionRepository();
    repo.setInvalidateSessionOnDeserializationError(true);

    ReactiveSession session = repo.createSession().block();
    repo.save(session).block();

    messWithSessionData(session);

    ReactiveSession reloadedSession = repo.findById(session.getId()).block();
    assertThat(reloadedSession).isNull();
  }

  @Test
  void savingMultipleTimes() {
    ReactiveVertxSessionRepository repo = sessionRepository();
    ReactiveSession session = repo.createSession().block();

    session.setAttribute("keyA", "value A");
    Mono<Void> saveA = repo.save(session);
    saveA.block();

    session.setAttribute("keyB", "value B");
    Mono<Void> saveB = repo.save(session);
    saveB.block();

    ReactiveSession reloadedSession = repo.findById(session.getId()).block();
    assertThat(reloadedSession.<String>getAttribute("keyA")).isEqualTo("value A");
    assertThat(reloadedSession.<String>getAttribute("keyB")).isEqualTo("value B");
  }

  @Test
  void savingInParallel() {
    ReactiveVertxSessionRepository repo = sessionRepository();
    ReactiveSession session = repo.createSession().block();

    session.setAttribute("keyA", "value A");
    Mono<Void> saveA = repo.save(session);

    session.setAttribute("keyB", "value B");
    Mono<Void> saveB = repo.save(session);

    Flux.concat(saveA, saveB).blockLast();

    ReactiveSession reloadedSession = repo.findById(session.getId()).block();
    assertThat(reloadedSession.<String>getAttribute("keyA")).isEqualTo("value A");
    assertThat(reloadedSession.<String>getAttribute("keyB")).isEqualTo("value B");
  }

  @Test
  void expiredSessionsCanNotBeRetrieved() {
    ReactiveVertxSessionRepository repo = sessionRepository();
    repo.setDefaultMaxInactiveInterval(Duration.ZERO);

    ReactiveSession session = repo.createSession().block();
    repo.save(session).block();
    assertThat(session.isExpired()).isTrue();

    ReactiveSession loadedSession = repo.findById(session.getId()).block();
    assertThat(loadedSession).isNull();
  }

  @Test
  void expiredSessionsArePurgedByCleanup() {
    ReactiveVertxSessionRepository repo = sessionRepository();
    repo.setDefaultMaxInactiveInterval(Duration.ZERO);

    ReactiveSession session = repo.createSession().block();
    repo.save(session).block();

    Integer count = repo.cleanupExpiredSessions().block();

    assertThat(count).isGreaterThan(0);
  }

  private ReactiveVertxSessionRepository sessionRepository() {
    return new ReactiveVertxSessionRepository(
        pool,
        new ReactivePostgresSessionRepositoryQueries(),
        new JdkSerializationStrategy(),
        Clock.system(ZoneId.systemDefault()));
  }

  private Pool pool() {
    return TestPostgresOptions.pool(embeddedPostgres.getConnectionInfo().getPort());
  }

  private void setSessionId(ReactiveSession anotherSession, String sessionId) {
    Field sessionIdField = ReflectionUtils.findField(ReactiveSession.class, "sessionId");
    ReflectionUtils.makeAccessible(sessionIdField);
    ReflectionUtils.setField(sessionIdField, anotherSession, sessionId);
  }

  private void messWithSessionData(ReactiveSession session) {
    Tuple parameters = Tuple.of(Buffer.buffer(new byte[] {1, 2}), session.getId());
    Mono.<RowSet<Row>>create(
            sink ->
                pool.preparedQuery(UPDATE_SESSION_DATA_QUERY)
                    .execute(parameters, new MonoToVertxHandlerAdapter<>(sink)))
        .block();
  }

  @SuppressWarnings("UnusedVariable")
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
      if (!(o instanceof Complex)) {
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
