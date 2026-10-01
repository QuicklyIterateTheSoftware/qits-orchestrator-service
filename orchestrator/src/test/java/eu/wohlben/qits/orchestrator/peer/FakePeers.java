package eu.wohlben.qits.orchestrator.peer;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Alternative;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * The peers, faked at the one seam a process talks through.
 *
 * <p><b>An {@code @Alternative} subclass rather than a stub server</b>: the whole of what this
 * repository does over the wire is {@link PeerClient#get} and {@link PeerClient#post}, so replacing
 * those two is replacing the network. It costs no port, no thread and no WireMock dependency, and
 * the urls in the assertions are the REAL ones — the inherited {@code url()} still resolves them
 * from the shipped target configuration, so a wrong path or a wrong peer fails here.
 *
 * <p>What it cannot prove is what {@code send} does with headers, a timeout or a 1 MiB body. That
 * is {@code PeerClientTest}'s, which drives the real client against a loopback server.
 */
@Alternative
@Priority(1)
@ApplicationScoped
public class FakePeers extends PeerClient {

  private static final ObjectMapper JSON = new ObjectMapper();

  /** One scripted answer: what the peer says to one method-and-path. */
  public record Scripted(Integer status, String body, String transportError) {

    public static Scripted ok(String body) {
      return new Scripted(200, body, null);
    }

    public static Scripted status(int status, String body) {
      return new Scripted(status, body, null);
    }

    /** A peer that cannot be reached at all — no status, a sentence instead. */
    public static Scripted unreachable(String message) {
      return new Scripted(null, null, message);
    }
  }

  private final Map<String, Scripted> script = new ConcurrentHashMap<>();

  /** Every call made, in order, so a test can assert what a body carried. */
  public final List<PeerCall> calls = new CopyOnWriteArrayList<>();

  /** Held before a scripted answer is returned, when a test wants a run to stay RUNNING. */
  private volatile CountDownLatch gate;

  public void reset() {
    script.clear();
    calls.clear();
    gate = null;
  }

  /** Scripts one answer. The key is the path, which is unique across the four peers here. */
  public void answer(String path, Scripted scripted) {
    script.put(path, scripted);
  }

  /** Blocks every call until {@link #release()}, so a test can catch a run mid-flight. */
  public CountDownLatch hold() {
    CountDownLatch latch = new CountDownLatch(1);
    gate = latch;
    return latch;
  }

  public void release() {
    CountDownLatch latch = gate;
    gate = null;
    if (latch != null) {
      latch.countDown();
    }
  }

  /** The bodies of every call to one path, in order. */
  public List<String> bodiesFor(String path) {
    List<String> bodies = new ArrayList<>();
    for (PeerCall call : calls) {
      if (call.url().endsWith(path)) {
        bodies.add(call.body());
      }
    }
    return bodies;
  }

  @Override
  public PeerExchange get(String target, String path) {
    return exchange(new PeerCall("GET", url(target, path), null), path);
  }

  @Override
  public PeerExchange post(String target, String path, String body) {
    return exchange(new PeerCall("POST", url(target, path), body), path);
  }

  /** Same seam, timeout ignored — a fake never blocks on the network it is standing in for. */
  @Override
  public PeerExchange post(String target, String path, String body, java.time.Duration timeout) {
    return post(target, path, body);
  }

  private PeerExchange exchange(PeerCall call, String path) {
    calls.add(call);
    CountDownLatch latch = gate;
    if (latch != null) {
      try {
        // Bounded: a test that forgets to release must fail on its own assertion rather than hang
        // the whole suite on a worker thread nothing will interrupt.
        latch.await(30, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
    Scripted scripted = script.get(path);
    if (scripted == null) {
      return new PeerExchange(
          call, new PeerAnswer(null, null, null, "no scripted answer for " + path));
    }
    if (scripted.transportError() != null) {
      return new PeerExchange(call, new PeerAnswer(null, null, null, scripted.transportError()));
    }
    return new PeerExchange(
        call,
        new PeerAnswer(scripted.status(), scripted.body(), parse(scripted.body()), null));
  }

  private static com.fasterxml.jackson.databind.JsonNode parse(String body) {
    if (body == null || body.isBlank()) {
      return null;
    }
    try {
      return JSON.readTree(body);
    } catch (Exception e) {
      return null;
    }
  }
}
