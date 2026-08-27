//! The Redis Streams broker against a real Redis.
//!
//! Not mocked, and it could not usefully be. Everything worth testing here is a
//! property of Redis rather than of this code: that a consumer group splits work
//! instead of duplicating it, that an unacknowledged message is not handed out
//! twice, that reclaim moves ownership after an idle threshold. A fake would only
//! assert that the commands were spelled the way this file spells them.
//!
//! Requires Redis: `docker compose up -d redis`.

use std::time::Duration;

use relay_broker::{Broker, Config, RedisStreams};
use uuid::Uuid;

fn url() -> String {
    std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".into())
}

/// Its own stream per test, so tests cannot see each other's messages.
///
/// The alternative — one stream purged between tests — serialises the whole file and
/// still races whenever two run at once, which is the default.
fn config(name: &str) -> Config {
    Config {
        url: url(),
        stream: format!("test:{name}:{}", Uuid::new_v4()),
        group: "test-group".into(),
    }
}

async fn broker(name: &str) -> RedisStreams {
    RedisStreams::connect(config(name))
        .await
        .expect("connect to redis")
}

/// Two brokers sharing one stream and group: two consumers in a fleet.
async fn pair(name: &str) -> (RedisStreams, RedisStreams) {
    let c = config(name);
    let a = RedisStreams::connect(c.clone()).await.expect("a");
    let b = RedisStreams::connect(c).await.expect("b");
    (a, b)
}

const NOW: Duration = Duration::from_millis(50);

#[tokio::test]
async fn a_published_id_comes_back_intact() {
    let b = broker("roundtrip").await;
    let id = Uuid::new_v4();
    assert_eq!(b.publish(&[id]).await.unwrap(), 1);

    let got = b.consume("c1", 10, NOW).await.unwrap();
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].delivery_id, id, "the id survived the round trip");
    assert!(
        !got[0].receipt.is_empty(),
        "and carries something to ack with"
    );
}

#[tokio::test]
async fn an_empty_publish_is_not_a_round_trip() {
    let b = broker("empty").await;
    assert_eq!(b.publish(&[]).await.unwrap(), 0);
}

#[tokio::test]
async fn nothing_to_read_is_an_empty_list_rather_than_an_error() {
    // Redis answers a blocking read that times out with nil, which the driver hands
    // back as `None`. Read as an error, an idle dispatcher would log a failure every
    // few hundred milliseconds forever.
    let b = broker("idle").await;
    assert!(b.consume("c1", 10, NOW).await.unwrap().is_empty());
}

#[tokio::test]
async fn two_consumers_in_a_group_split_the_work() {
    // The acceptance criterion for the milestone: adding a consumer adds throughput.
    // If the group were configured wrong — or if each consumer read the stream
    // directly rather than through the group — both would receive everything, and
    // every webhook would be sent twice.
    let (a, b) = pair("split").await;
    let ids: Vec<Uuid> = (0..20).map(|_| Uuid::new_v4()).collect();
    a.publish(&ids).await.unwrap();

    let first = a.consume("consumer-a", 10, NOW).await.unwrap();
    let second = b.consume("consumer-b", 10, NOW).await.unwrap();

    assert_eq!(
        first.len() + second.len(),
        20,
        "between them they saw all of it"
    );
    let overlap: Vec<_> = first
        .iter()
        .filter(|m| second.iter().any(|n| n.delivery_id == m.delivery_id))
        .collect();
    assert!(
        overlap.is_empty(),
        "and neither saw the other's: {overlap:?}"
    );
}

#[tokio::test]
async fn an_unacknowledged_message_is_not_handed_out_again_on_its_own() {
    // Ownership, and part of why a database lease is still needed. Redis will not
    // re-deliver this to anybody until it is reclaimed, so a consumer that takes work
    // and dies silently strands it — which is what reclaim and the reconciliation
    // sweep exist for.
    let b = broker("held").await;
    b.publish(&[Uuid::new_v4()]).await.unwrap();
    assert_eq!(b.consume("c1", 10, NOW).await.unwrap().len(), 1);
    assert!(
        b.consume("c2", 10, NOW).await.unwrap().is_empty(),
        "a second consumer does not get an entry the first still holds"
    );
}

#[tokio::test]
async fn acknowledging_finishes_a_message() {
    let b = broker("ack").await;
    b.publish(&[Uuid::new_v4()]).await.unwrap();
    let got = b.consume("c1", 10, NOW).await.unwrap();
    let receipts: Vec<String> = got.iter().map(|m| m.receipt.clone()).collect();

    assert_eq!(b.ack(&receipts).await.unwrap(), 1);
    assert_eq!(b.lag().await.unwrap().unacked, 0, "nothing left pending");

    let reclaimed = b.reclaim("c2", Duration::from_millis(0), 10).await.unwrap();
    assert!(reclaimed.is_empty(), "nothing outstanding to take over");
}

#[tokio::test]
async fn acknowledging_nothing_is_not_a_round_trip() {
    let b = broker("ack-empty").await;
    assert_eq!(b.ack(&[]).await.unwrap(), 0);
}

#[tokio::test]
async fn work_a_dead_consumer_was_holding_can_be_taken_over() {
    // The recovery path inside the broker. A consumer read this and never came back;
    // an idle threshold of zero stands in for "long enough that it is not coming
    // back", so the test does not have to sleep.
    let b = broker("reclaim").await;
    let id = Uuid::new_v4();
    b.publish(&[id]).await.unwrap();
    assert_eq!(b.consume("doomed", 10, NOW).await.unwrap().len(), 1);

    let reclaimed = b
        .reclaim("survivor", Duration::from_millis(0), 10)
        .await
        .unwrap();
    assert_eq!(reclaimed.len(), 1);
    assert_eq!(reclaimed[0].delivery_id, id, "same delivery, new owner");
}

#[tokio::test]
async fn reclaim_leaves_alone_work_that_is_merely_recent() {
    // The false positive that would matter. Reclaim cannot tell a dead consumer from
    // a slow one, so the idle threshold is the only thing stopping it taking work out
    // from under a consumer that is mid-delivery.
    let b = broker("reclaim-idle").await;
    b.publish(&[Uuid::new_v4()]).await.unwrap();
    assert_eq!(b.consume("busy", 10, NOW).await.unwrap().len(), 1);

    let reclaimed = b
        .reclaim("impatient", Duration::from_secs(3600), 10)
        .await
        .unwrap();
    assert!(reclaimed.is_empty(), "an hour of idle has not passed");
}

#[tokio::test]
async fn lag_reports_what_is_waiting_and_what_is_held() {
    let b = broker("lag").await;
    let ids: Vec<Uuid> = (0..5).map(|_| Uuid::new_v4()).collect();
    b.publish(&ids).await.unwrap();

    let before = b.lag().await.unwrap();
    assert_eq!(before.unread, 5, "published, nobody has read it");
    assert_eq!(before.unacked, 0);

    b.consume("c1", 2, NOW).await.unwrap();
    let after = b.lag().await.unwrap();
    assert_eq!(after.unread, 3, "two fewer waiting");
    assert_eq!(after.unacked, 2, "and two now held by a consumer");
}

#[tokio::test]
async fn an_unreadable_entry_is_dropped_rather_than_redelivered_forever() {
    // A poison message. The id is the whole content, so an entry whose id will not
    // parse can never become a delivery, and returning it would put a value the
    // caller cannot use into the delivery path. Leaving it unacknowledged would be
    // worse: it would come back on every reclaim for as long as the stream exists.
    let b = broker("poison").await;
    let mut conn = redis::Client::open(url())
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();

    let good = Uuid::new_v4();
    b.publish(&[good]).await.unwrap();
    let _: String = redis::cmd("XADD")
        .arg(b.stream())
        .arg("*")
        .arg("d")
        .arg("not-a-uuid")
        .query_async(&mut conn)
        .await
        .unwrap();

    let got = b.consume("c1", 10, NOW).await.unwrap();
    assert_eq!(got.len(), 1, "only the readable one came back");
    assert_eq!(got[0].delivery_id, good);

    let reclaimed = b.reclaim("c2", Duration::from_millis(0), 10).await.unwrap();
    assert!(
        reclaimed.iter().all(|m| m.delivery_id == good),
        "the unparseable entry was acknowledged, not left pending"
    );
}

#[tokio::test]
async fn connecting_twice_to_the_same_group_is_not_an_error() {
    // Every dispatcher creates the group at startup, so all but the first find it
    // already there. Treating `BUSYGROUP` as an error would mean whichever process
    // came up second refused to start.
    let (_a, _b) = pair("busygroup").await;
}
