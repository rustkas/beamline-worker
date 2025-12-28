use std::process::Command;
use std::time::Duration;
use tokio::time::sleep;
use futures::StreamExt;

#[tokio::test]
#[ignore]
async fn resubscribe_after_nats_restart() {
    // Ensure NATS is up (CI starts it). Publish and receive a message.
    let url = std::env::var("NATS_URL").unwrap_or_else(|_| "nats://127.0.0.1:4222".to_string());
    let subject = "test.resub.v1";

    // Initial connect and subscribe
    let nc1 = async_nats::connect(&url).await.expect("connect nats");
    let mut sub1 = nc1.subscribe(subject).await.expect("subscribe");
    nc1.publish(subject.to_string(), "before".into()).await.expect("publish before");
    let msg = tokio::time::timeout(Duration::from_secs(3), sub1.next()).await.expect("wait msg");
    assert_eq!(String::from_utf8_lossy(&msg.unwrap().payload), "before");

    // Restart NATS via docker compose
    Command::new("docker")
        .args(["compose", "restart", "nats"])
        .status()
        .expect("restart nats");

    // Wait for NATS to come back
    sleep(Duration::from_secs(5)).await;

    // Reconnect and resubscribe
    let nc2 = async_nats::connect(&url).await.expect("reconnect nats");
    let mut sub2 = nc2.subscribe(subject).await.expect("resubscribe");
    nc2.publish(subject.to_string(), "after".into()).await.expect("publish after");
    let msg2 = tokio::time::timeout(Duration::from_secs(5), sub2.next()).await.expect("wait msg2");
    assert_eq!(String::from_utf8_lossy(&msg2.unwrap().payload), "after");
}
