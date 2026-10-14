//! `Node::subscribe_raw_counting`: counts messages and serialized bytes without
//! copying them out, records the rmw timestamps, and removes its subscription
//! once every handle is dropped (at its next message, as subscribe_raw does).
use r2r::QosProfile;
use std::time::{Duration, Instant};

fn spin_until(
    node: &mut r2r::Node, timeout: Duration, mut done: impl FnMut(&mut r2r::Node) -> bool,
) -> bool {
    let start = Instant::now();
    while start.elapsed() < timeout {
        node.spin_once(Duration::from_millis(20));
        if done(node) {
            return true;
        }
    }
    false
}

#[test]
fn counts_messages_and_bytes_without_deserializing() -> Result<(), Box<dyn std::error::Error>> {
    let ctx = r2r::Context::create()?;
    let mut node = r2r::Node::create(ctx, "raw_counting_test", "")?;
    let counter = node.subscribe_raw_counting(
        "/raw_counting/int_array",
        "std_msgs/msg/Int32MultiArray",
        QosProfile::default(),
    )?;
    let clone = counter.clone();
    let publisher = node.create_publisher::<r2r::std_msgs::msg::Int32MultiArray>(
        "/raw_counting/int_array",
        QosProfile::default(),
    )?;
    assert!(spin_until(&mut node, Duration::from_secs(10), |_| {
        publisher
            .get_inter_process_subscription_count()
            .unwrap_or(0)
            > 0
    }));
    assert_eq!(counter.snapshot(), r2r::RawCounterSnapshot::default());

    // 1000 int32 = 4000 bytes of payload: a variable-sized message.
    let msg = r2r::std_msgs::msg::Int32MultiArray {
        layout: r2r::std_msgs::msg::MultiArrayLayout::default(),
        data: (0..1000).collect(),
    };
    let one = r2r::WrappedTypesupport::to_serialized_bytes(&msg)?.len() as u64;
    for _ in 0..10 {
        publisher.publish(&msg)?;
        // KEEP_LAST 10 (default QoS) holds them all; spin between publishes anyway.
        node.spin_once(Duration::from_millis(10));
    }
    assert!(spin_until(&mut node, Duration::from_secs(10), |_| counter.snapshot().count == 10));
    let s = clone.snapshot();
    assert_eq!(s.count, 10);
    assert_eq!(s.bytes, 10 * one);
    assert!(s.last_source_timestamp_ns > 0);
    assert!(s.last_received_timestamp_ns > 0);

    // Drop every handle. Like subscribe_raw, the subscription notices at its
    // next message and is removed then.
    drop(counter);
    drop(clone);
    publisher.publish(&msg)?;
    assert!(spin_until(&mut node, Duration::from_secs(10), |_| {
        publisher
            .get_inter_process_subscription_count()
            .unwrap_or(1)
            == 0
    }));
    Ok(())
}

#[test]
fn unknown_type_is_an_error() {
    let ctx = r2r::Context::create().unwrap();
    let mut node = r2r::Node::create(ctx, "raw_counting_bad_type", "").unwrap();
    assert!(node
        .subscribe_raw_counting("/x", "no_such_pkg/msg/Nope", QosProfile::default())
        .is_err());
}
