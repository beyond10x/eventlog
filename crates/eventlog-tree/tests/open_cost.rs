//! How long a tree store at `EVENTLOG_TREE_BENCH` takes to open. Run by hand, in release:
//! `EVENTLOG_TREE_BENCH=<dir> cargo test --release -p eventlog-tree --test open_cost -- --ignored --nocapture`.

use std::time::Instant;

use eventlog_tree::TreeEventStore;

#[tokio::test]
#[ignore = "measures a store named by the environment"]
async fn opening_a_named_store_is_timed() {
    let Some(root) = std::env::var_os("EVENTLOG_TREE_BENCH") else {
        return;
    };
    for attempt in 1..=3 {
        let started = Instant::now();
        let store = TreeEventStore::open(&root).await.expect("the store opens");
        println!("open {attempt}: {:?}", started.elapsed());
        drop(store);
    }
}
