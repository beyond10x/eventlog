//! Adversarial cases against the `eventlog-tree/3` layout choice (`TreeLayout::Hashed`): two
//! handles creating one store at once, and the path bound the hashed layout documents.

use eventlog_conformance::{event, meta};
use eventlog_core::{EventStore, Expected, StreamId, TenantId};
use eventlog_tree::{TreeEventStore, TreeLayout};
use serde_json::json;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Barrier};

fn stream(tenant: &str, id: &str) -> StreamId {
    StreamId::new(TenantId::new(tenant).unwrap(), "item", id).unwrap()
}

fn files(root: &Path) -> Vec<PathBuf> {
    let mut found = Vec::new();
    let mut stack = vec![root.to_owned()];
    while let Some(directory) = stack.pop() {
        for entry in fs::read_dir(&directory).unwrap() {
            let path = entry.unwrap().path();
            let relative = path.strip_prefix(root).unwrap().to_owned();
            if relative == Path::new(".lock") || relative == Path::new(".cache") {
                continue;
            }
            if path.is_dir() {
                stack.push(path);
            } else {
                found.push(relative);
            }
        }
    }
    found.sort();
    found
}

/// Two handles create one store at the same moment, one asking for each layout. `prepare` runs
/// before the writer lock, so creating `store.json` must be first-writer-wins: both handles open,
/// both append under the one layout that was published, and the store opens again and serves
/// every append. A publish that replaced the first `store.json` would leave the first handle's
/// events in a layout the store no longer declares.
#[test]
fn two_handles_creating_one_store_in_different_layouts_both_open_and_agree() {
    const ROUNDS: usize = 300;
    for round in 0..ROUNDS {
        let directory = tempfile::tempdir().unwrap();
        let root = Arc::new(directory.path().join("store"));
        let barrier = Arc::new(Barrier::new(2));
        let handles: Vec<_> = [(TreeLayout::Nested, "n"), (TreeLayout::Hashed, "h")]
            .into_iter()
            .map(|(layout, id)| {
                let root = Arc::clone(&root);
                let barrier = Arc::clone(&barrier);
                std::thread::spawn(move || {
                    let runtime = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .unwrap();
                    runtime.block_on(async move {
                        barrier.wait();
                        let store = TreeEventStore::open_in(root.as_path(), layout)
                            .await
                            .map_err(|error| format!("open: {error}"))?;
                        store
                            .append(
                                &stream("tenant-a", id),
                                Expected::NoStream,
                                &[event("item.received", 1)],
                                &meta(id, &json!({})),
                            )
                            .await
                            .map_err(|error| format!("append: {error}"))?;
                        store
                            .layout()
                            .map(|layout| (id, layout))
                            .map_err(|error| format!("layout: {error}"))
                    })
                })
            })
            .collect();
        let outcomes: Vec<_> = handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .collect();
        let appended: Vec<(&str, TreeLayout)> = outcomes
            .into_iter()
            .map(|outcome| {
                outcome.unwrap_or_else(|error| {
                    panic!("round {round}: a concurrent creation of one store failed: {error}")
                })
            })
            .collect();
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let reopened = match TreeEventStore::open(root.as_path()).await {
                Ok(store) => store,
                Err(error) => panic!(
                    "round {round}: two concurrent creations in different layouts left a store \
                     that no longer opens: {error}\nfiles: {:#?}\nstore.json: {}",
                    files(&root),
                    fs::read_to_string(root.join("store.json")).unwrap_or_default()
                ),
            };
            let published = reopened.layout().unwrap();
            for (id, layout) in &appended {
                assert_eq!(
                    *layout, published,
                    "round {round}: handle {id} saw a layout the store no longer declares"
                );
                assert_eq!(
                    reopened
                        .stream_version(&stream("tenant-a", id))
                        .await
                        .unwrap(),
                    Some(1),
                    "round {round}: an acknowledged append in stream {id} is not served"
                );
            }
        });
        assert!(
            files(&root)
                .iter()
                .all(|path| !path.to_string_lossy().contains(".tmp")),
            "round {round}: a staging file was left behind: {:#?}",
            files(&root)
        );
    }
}

/// Two plain `open`s of one new directory at the same moment, in the one layout every release
/// creates: both must open. Each stages its own `store.json`, so neither finds its staging file
/// taken by the other.
#[test]
fn two_handles_creating_one_nested_store_at_once_both_open() {
    for round in 0..50 {
        let directory = tempfile::tempdir().unwrap();
        let root = Arc::new(directory.path().join("store"));
        let barrier = Arc::new(Barrier::new(2));
        let handles: Vec<_> = (0..2)
            .map(|_| {
                let root = Arc::clone(&root);
                let barrier = Arc::clone(&barrier);
                std::thread::spawn(move || {
                    let runtime = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .unwrap();
                    runtime.block_on(async move {
                        barrier.wait();
                        TreeEventStore::open(root.as_path())
                            .await
                            .map(drop)
                            .map_err(|error| error.to_string())
                    })
                })
            })
            .collect();
        for handle in handles {
            if let Err(error) = handle.join().unwrap() {
                panic!("round {round}: a concurrent open of a new store failed: {error}");
            }
        }
    }
}

/// The bound `TreeLayout::Hashed` documents: the hashed layout bounds the stream part of a path,
/// not the tenant, so the Windows budget holds for a tenant whose escaped segment is at most 36
/// bytes. At exactly that tenant, made of bytes that escaping triples, every file a planning
/// store writes stays under `MAX_PATH` after the longest checkout prefix the design budgets for,
/// and the longest path is one character short of it: one more escaped byte reaches `MAX_PATH`.
#[tokio::test(flavor = "multi_thread")]
async fn a_hashed_store_holds_the_documented_bound_at_the_longest_admitted_tenant() {
    /// The longest consumer checkout prefix the design budgets for.
    const CHECKOUT_PREFIX: usize = 82;
    /// Git without `core.longpaths` refuses a path of this many characters or more.
    const MAX_PATH: usize = 260;
    /// The longest escaped tenant segment the doc of `TreeLayout::Hashed` admits.
    const TENANT_BUDGET: usize = 36;
    let directory = tempfile::tempdir().unwrap();
    let relative_root = Path::new(".engineering/state");
    let root = directory.path().join(relative_root);
    // `/` escapes to `%2F`: twelve of them are the 36 escaped bytes the doc admits.
    let tenant = "/".repeat(TENANT_BUDGET / 3);
    let store = TreeEventStore::open_in(&root, TreeLayout::Hashed)
        .await
        .unwrap();
    let long_id = "/".repeat(200);
    store
        .append(
            &stream(&tenant, &long_id),
            Expected::NoStream,
            &[event("item.received", 1)],
            &meta("one", &json!({})),
        )
        .await
        .unwrap();
    store
        .put_blob(
            &TenantId::new(&tenant).unwrap(),
            &format!("sha256:{}", "f".repeat(64)),
            b"bytes",
        )
        .await
        .unwrap();
    drop(store);
    let written = files(&root);
    let longest = written
        .iter()
        .map(|path| relative_root.join(path).to_string_lossy().chars().count())
        .max()
        .unwrap();
    assert!(
        CHECKOUT_PREFIX + longest < MAX_PATH,
        "a hashed store with a {TENANT_BUDGET}-byte escaped tenant writes a {longest}-character \
         path: {} after the checkout prefix\nfiles: {written:#?}",
        CHECKOUT_PREFIX + longest
    );
    assert_eq!(
        CHECKOUT_PREFIX + longest,
        MAX_PATH - 1,
        "the documented tenant bound is no longer the largest that fits: update the doc of \
         TreeLayout::Hashed and the crate docs"
    );
}
