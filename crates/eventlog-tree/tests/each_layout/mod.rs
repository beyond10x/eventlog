//! Every case that creates a store runs once per layout a new store can be created in.
//!
//! A case is an `async fn name(layout: TreeLayout)`; `each_layout!` turns each into a module
//! `name` holding one test per layout, `name::nested` and `name::hashed`.

/// Run each named case once per [`eventlog_tree::TreeLayout`].
macro_rules! each_layout {
    (current_thread: $($name:ident),* $(,)?) => {
        $(
            mod $name {
                #[tokio::test]
                async fn nested() {
                    super::$name(eventlog_tree::TreeLayout::Nested).await;
                }

                #[tokio::test]
                async fn hashed() {
                    super::$name(eventlog_tree::TreeLayout::Hashed).await;
                }
            }
        )*
    };
    ($($name:ident),* $(,)?) => {
        $(
            mod $name {
                #[tokio::test(flavor = "multi_thread")]
                async fn nested() {
                    super::$name(eventlog_tree::TreeLayout::Nested).await;
                }

                #[tokio::test(flavor = "multi_thread")]
                async fn hashed() {
                    super::$name(eventlog_tree::TreeLayout::Hashed).await;
                }
            }
        )*
    };
}
