//! A generic mutex-synchronized intrusive doubly-linked list.
//!
//! This module provides [`IntrusiveListNode`], [`IntrusiveListGuard`], and the
//! [`IntrusiveNodeValue`] trait — the building blocks for intrusive linked lists
//! where each node carries its own discriminant value and the head node owns a
//! mutex that synchronizes all list operations.
//!
//! # Architecture
//!
//! Every list has exactly one *head* node and zero or more *leaf* nodes, all
//! sharing the [`IntrusiveListNode`] structure.  The [`IntrusiveNodeValue`]
//! trait, implemented by the discriminant stored in each node, defines how to
//! acquire the head mutex and how to navigate from a leaf back to its head.
//!
//! List mutations (link, unlink, filter) are performed through an
//! [`IntrusiveListGuard`], which holds both a reference to the head node and
//! the mutex guard, ensuring all operations are properly synchronized.
//!
//! # Safety
//!
//! The linked-list pointers are raw (`*const IntrusiveListNode<V>`) wrapped in
//! `UnsafeCell`.  All mutations go through the head's mutex.  Nodes must be
//! pinned before linking, because the list stores pointers to their addresses.

use core::{cell::UnsafeCell, marker::PhantomPinned};

// ---------------------------------------------------------------------------
// IntrusiveNodeValue — trait for the node-type discriminant
// ---------------------------------------------------------------------------

/// Trait that captures all functionality required from the node-type
/// discriminant in the intrusive list.
///
/// Each implementation defines how to acquire the head mutex and how to
/// navigate from a leaf back to its head node.
pub trait IntrusiveNodeValue: Sized {
    /// The value type stored in the head node mutex.
    type HeadValue;

    /// Locks the mutex on this head node.
    ///
    /// # Panics
    ///
    /// Panics if called on a leaf node.
    fn lock_mutex(&self) -> parking_lot::MutexGuard<'_, Self::HeadValue>;

    /// Returns the head [`IntrusiveListNode`] that this leaf targets, or
    /// `None` if this is itself a head node.
    fn target_node(&self) -> Option<&IntrusiveListNode<Self>>;
}

// ---------------------------------------------------------------------------
// UnsafeLink — a raw, nullable, interior-mutable pointer to a node
// ---------------------------------------------------------------------------

/// A nullable, interior-mutable pointer used for intrusive linked-list links.
///
/// All reads and writes go through `UnsafeCell`, so they are only safe when the
/// caller holds the head node's mutex (or during single-threaded construction).
struct UnsafeLink<T> {
    inner: UnsafeCell<*const T>,
}

impl<T> UnsafeLink<T> {
    /// Creates a new null link.
    fn new() -> Self {
        Self {
            inner: UnsafeCell::new(core::ptr::null()),
        }
    }

    /// Returns the raw pointer (may be null).
    #[inline(always)]
    fn get(&self) -> *const T {
        unsafe { *self.inner.get() }
    }

    /// Dereferences the pointer, returning `None` if null.
    ///
    /// # Safety
    ///
    /// The pointee must be alive and the caller must hold the head mutex.
    /// The returned reference lifetime is *not* tied to any guard.
    #[inline(always)]
    unsafe fn get_ref(&self) -> Option<&T> {
        let p = unsafe { *self.inner.get() };
        if p.is_null() {
            None
        } else {
            unsafe { Some(&*p) }
        }
    }

    /// Stores a raw pointer.
    #[inline(always)]
    fn set(&self, val: *const T) {
        unsafe { *self.inner.get() = val };
    }

    /// Stores a pointer derived from a reference.
    #[inline(always)]
    fn set_ref(&self, val: &T) {
        unsafe { *self.inner.get() = val as *const T };
    }

    /// Returns `true` if the stored pointer is null.
    #[inline(always)]
    fn is_null(&self) -> bool {
        unsafe { (*self.inner.get()).is_null() }
    }

    /// Sets the stored pointer to null.
    #[inline(always)]
    fn clear(&self) {
        unsafe {
            *self.inner.get() = core::ptr::null();
        }
    }
}

// ---------------------------------------------------------------------------
// IntrusiveListGuard — holds the head node reference and the mutex guard
// ---------------------------------------------------------------------------

/// A guard returned by [`IntrusiveListNode::lock_head`] that holds both a
/// reference to the head [`IntrusiveListNode`] and the
/// [`MutexGuard`](parking_lot::MutexGuard) protecting the head's value.
pub struct IntrusiveListGuard<'a, V: IntrusiveNodeValue> {
    head: &'a IntrusiveListNode<V>,
    guard: parking_lot::MutexGuard<'a, V::HeadValue>,
}

impl<V: IntrusiveNodeValue> core::ops::Deref for IntrusiveListGuard<'_, V> {
    type Target = V::HeadValue;
    #[inline(always)]
    fn deref(&self) -> &V::HeadValue {
        &self.guard
    }
}

impl<V: IntrusiveNodeValue> core::ops::DerefMut for IntrusiveListGuard<'_, V> {
    #[inline(always)]
    fn deref_mut(&mut self) -> &mut V::HeadValue {
        &mut self.guard
    }
}

impl<V: IntrusiveNodeValue> IntrusiveListGuard<'_, V> {
    /// Links a leaf node into this list, if it is not already linked.
    ///
    /// # Panics
    ///
    /// Panics if `node` does not target this guard's head node.
    pub fn link(&self, node: &IntrusiveListNode<V>) {
        unsafe {
            assert!(
                matches!(node.typ.target_node(), Some(h) if core::ptr::eq(h, self.head)),
                "node does not target this list's head"
            );
            if node.is_linked() {
                return;
            }
            let head = self.head;
            // Lazily initialise the head's self-link (circular sentinel) the
            // first time any node is linked.
            if !head.is_linked() {
                head.next.set_ref(head);
                head.prev.set_ref(head);
            }
            // Insert at the tail of the circular list (just before the head).
            let pn = head.prev.get_ref().unwrap();
            pn.next.set_ref(node);
            node.prev.set_ref(pn);
            node.next.set(head);
            head.prev.set_ref(node);
        }
    }

    /// Unlinks a node from this list.
    ///
    /// If `node` is the head node, the entire list is torn down (all leaves
    /// are unlinked first, then the head's self-link is cleared).  If `node`
    /// is a leaf that is not currently linked, this is a no-op.
    ///
    /// # Panics
    ///
    /// Panics if `node` is a leaf that does not target this guard's head node.
    pub fn unlink(&self, node: &IntrusiveListNode<V>) {
        unsafe {
            if core::ptr::eq(node, self.head) {
                // Tearing down the entire list: unlink every leaf until we loop
                // back to the head, then clear the head's self-link.
                while !node.next.is_null() && !core::ptr::eq(node.next.get(), self.head) {
                    self.unlink(&*node.next.get());
                }
            } else {
                assert!(
                    matches!(node.typ.target_node(), Some(h) if core::ptr::eq(h, self.head)),
                    "node does not target this list's head"
                );
                if !node.is_linked() {
                    return;
                }
                // Stitch prev and next together, bypassing this node.
                let pn = &*node.prev.get();
                let nn = &*node.next.get();
                pn.next.set_ref(nn);
                nn.prev.set_ref(pn);
            }
            node.prev.clear();
            node.next.clear();
        }
    }

    /// Walks every leaf node in the list, calling `f` on each one's
    /// [`IntrusiveNodeValue`].
    ///
    /// If `f` returns `true`, the node is kept in the list.  If `f` returns
    /// `false`, the node is unlinked.  The caller is responsible for any
    /// additional actions (e.g. waking) on unlinked nodes.
    pub fn filter<F>(&self, mut f: F)
    where
        F: FnMut(&V) -> bool,
    {
        unsafe {
            let head = self.head;
            if !head.is_linked() {
                return;
            }
            let mut pn = head.next.get();
            while !core::ptr::eq(pn, head) {
                let n = &*pn;
                pn = n.next.get();
                if !f(&n.typ) {
                    self.unlink(n);
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// IntrusiveListNode — intrusive doubly-linked list node
// ---------------------------------------------------------------------------

/// An intrusive doubly-linked list node, generic over [`IntrusiveNodeValue`].
///
/// Both head and leaf nodes share this structure so they can participate
/// in the same circular linked list.  The [`IntrusiveNodeValue`] discriminant
/// (`typ`) determines the node's role and provides mutex access and
/// head-pointer navigation.
pub struct IntrusiveListNode<V: IntrusiveNodeValue> {
    pub typ: V,
    prev: UnsafeLink<IntrusiveListNode<V>>,
    next: UnsafeLink<IntrusiveListNode<V>>,
    // Nodes must not be moved once linked, because the list stores raw pointers.
    _marker: PhantomPinned,
}

// SAFETY: All mutable state is behind a Mutex or only accessed while the Mutex
// is held. The raw pointers point to pinned, lifetime-guaranteed nodes.
unsafe impl<V: IntrusiveNodeValue> Send for IntrusiveListNode<V> {}
unsafe impl<V: IntrusiveNodeValue> Sync for IntrusiveListNode<V> {}

impl<V: IntrusiveNodeValue> IntrusiveListNode<V> {
    /// Creates a new node with the given [`IntrusiveNodeValue`] discriminant.
    pub fn new(typ: V) -> Self {
        Self {
            typ,
            prev: UnsafeLink::new(),
            next: UnsafeLink::new(),
            _marker: PhantomPinned,
        }
    }

    /// Acquires the head mutex and returns an [`IntrusiveListGuard`] holding
    /// both a reference to the head node and the mutex guard.
    ///
    /// For a head node, the head is `self`.  For a leaf node, the head is
    /// found by following the target pointer.
    pub fn lock_head(&self) -> IntrusiveListGuard<'_, V> {
        let head = self.typ.target_node().unwrap_or(self);
        IntrusiveListGuard {
            guard: head.typ.lock_mutex(),
            head,
        }
    }

    /// Returns `true` if this node is currently part of a linked list.
    ///
    /// A node is considered linked when both `prev` and `next` are non-null.
    ///
    /// # Safety
    ///
    /// Caller must hold the head mutex.
    pub unsafe fn is_linked(&self) -> bool {
        !self.prev.is_null() && !self.next.is_null()
    }
}

impl<V: IntrusiveNodeValue> Drop for IntrusiveListNode<V> {
    fn drop(&mut self) {
        // Acquire the head mutex and unlink ourselves so no dangling pointers
        // remain in the list. For a head node this tears down the entire list.
        let guard = self.lock_head();
        guard.unlink(self);
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use parking_lot::Mutex;

    // ── Test fixture ──────────────────────────────────────────────────────

    enum TestNode {
        Head {
            mutex: Mutex<i32>,
        },
        Leaf {
            target: *const IntrusiveListNode<TestNode>,
            id: i32,
        },
    }

    // SAFETY: The raw pointer in Leaf is only dereferenced while the head
    // mutex is held.
    unsafe impl Send for TestNode {}
    unsafe impl Sync for TestNode {}

    impl IntrusiveNodeValue for TestNode {
        type HeadValue = i32;

        fn lock_mutex(&self) -> parking_lot::MutexGuard<'_, i32> {
            match self {
                TestNode::Head { mutex } => mutex.lock(),
                TestNode::Leaf { .. } => panic!("lock_mutex called on leaf node"),
            }
        }

        fn target_node(&self) -> Option<&IntrusiveListNode<Self>> {
            match self {
                TestNode::Head { .. } => None,
                TestNode::Leaf { target, .. } => Some(unsafe { &**target }),
            }
        }
    }

    impl TestNode {
        fn id(&self) -> i32 {
            match self {
                TestNode::Leaf { id, .. } => *id,
                TestNode::Head { .. } => panic!("id() called on head node"),
            }
        }
    }

    fn make_head(val: i32) -> IntrusiveListNode<TestNode> {
        IntrusiveListNode::new(TestNode::Head {
            mutex: Mutex::new(val),
        })
    }

    fn make_leaf(head: &IntrusiveListNode<TestNode>, id: i32) -> IntrusiveListNode<TestNode> {
        IntrusiveListNode::new(TestNode::Leaf {
            target: head as *const _,
            id,
        })
    }

    /// Collects the IDs of all linked leaf nodes in list order.
    fn collect_ids(head: &IntrusiveListNode<TestNode>) -> Vec<i32> {
        let guard = head.lock_head();
        let mut ids = Vec::new();
        guard.filter(|v| {
            ids.push(v.id());
            true
        });
        ids
    }

    fn node_is_linked(
        head: &IntrusiveListNode<TestNode>,
        node: &IntrusiveListNode<TestNode>,
    ) -> bool {
        let _guard = head.lock_head();
        unsafe { node.is_linked() }
    }

    // ── Construction & basic state ────────────────────────────────────────

    #[test]
    fn new_head_is_not_linked() {
        let head = make_head(0);
        let guard = head.lock_head();
        assert!(!unsafe { head.is_linked() });
        drop(guard);
    }

    #[test]
    fn new_leaf_is_not_linked() {
        let head = make_head(0);
        let leaf = make_leaf(&head, 1);
        assert!(!node_is_linked(&head, &leaf));
    }

    #[test]
    fn lock_head_from_head_returns_guard_with_value() {
        let head = make_head(42);
        let guard = head.lock_head();
        assert_eq!(*guard, 42);
    }

    #[test]
    fn lock_head_from_leaf_returns_guard_with_head_value() {
        let head = make_head(42);
        let leaf = make_leaf(&head, 1);
        let guard = leaf.lock_head();
        assert_eq!(*guard, 42);
    }

    #[test]
    fn guard_deref_mut_writes_head_value() {
        let head = make_head(0);
        {
            let mut guard = head.lock_head();
            *guard = 123;
        }
        let guard = head.lock_head();
        assert_eq!(*guard, 123);
    }

    // ── Link operations ───────────────────────────────────────────────────

    #[test]
    fn link_single_leaf() {
        let head = make_head(0);
        let leaf = make_leaf(&head, 1);
        {
            let guard = head.lock_head();
            guard.link(&leaf);
        }
        assert!(node_is_linked(&head, &leaf));
        assert_eq!(collect_ids(&head), vec![1]);
    }

    #[test]
    fn link_two_leaves() {
        let head = make_head(0);
        let a = make_leaf(&head, 1);
        let b = make_leaf(&head, 2);
        {
            let guard = head.lock_head();
            guard.link(&a);
            guard.link(&b);
        }
        assert_eq!(collect_ids(&head), vec![1, 2]);
    }

    #[test]
    fn link_three_leaves_preserves_insertion_order() {
        let head = make_head(0);
        let a = make_leaf(&head, 10);
        let b = make_leaf(&head, 20);
        let c = make_leaf(&head, 30);
        {
            let guard = head.lock_head();
            guard.link(&a);
            guard.link(&b);
            guard.link(&c);
        }
        assert_eq!(collect_ids(&head), vec![10, 20, 30]);
    }

    #[test]
    fn link_already_linked_is_noop() {
        let head = make_head(0);
        let a = make_leaf(&head, 1);
        let b = make_leaf(&head, 2);
        {
            let guard = head.lock_head();
            guard.link(&a);
            guard.link(&b);
            guard.link(&a); // already linked — no-op
        }
        // Order unchanged, no duplicate.
        assert_eq!(collect_ids(&head), vec![1, 2]);
    }

    #[test]
    fn link_initializes_head_self_link() {
        let head = make_head(0);
        let leaf = make_leaf(&head, 1);
        let guard = head.lock_head();
        assert!(!unsafe { head.is_linked() });
        guard.link(&leaf);
        assert!(unsafe { head.is_linked() });
    }

    // ── Unlink operations ─────────────────────────────────────────────────

    #[test]
    fn unlink_single_leaf() {
        let head = make_head(0);
        let leaf = make_leaf(&head, 1);
        {
            let guard = head.lock_head();
            guard.link(&leaf);
            guard.unlink(&leaf);
        }
        assert!(!node_is_linked(&head, &leaf));
        assert_eq!(collect_ids(&head), vec![]);
    }

    #[test]
    fn unlink_first_of_two_leaves() {
        let head = make_head(0);
        let a = make_leaf(&head, 1);
        let b = make_leaf(&head, 2);
        {
            let guard = head.lock_head();
            guard.link(&a);
            guard.link(&b);
            guard.unlink(&a);
        }
        assert!(!node_is_linked(&head, &a));
        assert!(node_is_linked(&head, &b));
        assert_eq!(collect_ids(&head), vec![2]);
    }

    #[test]
    fn unlink_last_of_two_leaves() {
        let head = make_head(0);
        let a = make_leaf(&head, 1);
        let b = make_leaf(&head, 2);
        {
            let guard = head.lock_head();
            guard.link(&a);
            guard.link(&b);
            guard.unlink(&b);
        }
        assert!(node_is_linked(&head, &a));
        assert!(!node_is_linked(&head, &b));
        assert_eq!(collect_ids(&head), vec![1]);
    }

    #[test]
    fn unlink_middle_of_three_leaves() {
        let head = make_head(0);
        let a = make_leaf(&head, 1);
        let b = make_leaf(&head, 2);
        let c = make_leaf(&head, 3);
        {
            let guard = head.lock_head();
            guard.link(&a);
            guard.link(&b);
            guard.link(&c);
            guard.unlink(&b);
        }
        assert_eq!(collect_ids(&head), vec![1, 3]);
    }

    #[test]
    fn unlink_not_linked_is_noop() {
        let head = make_head(0);
        let a = make_leaf(&head, 1);
        let b = make_leaf(&head, 2);
        {
            let guard = head.lock_head();
            guard.link(&a);
            guard.unlink(&b); // b was never linked
        }
        assert_eq!(collect_ids(&head), vec![1]);
    }

    #[test]
    fn unlink_head_tears_down_entire_list() {
        let head = make_head(0);
        let a = make_leaf(&head, 1);
        let b = make_leaf(&head, 2);
        let c = make_leaf(&head, 3);
        {
            let guard = head.lock_head();
            guard.link(&a);
            guard.link(&b);
            guard.link(&c);
            guard.unlink(&head);
        }
        assert!(!node_is_linked(&head, &a));
        assert!(!node_is_linked(&head, &b));
        assert!(!node_is_linked(&head, &c));
        assert_eq!(collect_ids(&head), vec![]);
    }

    #[test]
    fn unlink_head_when_empty_is_safe() {
        let head = make_head(0);
        let guard = head.lock_head();
        guard.unlink(&head); // no-op: head was never self-linked
        drop(guard);
        assert_eq!(collect_ids(&head), vec![]);
    }

    #[test]
    fn relink_after_unlink() {
        let head = make_head(0);
        let leaf = make_leaf(&head, 1);
        {
            let guard = head.lock_head();
            guard.link(&leaf);
            guard.unlink(&leaf);
            guard.link(&leaf);
        }
        assert!(node_is_linked(&head, &leaf));
        assert_eq!(collect_ids(&head), vec![1]);
    }

    #[test]
    fn relink_goes_to_tail() {
        let head = make_head(0);
        let a = make_leaf(&head, 1);
        let b = make_leaf(&head, 2);
        {
            let guard = head.lock_head();
            guard.link(&a);
            guard.link(&b);
            guard.unlink(&a);
            guard.link(&a); // goes to tail
        }
        assert_eq!(collect_ids(&head), vec![2, 1]);
    }

    #[test]
    fn head_retains_self_link_after_last_leaf_unlinked() {
        // Once the head's circular sentinel is initialised by the first link,
        // it persists even after all leaves are removed. Only unlink(head)
        // clears it. This is by design — the sentinel is lazily initialised
        // and never eagerly torn down.
        let head = make_head(0);
        let leaf = make_leaf(&head, 1);
        let guard = head.lock_head();
        guard.link(&leaf);
        guard.unlink(&leaf);
        assert!(unsafe { head.is_linked() });
    }

    #[test]
    fn unlink_head_clears_self_link() {
        let head = make_head(0);
        let leaf = make_leaf(&head, 1);
        let guard = head.lock_head();
        guard.link(&leaf);
        guard.unlink(&head);
        assert!(!unsafe { head.is_linked() });
    }

    // ── Filter operations ─────────────────────────────────────────────────

    #[test]
    fn filter_empty_list_does_not_call_closure() {
        let head = make_head(0);
        let guard = head.lock_head();
        let mut called = false;
        guard.filter(|_| {
            called = true;
            true
        });
        assert!(!called);
    }

    #[test]
    fn filter_empty_self_linked_head_does_not_call_closure() {
        // Head was once linked (self-link persists) but has no leaves.
        let head = make_head(0);
        let leaf = make_leaf(&head, 1);
        {
            let guard = head.lock_head();
            guard.link(&leaf);
            guard.unlink(&leaf);
        }
        let guard = head.lock_head();
        let mut called = false;
        guard.filter(|_| {
            called = true;
            true
        });
        assert!(!called);
    }

    #[test]
    fn filter_keep_all() {
        let head = make_head(0);
        let a = make_leaf(&head, 1);
        let b = make_leaf(&head, 2);
        let c = make_leaf(&head, 3);
        {
            let guard = head.lock_head();
            guard.link(&a);
            guard.link(&b);
            guard.link(&c);
            guard.filter(|_| true);
        }
        assert_eq!(collect_ids(&head), vec![1, 2, 3]);
    }

    #[test]
    fn filter_remove_all() {
        let head = make_head(0);
        let a = make_leaf(&head, 1);
        let b = make_leaf(&head, 2);
        let c = make_leaf(&head, 3);
        {
            let guard = head.lock_head();
            guard.link(&a);
            guard.link(&b);
            guard.link(&c);
            guard.filter(|_| false);
        }
        assert!(!node_is_linked(&head, &a));
        assert!(!node_is_linked(&head, &b));
        assert!(!node_is_linked(&head, &c));
        assert_eq!(collect_ids(&head), vec![]);
    }

    #[test]
    fn filter_selective_keep_even() {
        let head = make_head(0);
        let a = make_leaf(&head, 1);
        let b = make_leaf(&head, 2);
        let c = make_leaf(&head, 3);
        let d = make_leaf(&head, 4);
        {
            let guard = head.lock_head();
            guard.link(&a);
            guard.link(&b);
            guard.link(&c);
            guard.link(&d);
            guard.filter(|v| v.id() % 2 == 0);
        }
        assert_eq!(collect_ids(&head), vec![2, 4]);
    }

    #[test]
    fn filter_removes_first_node() {
        let head = make_head(0);
        let a = make_leaf(&head, 1);
        let b = make_leaf(&head, 2);
        let c = make_leaf(&head, 3);
        {
            let guard = head.lock_head();
            guard.link(&a);
            guard.link(&b);
            guard.link(&c);
            guard.filter(|v| v.id() != 1);
        }
        assert_eq!(collect_ids(&head), vec![2, 3]);
    }

    #[test]
    fn filter_removes_last_node() {
        let head = make_head(0);
        let a = make_leaf(&head, 1);
        let b = make_leaf(&head, 2);
        let c = make_leaf(&head, 3);
        {
            let guard = head.lock_head();
            guard.link(&a);
            guard.link(&b);
            guard.link(&c);
            guard.filter(|v| v.id() != 3);
        }
        assert_eq!(collect_ids(&head), vec![1, 2]);
    }

    #[test]
    fn filter_single_node_keep() {
        let head = make_head(0);
        let a = make_leaf(&head, 1);
        {
            let guard = head.lock_head();
            guard.link(&a);
            guard.filter(|_| true);
        }
        assert_eq!(collect_ids(&head), vec![1]);
    }

    #[test]
    fn filter_single_node_remove() {
        let head = make_head(0);
        let a = make_leaf(&head, 1);
        {
            let guard = head.lock_head();
            guard.link(&a);
            guard.filter(|_| false);
        }
        assert!(!node_is_linked(&head, &a));
        assert_eq!(collect_ids(&head), vec![]);
    }

    #[test]
    fn filter_visits_nodes_in_insertion_order() {
        let head = make_head(0);
        let a = make_leaf(&head, 10);
        let b = make_leaf(&head, 20);
        let c = make_leaf(&head, 30);
        {
            let guard = head.lock_head();
            guard.link(&a);
            guard.link(&b);
            guard.link(&c);
        }
        let guard = head.lock_head();
        let mut visited = Vec::new();
        guard.filter(|v| {
            visited.push(v.id());
            true
        });
        assert_eq!(visited, vec![10, 20, 30]);
    }

    // ── Drop behavior ─────────────────────────────────────────────────────

    #[test]
    fn drop_linked_leaf_unlinks_itself() {
        let head = make_head(0);
        let a = make_leaf(&head, 1);
        let b = make_leaf(&head, 2);
        let c = make_leaf(&head, 3);
        {
            let guard = head.lock_head();
            guard.link(&a);
            guard.link(&b);
            guard.link(&c);
        }
        drop(b);
        assert_eq!(collect_ids(&head), vec![1, 3]);
    }

    #[test]
    fn drop_unlinked_leaf_is_safe() {
        let head = make_head(0);
        let leaf = make_leaf(&head, 1);
        drop(leaf);
        // No panic, no crash.
    }

    #[test]
    fn drop_head_after_all_leaves_dropped() {
        // Do not call drop(head) explicitly — that moves the node, breaking
        // self-referential pointers.  Let it drop naturally at end of scope.
        let head = make_head(0);
        {
            let a = make_leaf(&head, 1);
            let b = make_leaf(&head, 2);
            {
                let guard = head.lock_head();
                guard.link(&a);
                guard.link(&b);
            }
            // a and b are dropped here (reverse order: b, then a).
        }
        assert_eq!(collect_ids(&head), vec![]);
        // head drops here in place — safe because all leaves are gone.
    }

    // ── Panics ────────────────────────────────────────────────────────────

    #[test]
    #[should_panic(expected = "node does not target this list's head")]
    fn link_wrong_head_panics() {
        let head1 = make_head(0);
        let head2 = make_head(0);
        let leaf = make_leaf(&head1, 1);
        let guard = head2.lock_head();
        guard.link(&leaf);
    }

    #[test]
    #[should_panic(expected = "node does not target this list's head")]
    fn unlink_leaf_from_wrong_head_panics() {
        let head1 = make_head(0);
        let head2 = make_head(0);
        let leaf = make_leaf(&head1, 1);
        {
            let guard = head1.lock_head();
            guard.link(&leaf);
        }
        let guard = head2.lock_head();
        guard.unlink(&leaf);
    }

    // ── Combined operations ───────────────────────────────────────────────

    #[test]
    fn link_unlink_relink_multiple_cycles() {
        let head = make_head(0);
        let leaf = make_leaf(&head, 1);
        for _ in 0..10 {
            {
                let guard = head.lock_head();
                guard.link(&leaf);
            }
            assert!(node_is_linked(&head, &leaf));
            {
                let guard = head.lock_head();
                guard.unlink(&leaf);
            }
            assert!(!node_is_linked(&head, &leaf));
        }
    }

    #[test]
    fn filter_then_relink_removed_nodes() {
        let head = make_head(0);
        let a = make_leaf(&head, 1);
        let b = make_leaf(&head, 2);
        let c = make_leaf(&head, 3);
        {
            let guard = head.lock_head();
            guard.link(&a);
            guard.link(&b);
            guard.link(&c);
            guard.filter(|v| v.id() != 2);
        }
        assert_eq!(collect_ids(&head), vec![1, 3]);
        {
            let guard = head.lock_head();
            guard.link(&b); // goes to tail
        }
        assert_eq!(collect_ids(&head), vec![1, 3, 2]);
    }

    #[test]
    fn unlink_all_leaves_individually_in_mixed_order() {
        let head = make_head(0);
        let a = make_leaf(&head, 1);
        let b = make_leaf(&head, 2);
        let c = make_leaf(&head, 3);
        {
            let guard = head.lock_head();
            guard.link(&a);
            guard.link(&b);
            guard.link(&c);
        }
        {
            let guard = head.lock_head();
            guard.unlink(&b); // middle
        }
        assert_eq!(collect_ids(&head), vec![1, 3]);
        {
            let guard = head.lock_head();
            guard.unlink(&c); // tail
        }
        assert_eq!(collect_ids(&head), vec![1]);
        {
            let guard = head.lock_head();
            guard.unlink(&a); // last remaining
        }
        assert_eq!(collect_ids(&head), vec![]);
    }

    #[test]
    fn unlink_head_then_relink_leaves() {
        let head = make_head(0);
        let a = make_leaf(&head, 1);
        let b = make_leaf(&head, 2);
        {
            let guard = head.lock_head();
            guard.link(&a);
            guard.link(&b);
            guard.unlink(&head); // tear down
        }
        assert_eq!(collect_ids(&head), vec![]);
        {
            let guard = head.lock_head();
            guard.link(&b);
            guard.link(&a);
        }
        assert_eq!(collect_ids(&head), vec![2, 1]);
    }

    #[test]
    fn many_nodes_link_and_filter() {
        let head = make_head(0);
        let leaves: Vec<_> = (0..10).map(|i| make_leaf(&head, i)).collect();
        {
            let guard = head.lock_head();
            for leaf in &leaves {
                guard.link(leaf);
            }
        }
        assert_eq!(collect_ids(&head), (0..10).collect::<Vec<_>>());
        {
            let guard = head.lock_head();
            guard.filter(|v| v.id() % 2 == 0);
        }
        assert_eq!(collect_ids(&head), vec![0, 2, 4, 6, 8]);
    }

    #[test]
    fn filter_alternating_keep_remove() {
        let head = make_head(0);
        let leaves: Vec<_> = (0..6).map(|i| make_leaf(&head, i)).collect();
        {
            let guard = head.lock_head();
            for leaf in &leaves {
                guard.link(leaf);
            }
            // Remove 0, keep 1, remove 2, keep 3, remove 4, keep 5.
            guard.filter(|v| v.id() % 2 == 1);
        }
        assert_eq!(collect_ids(&head), vec![1, 3, 5]);
    }

    #[test]
    fn filter_counts_removed_nodes() {
        let head = make_head(0);
        let leaves: Vec<_> = (1..=5).map(|i| make_leaf(&head, i)).collect();
        {
            let guard = head.lock_head();
            for leaf in &leaves {
                guard.link(leaf);
            }
        }
        let guard = head.lock_head();
        let mut removed = 0;
        guard.filter(|v| {
            if v.id() > 3 {
                removed += 1;
                false
            } else {
                true
            }
        });
        assert_eq!(removed, 2);
        drop(guard);
        assert_eq!(collect_ids(&head), vec![1, 2, 3]);
    }

    #[test]
    fn guard_deref_mut_from_leaf_lock() {
        let head = make_head(0);
        let leaf = make_leaf(&head, 1);
        {
            let mut guard = leaf.lock_head();
            *guard = 77;
        }
        let guard = head.lock_head();
        assert_eq!(*guard, 77);
    }

    #[test]
    fn double_unlink_same_leaf_is_noop() {
        let head = make_head(0);
        let a = make_leaf(&head, 1);
        let b = make_leaf(&head, 2);
        {
            let guard = head.lock_head();
            guard.link(&a);
            guard.link(&b);
            guard.unlink(&a);
            guard.unlink(&a); // already unlinked — no-op
        }
        assert_eq!(collect_ids(&head), vec![2]);
    }

    #[test]
    fn unlink_head_twice_is_safe() {
        let head = make_head(0);
        let leaf = make_leaf(&head, 1);
        {
            let guard = head.lock_head();
            guard.link(&leaf);
            guard.unlink(&head);
            guard.unlink(&head); // already cleared — no-op
        }
        assert_eq!(collect_ids(&head), vec![]);
    }
}
