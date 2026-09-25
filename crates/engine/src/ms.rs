//! A minimal reimplementation of `ms.c`'s `Ms` type: an array-backed
//! multiset with append-at-end insertion, swap-with-last removal, and a
//! round-robin `take()`. Order and `take()` semantics must match the
//! reference bit-for-bit because they determine list-tubes /
//! list-tubes-watched output order and which waiting connection gets the
//! next reserved job.

#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct Ms<T> {
    pub(crate) items: Vec<T>,
    /// Round-robin cursor of `take()` (`ms->last`). Any value is valid:
    /// `take()` reduces it modulo the length first.
    pub(crate) last: usize,
}

impl<T: PartialEq + Clone> Ms<T> {
    pub(crate) fn new() -> Self {
        Ms {
            items: Vec::new(),
            last: 0,
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.items.len()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    /// `ms_append`.
    pub(crate) fn append(&mut self, item: T) {
        self.items.push(item);
    }

    /// `ms_contains`.
    pub(crate) fn contains(&self, item: &T) -> bool {
        self.items.iter().any(|x| x == item)
    }

    /// `ms_remove`: linear search, then swap-with-last removal. Returns
    /// `true` if the item was found and removed.
    pub(crate) fn remove(&mut self, item: &T) -> bool {
        if let Some(i) = self.items.iter().position(|x| x == item) {
            self.items.swap_remove(i);
            true
        } else {
            false
        }
    }

    /// `ms_remove` when the caller already knows the item's index:
    /// swap-with-last removal of `items[i]`.
    pub(crate) fn remove_at(&mut self, i: usize) -> T {
        self.items.swap_remove(i)
    }

    /// `ms_take`: round-robin removal. NOT simple FIFO -- see ms.c's
    /// comment; with an even number of elements and several `take()` calls
    /// in a row (no intervening `append`), the order can deviate from pure
    /// arrival order. This must be mirrored exactly.
    pub(crate) fn take(&mut self) -> Option<T> {
        if self.items.is_empty() {
            return None;
        }
        self.last %= self.items.len();
        let item = self.items.swap_remove(self.last);
        self.last += 1;
        Some(item)
    }

    /// `ms_clear`: empties the set by deleting index 0 until empty (each
    /// deletion swaps the last item into slot 0). Returns the items in the
    /// order they were deleted, which is the order `onremove` fires in.
    pub(crate) fn clear_in_delete_order(&mut self) -> Vec<T> {
        let mut order = Vec::with_capacity(self.items.len());
        while !self.items.is_empty() {
            order.push(self.items.swap_remove(0));
        }
        self.last = 0;
        order
    }

    #[cfg(test)]
    pub(crate) fn clear(&mut self) {
        self.items.clear();
        self.last = 0;
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::Ms;

    #[test]
    fn append_preserves_order() {
        let mut m: Ms<i32> = Ms::new();
        m.append(1);
        m.append(2);
        m.append(3);
        assert_eq!(m.items, vec![1, 2, 3]);
    }

    #[test]
    fn remove_swaps_with_last() {
        let mut m: Ms<i32> = Ms::new();
        for x in [1, 2, 3, 4] {
            m.append(x);
        }
        assert!(m.remove(&2));
        // ms_delete(a,1): items[1] = items[--len] -> [1,4,3]
        assert_eq!(m.items, vec![1, 4, 3]);
        assert!(!m.remove(&99));
    }

    #[test]
    fn take_is_fifo_for_odd_or_single_drain_sequences() {
        let mut m: Ms<char> = Ms::new();
        for c in ['A', 'B', 'C'] {
            m.append(c);
        }
        assert_eq!(m.take(), Some('A'));
        assert_eq!(m.take(), Some('B'));
        assert_eq!(m.take(), Some('C'));
        assert_eq!(m.take(), None);
    }

    /// Documented exception in ms.c: an even number of elements, drained
    /// entirely without any intervening `append`, does NOT come out in
    /// pure arrival order. This must be mirrored exactly since it decides
    /// which waiting connection gets which reserved job.
    #[test]
    fn take_even_count_deviates_from_fifo() {
        let mut m: Ms<char> = Ms::new();
        for c in ['A', 'B', 'C', 'D'] {
            m.append(c);
        }
        let order: Vec<char> = std::iter::from_fn(|| m.take()).collect();
        assert_eq!(order, vec!['A', 'B', 'D', 'C']);
    }

    #[test]
    fn clear_in_delete_order_matches_ms_clear() {
        let mut m: Ms<char> = Ms::new();
        for c in ['A', 'B', 'C', 'D', 'E'] {
            m.append(c);
        }
        // delete(0): A out, E moves to 0 -> [E,B,C,D]; then E, D, C, B.
        assert_eq!(m.clear_in_delete_order(), vec!['A', 'E', 'D', 'C', 'B']);
        assert!(m.is_empty());
    }

    #[test]
    fn clear_resets_state() {
        let mut m: Ms<i32> = Ms::new();
        m.append(1);
        m.append(2);
        let _ = m.take();
        m.clear();
        assert_eq!(m.len(), 0);
        assert!(m.is_empty());
        m.append(5);
        assert_eq!(m.take(), Some(5));
    }
}
