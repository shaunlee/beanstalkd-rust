//! A minimal reimplementation of `ms.c`'s `Ms` type: an array-backed
//! multiset with append-at-end insertion, swap-with-last removal, and a
//! round-robin `take()`. Order and `take()` semantics must match the
//! reference bit-for-bit because they determine list-tubes /
//! list-tubes-watched output order and which waiting connection gets the
//! next reserved job.

#[derive(Debug, Clone, Default)]
pub(crate) struct Ms<T> {
    pub(crate) items: Vec<T>,
    last: usize,
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
}
