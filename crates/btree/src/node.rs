//! B+Tree node definitions.

use common::{PageId, RecordId};
use serde::{Deserialize, Serialize};
use types::Value;

/// The type of a B+Tree node.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeType {
    Internal,
    Leaf,
}

/// A B+Tree node, either internal or leaf.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum BTreeNode {
    /// Internal node with keys and child pointers.
    Internal {
        /// Separator keys (n keys for n+1 children).
        keys: Vec<Vec<Value>>,
        /// Child page IDs.
        children: Vec<PageId>,
    },
    /// Leaf node with key-value entries.
    Leaf {
        /// Key-value pairs stored in sorted order.
        entries: Vec<(Vec<Value>, RecordId)>,
        /// Pointer to the next leaf (for range scans).
        next_leaf: Option<PageId>,
    },
}

impl BTreeNode {
    /// Create a new empty leaf node.
    pub fn new_leaf() -> Self {
        Self::Leaf {
            entries: Vec::new(),
            next_leaf: None,
        }
    }

    /// Create a new internal node.
    pub fn new_internal(keys: Vec<Vec<Value>>, children: Vec<PageId>) -> Self {
        Self::Internal { keys, children }
    }

    /// Returns the node type.
    pub fn node_type(&self) -> NodeType {
        match self {
            Self::Internal { .. } => NodeType::Internal,
            Self::Leaf { .. } => NodeType::Leaf,
        }
    }

    /// Returns true if this is a leaf node.
    pub fn is_leaf(&self) -> bool {
        matches!(self, Self::Leaf { .. })
    }

    /// Returns the number of entries/keys in this node.
    pub fn len(&self) -> usize {
        match self {
            Self::Internal { keys, .. } => keys.len(),
            Self::Leaf { entries, .. } => entries.len(),
        }
    }

    /// Returns true if the node is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_leaf_is_empty() {
        let leaf = BTreeNode::new_leaf();
        assert!(leaf.is_leaf());
        assert!(leaf.is_empty());
        assert_eq!(leaf.len(), 0);
    }

    #[test]
    fn new_internal_has_correct_type() {
        let internal =
            BTreeNode::new_internal(vec![vec![Value::Int(5)]], vec![PageId(0), PageId(1)]);
        assert!(!internal.is_leaf());
        assert_eq!(internal.node_type(), NodeType::Internal);
        assert_eq!(internal.len(), 1);
    }

    #[test]
    fn leaf_with_entries() {
        let leaf = BTreeNode::Leaf {
            entries: vec![
                (
                    vec![Value::Int(1)],
                    RecordId {
                        page_id: PageId(0),
                        slot: 0,
                    },
                ),
                (
                    vec![Value::Int(2)],
                    RecordId {
                        page_id: PageId(0),
                        slot: 1,
                    },
                ),
            ],
            next_leaf: None,
        };
        assert!(leaf.is_leaf());
        assert_eq!(leaf.len(), 2);
        assert!(!leaf.is_empty());
    }
}
