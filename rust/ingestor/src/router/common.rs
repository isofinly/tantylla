use scylla_cdc::consumer::OperationType;
use tantylla_common::indexer::index_operation::OpType;

/// One half (the start bound) of a CDC range-delete pair.
///
/// ScyllaDB CDC encodes a range delete as exactly two log rows sharing
/// the same `cdc$time`:
///   - Row 1: start bound  (op 5 = inclusive, 6 = exclusive)
///   - Row 2: end bound    (op 7 = inclusive, 8 = exclusive)
///
/// This struct is owned by the per-stream [`super::super::cdc::consumer::Consumer`]
/// so that each stream independently tracks its own in-flight range delete,
/// eliminating the cross-stream race that the old shared `Mutex` introduced.
#[derive(Debug)]
pub(crate) struct PendingRangeStart {
    /// Partition key values joined with ":" — identifies which node to query.
    pub(crate) partition_key: String,
    /// Target node address for this partition.
    pub(crate) target_node: String,
    /// Clustering key column values from the start-bound CDC row.
    /// `None` for a given column means that side is unbounded.
    pub(crate) ck_values: Vec<Option<String>>,
    /// Whether the start bound is inclusive (op 5) or exclusive (op 6).
    pub(crate) start_inclusive: bool,
    /// The writetime extracted from the CDC timeuuid.
    pub(crate) writetime: u64,
}

/// The resolved routing intent for a CDC row.
///
/// `pub(crate)` so that [`super::super::cdc::consumer::Consumer`] can
/// pattern-match on it and drive the dispatch itself (rather than delegating
/// everything through a shared `route()` method).
pub(crate) enum RoutingAction {
    Skip,
    RangeDeleteStart,
    RangeDeleteEnd,
    Forward { op_type: OpType },
}

impl RoutingAction {
    pub(crate) fn determine(row: &scylla_cdc::consumer::CDCRow<'_>) -> anyhow::Result<Self> {
        let action = match &row.operation {
            OperationType::PreImage => Self::Skip,

            OperationType::RowRangeDelInclLeft | OperationType::RowRangeDelExclLeft => {
                Self::RangeDeleteStart
            }

            OperationType::RowRangeDelInclRight | OperationType::RowRangeDelExclRight => {
                Self::RangeDeleteEnd
            }

            OperationType::RowInsert | OperationType::RowUpdate | OperationType::PostImage => {
                Self::Forward {
                    op_type: OpType::Upsert,
                }
            }

            OperationType::RowDelete => Self::Forward {
                op_type: OpType::Delete,
            },

            OperationType::PartitionDelete => Self::Forward {
                op_type: OpType::PartitionDelete,
            },
        };
        Ok(action)
    }
}
