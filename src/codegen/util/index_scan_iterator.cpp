//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// index_scan_iterator.cpp
//
// Identification: src/codegen/util/index_scan_iterator.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/util/index_scan_iterator.h"
#include "common/logger.h"
#include "executor/executor_context.h"
#include "type/value_factory.h"
#include "storage/tuple.h"
#include <string.h>

namespace peloton {
namespace codegen {
namespace util {

/* TODO, fix to take in the index scan information from the executor context
 * and initialize the index scan using it. The current approach is
 * not thread safe or general.
 */
  
IndexScanIterator::IndexScanIterator(index::Index *index,
                                     executor::ExecutorContext *executor_context) {
  index_ = index;
  planner::AbstractPlan *plan_root = executor_context->GetPlan();
  const planner::IndexScanPlan *plan = FindIndexPlan(plan_root);
  PELOTON_ASSERT(plan);

  const index::ConjunctionScanPredicate *csp = 
    &(plan->GetIndexPredicate().GetConjunctionList()[0]);

  if (csp->IsPointQuery()) {
    is_point_query_ = true;
    is_full_scan_ = false;    
    point_key_p_ = const_cast<storage::Tuple *> (csp->GetPointQueryKey());
  } else if (!csp->IsFullIndexScan()) {
    // range scan
    is_point_query_ = false;
    is_full_scan_ = false;
    low_key_p_ = const_cast<storage::Tuple *> (csp->GetLowKey());
    high_key_p_ = const_cast<storage::Tuple *> (csp->GetHighKey());
  } else {
    // full scan
    is_point_query_ = false;
    is_full_scan_ = true;
  }
}

void IndexScanIterator::DoScan() {
  if (is_point_query_) {
    index_->ScanKey(point_key_p_, result_);
  } else if (is_full_scan_) {
    index_->ScanAllKeys(result_);
  } else {
    index_->CodeGenRangeScan(low_key_p_, high_key_p_, result_);
  }
  LOG_DEBUG("result size = %lu\n", result_.size());
  LogDoScanResults();

  // TODO: fix whatever implementation deficiency this comment implies
  // TODO:
  // currently the RowBatch produced in the index scan only contains one
  // tuple because 1.the tuples in RowBatch have to be in the same tile
  // group 2.the result order of index scan has to follow the key order
  // 3.in most cases, index is built on random data so the probability
  // that two continuous result tuples are in the same tile group is low
  // potential optimization: in the index scan results, find out the
  // continuous result tuples that are in the same tile group (and have
  // ascending tile group offset) and produce one RowBatch for these tuples
}

/*
 * Temporary
 * Find the index plan node. Correct solution is to take the runtime parameter
 * values and use them to initialize the scan range, in a thread safe
 * manner.
 */  
const planner::IndexScanPlan *IndexScanIterator::FindIndexPlan(const planner::AbstractPlan *node) {
  const planner::IndexScanPlan *index_node = nullptr;

  if (node->GetPlanNodeType() == PlanNodeType::INDEXSCAN) {
    return (reinterpret_cast<const planner::IndexScanPlan *> (node));
  }

  for (uint i=0; i<node->GetChildrenSize(); i++) {
    const planner::AbstractPlan *child_node = node->GetChild(i);
    index_node = FindIndexPlan(child_node);
    if (index_node) {
      return (index_node);
    }
  }
  // not found, return nullptr
  return (index_node);
}

// binary search to check whether the target offset is in the results
bool IndexScanIterator::RowOffsetInResult(uint64_t distinct_tile_index,
                                          uint32_t row_offset) {
  uint32_t l = result_metadata_[distinct_tile_index * 3 + 1];
  uint32_t r = result_metadata_[distinct_tile_index * 3 + 2];
  // binary search
  while (l < r) {
    uint32_t mid = l + (r - l) / 2;
    if (result_[mid]->offset == row_offset) {
      return true;
    }
    if (result_[mid]->offset > row_offset) {
      // right boundary exclusive?
      r = mid;
    } else {
      l = mid + 1;
    }
  }
  return false;
}

// for debugging
void IndexScanIterator::LogDoScanResults() {
  LOG_DEBUG("\n");
  LOG_DEBUG("\nDoScan: result size = %lu\n", result_.size());
  for (uint32_t i=0; i<result_.size(); i++) {
    LOG_DEBUG("%u: block: %u offset %u",
             i, result_[i]->block, result_[i]->offset);
  }
  LOG_DEBUG("\n");
}

}  // namespace util
}  // namespace codegen
}  // namespace peloton
