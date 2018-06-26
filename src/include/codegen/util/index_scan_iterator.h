//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// index_scan_iterator.h
//
// Identification: src/include/codegen/util/index_scan_iterator.h
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <algorithm>
#include <vector>

#include "codegen/codegen.h"
#include "index/index.h"
#include "planner/index_scan_plan.h"

namespace peloton {
namespace codegen {
namespace util {

// This class is for codegen and used by IndexScanIteratorProxy
//
// It extracts tile group information from the index, stores it temporarily,
// and makes it available via iterator usage. Three types of scan are
// supported:
// 1. point query
// 2. range scan
// 3. full scan
//  
// existing comment:  
// besides, the codegen index scan operator needs to update the parameters in
// this class when the codegen cache provides new parameters.
  
class IndexScanIterator {
 public:

  /**
   * Construct an iterator for the index. 
   *
   * @param[in]   index - constructing iterator for this index
   * @param[in]   executor_context. Runtime values for query parameters
   */
  
  IndexScanIterator(index::Index *index,
                    executor::ExecutorContext *executor_context);
  /**
   * Iterate over the index and save results in temporary storage.
   */
  void DoScan();

  const planner::IndexScanPlan *FindIndexPlan(const planner::AbstractPlan *node);

  uint64_t GetDistinctTileGroupNum() {
    return (uint64_t)distinct_tile_group_num_;
  }

  // TODO - consisent arg names...
  uint32_t GetTileGroupId(uint64_t distinct_tile_index) {
    return (uint32_t)(result_[distinct_tile_index]->block);
  }
  
  // TODO - consisent arg names...
  uint32_t GetTileGroupOffset(uint64_t result_iter) {
    return (uint32_t)(result_[result_iter]->offset);
  }

  bool RowOffsetInResult(uint64_t distinct_tile_index, uint32_t row_offset);

  uint64_t GetResultSize() { return (uint64_t)result_.size(); }

  // for debugging
  void LogDoScanResults();

 private:
  bool is_full_scan_;
  bool is_point_query_;
  index::Index *index_;

  // used if is_point_query_
  storage::Tuple *point_key_p_;

  // otherwise if a range scan
  storage::Tuple *low_key_p_;
  storage::Tuple *high_key_p_;

  // temporary storage. Information from index is stored here, and fed out
  // to the iterator.
  std::vector<ItemPointer *> result_;

  oid_t distinct_tile_group_num_;
  std::vector<uint32_t> result_metadata_;
};

}  // namespace util
}  // namespace codegen
}  // namespace peloton
