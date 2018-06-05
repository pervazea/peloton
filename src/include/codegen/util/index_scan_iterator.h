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
   * Construct an iterator for the index. Arguments are either
   * a point_key or low, high keys
   *
   * @param[in]   point_key_p
   * @param[in]   low_key_p
   * @param[in]   high_key_p
   */
  IndexScanIterator(index::Index *index,
                    storage::Tuple *point_key_p,
                    storage::Tuple *low_key_p, storage::Tuple *high_key_p);

  /**
   * Iterate over the index and save results in temporary storage.
   */
  void DoScan();

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

  void UpdateTupleWithInteger(int value, int attribute_id, char *attribute_name,
                              bool is_lower_key);

  void UpdateTupleWithBigInteger(int64_t value, int attribute_id,
                                 char *attribute_name, bool is_lower_key);

  void UpdateTupleWithDouble(double value, int attribute_id,
                             char *attribute_name, bool is_lower_key);

  void UpdateTupleWithVarchar(char *value, int attribute_id,
                              char *attribute_name, bool is_lower_key);

  void UpdateTupleWithBoolean(bool value, int attribute_id,
                              char *attribute_name, bool is_lower_key);

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
