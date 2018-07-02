//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// table.cpp
//
// Identification: src/codegen/table.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/table.h"

#include "catalog/schema.h"
#include "codegen/proxy/data_table_proxy.h"
#include "codegen/lang/loop.h"
#include "codegen/lang/if.h"
#include "codegen/proxy/index_scan_iterator_proxy.h"
#include "codegen/proxy/runtime_functions_proxy.h"
#include "codegen/vector.h"
#include "codegen/proxy/zone_map_proxy.h"
#include "storage/data_table.h"

namespace peloton {
namespace codegen {

// Constructor
Table::Table(storage::DataTable &table)
    : table_(table), tile_group_(*table_.GetSchema()) {}

  /*
llvm::Value *Table::GetExecutorContextPtr() const {
  return context_.GetExecutionConsumer().GetExecutorContextPtr(context_);
  */

// We determine tile group count by calling DataTable::GetTileGroupCount(...)
llvm::Value *Table::GetTileGroupCount(CodeGen &codegen,
                                      llvm::Value *table_ptr) const {
  return codegen.Call(DataTableProxy::GetTileGroupCount, {table_ptr});
}

// We acquire a tile group instance by calling RuntimeFunctions::GetTileGroup().
llvm::Value *Table::GetTileGroup(CodeGen &codegen, llvm::Value *table_ptr,
                                 llvm::Value *tile_group_id) const {
  return codegen.Call(RuntimeFunctionsProxy::GetTileGroup,
                      {table_ptr, tile_group_id});
}

// We acquire a Zone Map manager instance
llvm::Value *Table::GetZoneMapManager(CodeGen &codegen) const {
  return codegen.Call(ZoneMapManagerProxy::GetInstance, {});
}

// Generate a scan over all tile groups.
//
// @code
// column_layouts := alloca<peloton::ColumnLayoutInfo>(
//     table.GetSchema().GetColumnCount())
// predicate_array := alloca<peloton::PredicateInfo>(
//     num_predicates)
//
// oid_t tile_group_idx := 0
// num_tile_groups = GetTileGroupCount(table_ptr)
//
// for (; tile_group_idx < num_tile_groups; ++tile_group_idx) {
//   if (ShouldScanTileGroup(predicate_array, tile_group_idx)) {
//      tile_group_ptr := GetTileGroup(table_ptr, tile_group_idx)
//      consumer.TileGroupStart(tile_group_ptr);
//      tile_group.TidScan(tile_group_ptr, column_layouts, vector_size,
//                         consumer);
//      consumer.TileGroupEnd(tile_group_ptr);
//   }
// }
//
// @endcode
void Table::GenerateScan(CodeGen &codegen, llvm::Value *table_ptr,
                         llvm::Value *tilegroup_start,
                         llvm::Value *tilegroup_end, uint32_t batch_size,
                         llvm::Value *predicate_ptr, size_t num_predicates,
                         ScanCallback &consumer) const {
  // Allocate some space for the column layouts
  const auto num_columns =
      static_cast<uint32_t>(table_.GetSchema()->GetColumnCount());
  llvm::Value *column_layouts = codegen.AllocateBuffer(
      ColumnLayoutInfoProxy::GetType(codegen), num_columns, "columnLayout");

  // Allocate some space for the parsed predicates (if need be!)
  llvm::Value *predicate_array =
      codegen.NullPtr(PredicateInfoProxy::GetType(codegen)->getPointerTo());
  if (num_predicates != 0) {
    // LOG_DEBUG("num_predicates = %lu", num_predicates);
    predicate_array = codegen.AllocateBuffer(
        PredicateInfoProxy::GetType(codegen), num_predicates, "predicateInfo");
    codegen.Call(RuntimeFunctionsProxy::FillPredicateArray,
                 {predicate_ptr, predicate_array});
  }

  // Get the number of tile groups in the given table
  llvm::Value *tile_group_idx =
      (tilegroup_start != nullptr ? tilegroup_start : codegen.Const64(0));
  llvm::Value *num_tile_groups =
      (tilegroup_end != nullptr ? tilegroup_end
                                : GetTileGroupCount(codegen, table_ptr));

  lang::Loop loop{codegen,
                  codegen->CreateICmpULT(tile_group_idx, num_tile_groups),
                  {{"tileGroupIdx", tile_group_idx}}};
  {
    // Get the tile group with the given tile group ID
    tile_group_idx = loop.GetLoopVar(0);
    llvm::Value *tile_group_ptr =
        GetTileGroup(codegen, table_ptr, tile_group_idx);
    llvm::Value *tile_group_id =
        tile_group_.GetTileGroupId(codegen, tile_group_ptr);

    // Check zone map
    llvm::Value *cond = codegen.Call(
        ZoneMapManagerProxy::ShouldScanTileGroup,
        {GetZoneMapManager(codegen), predicate_array,
         codegen.Const32(num_predicates), table_ptr, tile_group_idx});

    codegen::lang::If should_scan_tilegroup{codegen, cond};
    {
      // Inform the consumer that we're starting iteration over the tile group
      consumer.TileGroupStart(codegen, tile_group_id, tile_group_ptr);

      // Generate the scan cover over the given tile group
      tile_group_.GenerateTidScan(codegen, tile_group_ptr, column_layouts,
                                  batch_size, consumer);

      // Inform the consumer that we've finished iteration over the tile group
      consumer.TileGroupFinish(codegen, tile_group_ptr);
    }
    should_scan_tilegroup.EndIf();

    // Move to next tile group in the table
    tile_group_idx = codegen->CreateAdd(tile_group_idx, codegen.Const64(1));
    loop.LoopEnd(codegen->CreateICmpULT(tile_group_idx, num_tile_groups),
                 {tile_group_idx});
  }
}

// TODO - *** update comment  
// Generate an index scan
//
// @code
// column_layouts := alloca<peloton::ColumnLayoutInfo>(
//     table.GetSchema().GetColumnCount())
// predicate_array := alloca<peloton::PredicateInfo>(
//     num_predicates)
//
// oid_t tile_group_idx := 0
// num_tile_groups = GetTileGroupCount(table_ptr)
//
// for (; tile_group_idx < num_tile_groups; ++tile_group_idx) {
//   if (ShouldScanTileGroup(predicate_array, tile_group_idx)) {
//      tile_group_ptr := GetTileGroup(table_ptr, tile_group_idx)
//      consumer.TileGroupStart(tile_group_ptr);
//      tile_group.TidScan(tile_group_ptr, column_layouts, vector_size,
//                         consumer);
//      consumer.TileGroupEnd(tile_group_ptr);
//   }
// }
//
// @endcode
void Table::GenerateIndexScan(CodeGen &codegen,
                              CompilationContext &context,
                              llvm::Value *table_ptr,
                              uint32_t batch_size,
                              ScanCallback &consumer,
                              llvm::Value *predicate_ptr,
                              size_t num_predicates,
                              const index::ConjunctionScanPredicate *csp,
                              llvm::Value *index_ptr,
                              const planner::IndexScanPlan &index_scan) const {

  (void) csp;  // remove csp
  
  // Allocate some space for the column layouts
  const auto num_columns =
      static_cast<uint32_t>(table_.GetSchema()->GetColumnCount());
  llvm::Value *column_layouts = codegen.AllocateBuffer(
      ColumnLayoutInfoProxy::GetType(codegen), num_columns, "columnLayout");

  (void) batch_size;  // unused
  
  // Allocate some space for the parsed predicates (if need be!)
  // Remove? Since no zone maps?
  llvm::Value *predicate_array =
      codegen.NullPtr(PredicateInfoProxy::GetType(codegen)->getPointerTo());
  if (num_predicates != 0) {
    predicate_array = codegen.AllocateBuffer(
        PredicateInfoProxy::GetType(codegen), num_predicates, "predicateInfo");
    codegen.Call(RuntimeFunctionsProxy::FillPredicateArray,
                 {predicate_ptr, predicate_array});
  }

  // construct array of column ids GetKeyColumnIds()
  // type = oid_t, size taken from plan
  auto plan_key_column_id_vec = index_scan.GetKeyColumnIds();
  auto kci_size = plan_key_column_id_vec.size();
  llvm::Value *raw_column_vec = codegen.AllocateBuffer(codegen.Int32Type(),
                                                       kci_size,
                                                       "keyColumnIds");
  Vector key_column_ids(raw_column_vec, kci_size, codegen.Int32Type());
  // populate key_column_ids
  for (uint32_t i=0; i<kci_size; i++) {
    llvm::Value *key_col_index = codegen.Const32(i);
    llvm::Value *key_col_value = codegen.Const32(plan_key_column_id_vec[i]);
    key_column_ids.SetValue(codegen, key_col_index, key_col_value);
  }
  
  // construct array of expressions GetExprTypes()
  // TODO

  // Get the iterator, to iterate over the index values.
  llvm::Value *iterator_ptr =
    codegen.Call(RuntimeFunctionsProxy::GetIterator,
                 {context.GetExecutionConsumer().GetExecutorContextPtr(context),
                     index_ptr});

  (void) index_scan;
  // DoScan extracts data from the index and keeps the results, to be
  // retrieved via subsequent calls to the proxy
  codegen.Call(IndexScanIteratorProxy::DoScan, {iterator_ptr});

  // result_size is the number of index entries i.e. rows, to read
  llvm::Value *result_size =
    codegen.Call(IndexScanIteratorProxy::GetResultSize, {iterator_ptr});

  // result_idx will be used to iterate over the results and retrieve
  // them
  llvm::Value *result_idx = codegen.Const64(0);
  lang::Loop loop{codegen,
      codegen->CreateICmpULT(result_idx, result_size),
        {{"distinctTileGroupIter", result_idx}}};
  {
    result_idx = loop.GetLoopVar(0);

    // get the tile group id
    llvm::Value *tile_group_id = codegen.Call(
        IndexScanIteratorProxy::GetTileGroupId, {iterator_ptr, result_idx});
    
    // for the row, get the offset into the tile group
    llvm::Value *tile_group_offset = codegen.Call(
      IndexScanIteratorProxy::GetTileGroupOffset, {iterator_ptr, result_idx});

    // get a ptr to the tile group
    llvm::Value *tile_group_ptr = codegen.Call(
      RuntimeFunctionsProxy::GetTileGroupById, {table_ptr, tile_group_id});

    // TODO: optimize to do metadata looks on the tile_group_id only
    // once.
    
    // initialize the consumer, to handle the tile_group
    consumer.TileGroupStart(codegen, tile_group_id, tile_group_ptr);
    
    // read and process the tuple from tile_group_offset
    tile_group_.GenerateIndexScan(codegen, tile_group_ptr, column_layouts,
                                  tile_group_offset,
                                  consumer);
    // done with this tuple
    consumer.TileGroupFinish(codegen, tile_group_ptr);    

    // Move to next result item from the index
    result_idx = codegen->CreateAdd(result_idx, codegen.Const64(1));
    
    loop.LoopEnd(codegen->CreateICmpULT(result_idx, result_size),
                 {result_idx});
  }
}

}  // namespace codegen
}  // namespace peloton
