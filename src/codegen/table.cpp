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
#include "codegen/proxy/zone_map_proxy.h"
#include "storage/data_table.h"

namespace peloton {
namespace codegen {

// Constructor
Table::Table(storage::DataTable &table)
    : table_(table), tile_group_(*table_.GetSchema()) {}

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

  /* PA -  was
  llvm::Value *tile_group_idx = codegen.Const64(0);
  // Get the number of tile groups in the given table  
  llvm::Value *num_tile_groups = GetTileGroupCount(codegen, table_ptr);
  */
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
                              llvm::Value *table_ptr,
                              uint32_t batch_size,
                              ScanCallback &consumer,
                              llvm::Value *predicate_ptr,
                              size_t num_predicates,
                              const index::ConjunctionScanPredicate *csp,
                              llvm::Value *index_ptr) const {
  
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

  // point scan key
  llvm::Value *point_key = codegen.Const64(0);
  // range scan keys (low, high)  
  llvm::Value *low_key = codegen.Const64(0);
  llvm::Value *high_key = codegen.Const64(0);

  // determine the type of scan, and set values for the corresponding keys
  if (csp->IsPointQuery()) {
    auto ncg_point_key = csp->GetPointQueryKey();
    point_key = codegen.Const64((int64_t)(ncg_point_key));
  } else if (!csp->IsFullIndexScan()) {
    // range scan
    low_key = codegen.Const64((int64_t)(csp->GetLowKey()));
    high_key = codegen.Const64((int64_t)(csp->GetHighKey()));
  }
  
  // initialize the interator
  llvm::Value *iterator_ptr =
    codegen.Call(RuntimeFunctionsProxy::GetIterator,
                 {index_ptr, point_key, low_key, high_key});

  // before doing scan, update the tuple with parameter cache!
  // SetIndexPredicate(codegen, iterator_ptr);

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

    /*
    //??
    codegen.Call(
        RuntimeFunctionsProxy::GetTileGroupLayout,
        {tile_group_ptr, column_layouts, codegen.Const32(num_columns)});

    // tile_group_.ScanTileOffset(sel_vec):

    // to fix
    // Inform the consumer that we're starting iteration over the tile group
    consumer.TileGroupStart(codegen, tile_group_id, tile_group_ptr);

    // Generate the scan cover over the given tile group
    tile_group_.GenerateTidScan(codegen, tile_group_ptr, column_layouts,
                                batch_size, consumer);

    // Inform the consumer that we've finished iteration over the tile group
    consumer.TileGroupFinish(codegen, tile_group_ptr);
    // end to fix
    */
    
    // Move to next result item from the index
    result_idx = codegen->CreateAdd(result_idx, codegen.Const64(1));
    
    loop.LoopEnd(codegen->CreateICmpULT(result_idx, result_size),
                 {result_idx});
  }
}

/*
// replace index_scan with predicate, or pull out entirely and
// do check externally  
void Table::SetIndexPredicate(CodeGen &codegen,
                              llvm::Value *iterator_ptr,
                              const planner::IndexScanPlan &index_scan) const {
  std::vector<const planner::AttributeInfo *> where_clause_attributes;
  std::vector<const expression::AbstractExpression *>
    constant_value_expressions;
  std::vector<ExpressionType> comparison_type;
  // get predicate from abstract_scan_plan
  const auto *predicate = index_scan.GetPredicate();
  if (predicate == nullptr) {
    return;
  }
  auto &context = GetCompilationContext();
  const auto &parameter_cache = context.GetParameterCache();
  const QueryParametersMap &parameters_map =
    parameter_cache.GetQueryParametersMap();
  
  predicate->GetUsedAttributesInPredicateOrder(where_clause_attributes,
                                               constant_value_expressions);
  predicate->GetComparisonTypeInPredicateOrder(comparison_type);
  for (unsigned int i = 0; i < where_clause_attributes.size(); i++) {
    const auto *ai = where_clause_attributes[i];
    llvm::Value *attribute_id = codegen.Const32(ai->attribute_id);
    llvm::Value *attribute_name = codegen.ConstStringPtr(ai->name);
    bool is_lower_key = false;
    if (comparison_type[i] == peloton::ExpressionType::COMPARE_GREATERTHAN ||
        comparison_type[i] ==
        peloton::ExpressionType::COMPARE_GREATERTHANOREQUALTO) {
      is_lower_key = true;
    }
    llvm::Value *is_lower = codegen.ConstBool(is_lower_key);

    // figure out codegen parameter index for this attribute
    auto parameters_index =
      parameters_map.GetIndex(constant_value_expressions[i]);
    llvm::Value *parameter_value =
      parameter_cache.GetValue(parameters_index).GetValue();
    switch (ai->type.type_id) {
      case peloton::type::TypeId::TINYINT:
      case peloton::type::TypeId::SMALLINT:
      case peloton::type::TypeId::INTEGER: {
        codegen.Call(IndexScanIteratorProxy::UpdateTupleWithInteger,
                     {iterator_ptr, parameter_value, attribute_id,
                         attribute_name, is_lower});
        break;
      }
      
      case peloton::type::TypeId::TIMESTAMP:
      case peloton::type::TypeId::BIGINT: {
        codegen.Call(IndexScanIteratorProxy::UpdateTupleWithBigInteger,
                     {iterator_ptr, codegen->CreateSExt(parameter_value,
                                                        codegen.Int64Type()),
                         attribute_id, attribute_name, is_lower});
        break;
      }
      
      case peloton::type::TypeId::DECIMAL: {
        if (parameter_value->getType() != codegen.DoubleType()) {
          codegen.Call(
                       IndexScanIteratorProxy::UpdateTupleWithDouble,
                       {iterator_ptr,
                           codegen->CreateSIToFP(parameter_value, codegen.DoubleType()),
                           attribute_id, attribute_name, is_lower});
        } else {
          codegen.Call(IndexScanIteratorProxy::UpdateTupleWithDouble,
                       {iterator_ptr, parameter_value, attribute_id,
                           attribute_name, is_lower});
        }
        break;
      }
      
      case peloton::type::TypeId::VARBINARY:
      case peloton::type::TypeId::VARCHAR: {
        codegen.Call(IndexScanIteratorProxy::UpdateTupleWithVarchar,
                     {iterator_ptr, parameter_value, attribute_id,
                         attribute_name, is_lower});
        break;
      }
      
      case peloton::type::TypeId::BOOLEAN: {
        codegen.Call(IndexScanIteratorProxy::UpdateTupleWithBoolean,
                     {iterator_ptr, parameter_value, attribute_id,
                         attribute_name, is_lower});
        break;
      }
      
      default: {
        throw new Exception("Type" +
                            peloton::TypeIdToString(ai->type.type_id) +
                            " is not supported in codegen yet");
      }
    }
  }
} 
   
*/  

}  // namespace codegen
}  // namespace peloton
