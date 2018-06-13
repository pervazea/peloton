//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// index_scan_translator.cpp
//
// Identification: src/codegen/operator/index_scan_translator.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/operator/index_scan_translator.h"

#include "codegen/lang/if.h"
#include "codegen/lang/vectorized_loop.h"
#include "codegen/proxy/data_table_proxy.h"
#include "codegen/proxy/executor_context_proxy.h"
#include "codegen/proxy/index_proxy.h"
// #include "codegen/proxy/index_scan_iterator_proxy.h"
#include "codegen/proxy/runtime_functions_proxy.h"
#include "codegen/proxy/storage_manager_proxy.h"
#include "codegen/proxy/tile_group_proxy.h"
#include "codegen/proxy/transaction_runtime_proxy.h"
// #include "codegen/operator/table_scan_translator.h"
#include "codegen/type/boolean_type.h"
#include "codegen/vector.h"
#include "common/internal_types.h"
#include "index/scan_optimizer.h"
#include "planner/index_scan_plan.h"
#include "storage/data_table.h"

namespace peloton {
namespace codegen {

/*------------------
 * ATTRIBUTE ACCESS
 *------------------
 */

// An attribute accessor that uses the backing tile group to access columns
  
class IndexScanTranslator::AttributeAccess :
    public RowBatch::AttributeAccess {
    
public:
  // Constructor
  AttributeAccess(const TileGroup::TileGroupAccess &access,
                  const planner::AttributeInfo *ai)
    : tile_group_access_(access), ai_(ai) {}

  // Access an attribute in the given row
  codegen::Value Access(CodeGen &codegen, RowBatch::Row &row) override {
  auto raw_row = tile_group_access_.GetRow(row.GetTID(codegen));
  return raw_row.LoadColumn(codegen, ai_->attribute_id);
  }
  
  const planner::AttributeInfo *GetAttributeRef() const { return ai_; }

private:
  // The accessor we use to load column values
  const TileGroup::TileGroupAccess &tile_group_access_;
  
  // The attribute we will access
  const planner::AttributeInfo *ai_;
};

//-------------------
// Scan consumer
//-------------------
  
class IndexScanTranslator::ScanConsumer : public codegen::ScanCallback {
public:
  // Constructor
  ScanConsumer(ConsumerContext &ctx,
               const planner::IndexScanPlan &index_plan,
               Vector &selection_vector)
    : ctx_(ctx),
      index_plan_(index_plan),
      selection_vector_(selection_vector),
      tile_group_id_(nullptr),
      tile_group_ptr_(nullptr) {}
  
  /*
  ScanConsumer(const IndexScanTranslator &translator,
               Vector &selection_vector);

  */

  // Initialize for reading from a tile group
  void TileGroupStart(CodeGen &, llvm::Value *tile_group_id,
                      llvm::Value *tile_group_ptr) override {
    tile_group_id_ = tile_group_id;
    tile_group_ptr_ = tile_group_ptr;
  }

  // Process a single tuple from the index
  void ProcessTuples(CodeGen &codegen, llvm::Value *tid_start,
                     llvm::Value *tid_end,
                     TileGroup::TileGroupAccess &tile_group_access) override;

  // Finished with this tile group
  void TileGroupFinish(CodeGen &, llvm::Value *) override {}

 private:
  // Get the predicate, if one exists
  // ???
  // const expression::AbstractExpression *GetPredicate() const;

  void SetupRowBatch(RowBatch &batch,
                     TileGroup::TileGroupAccess &tile_group_access,
                     std::vector<AttributeAccess> &access) const;

  void FilterRowsByVisibility(CodeGen &codegen, llvm::Value *tid_start,
                              llvm::Value *tid_end,
                              Vector &selection_vector) const;

  // Filter all the rows whose TIDs are in the range [tid_start, tid_end] and
  // store their TIDs in the output TID selection vector
  void FilterRowsByPredicate(CodeGen &codegen,
                             const TileGroup::TileGroupAccess &access,
                             llvm::Value *tid_start, llvm::Value *tid_end,
                             Vector &selection_vector) const;

  /*
  llvm::Value *SIMDFilterRows(RowBatch &batch,
                              const TileGroup::TileGroupAccess &access) const;
  */

 private:
  // The consumer context
  ConsumerContext &ctx_;
  
  // The translator instance the consumer is generating code for
  // obsolete?
  // const IndexScanTranslator &translator_;
  const planner::IndexScanPlan &index_plan_;

  // The selection vector used for vectorized scans
  Vector &selection_vector_;

  // The current tile group id we're scanning over
  llvm::Value *tile_group_id_;

  // The current tile group we're scanning over
  llvm::Value *tile_group_ptr_;
};  

//===----------------------------------------------------------------------===//
// INDEX SCAN TRANSLATOR
//===----------------------------------------------------------------------===//

// Constructor
IndexScanTranslator::IndexScanTranslator(
    const planner::IndexScanPlan &index_scan, CompilationContext &context,
    Pipeline &pipeline)
  : OperatorTranslator(index_scan, context, pipeline),
      index_scan_(index_scan),
      table_(*index_scan_.GetTable()) {

  // PA - GetPredicate obtains the predicate from the AbstractPlan.
  // Currently never set in any of the test scenarios... possibly never
  // set?
  const auto *predicate = index_scan_.GetPredicate();
  
  if (predicate != nullptr) {
    context.Prepare(*predicate);
  }

  oid_t index_id = index_scan_.GetIndexId();
  auto index = index_scan_.GetTable()->GetIndexWithOid(index_id);
  PELOTON_ASSERT(index != nullptr);

  // Set the conjunction scan predicate into the index scan plan.  
  // TODO: fix, const_cast? Determine if there is a better place
  // for keeping the CSP, e.g. move csp to query_state? c.f. hash join
  const_cast<planner::IndexScanPlan &>(index_scan_).SetIndexPredicate(
    index.get());
  
  // debugging
  // std::vector<ItemPointer *> tuple_location_ptrs;
  // index->ScanAllKeys(tuple_location_ptrs);
}
  
void IndexScanTranslator::Produce() const {

#ifdef notdef
  auto &codegen = GetCodeGen();

  const index::ConjunctionScanPredicate *csp =
    &(index_scan_.GetIndexPredicate().GetConjunctionList()[0]);

  // get pointer to data table
  storage::DataTable &table = *index_scan_.GetTable();
  
  llvm::Value *storage_manager_ptr = GetStorageManagerPtr();
  llvm::Value *db_oid = codegen.Const32(table.GetDatabaseOid());
  llvm::Value *table_oid = codegen.Const32(table.GetOid());
  // get llvm's ptr to the data table  
  llvm::Value *table_ptr =
      codegen.Call(StorageManagerProxy::GetTableWithOid,
                   {storage_manager_ptr, db_oid, table_oid});

  // get llvm's handle to the index
  llvm::Value *index_oid = codegen.Const32(index_scan_.GetIndexId());
  llvm::Value *index_ptr =
    codegen.Call(StorageManagerProxy::GetIndexWithOid,
                 {storage_manager_ptr, db_oid, table_oid, index_oid});

  // The selection vector for the scan
  /*
  auto *raw_vec = codegen.AllocateBuffer(
      codegen.Int32Type(), Vector::kDefaultVectorSize, "scanSelVector");
  Vector sel_vec{raw_vec, Vector::kDefaultVectorSize, codegen.Int32Type()};
  */
  auto *i32_type = codegen.Int32Type();
  auto vec_size = Vector::kDefaultVectorSize.load();  
  auto *raw_vec = codegen.AllocateBuffer(i32_type, vec_size, "scanSelVector");
  Vector sel_vec{raw_vec, vec_size, i32_type};  

  // tracks the number of results from the scan
  llvm::Value *out_idx = codegen.Const32(0);  
  sel_vec.SetNumElements(out_idx);

  // TODO - examine
  auto predicate = const_cast<expression::AbstractExpression *>(
      GetIndexScanPlan().GetPredicate());
  llvm::Value *predicate_ptr = codegen->CreateIntToPtr(
      codegen.Const64((int64_t)predicate),
      AbstractExpressionProxy::GetType(codegen)->getPointerTo());

  // zone maps not currently used. Remove?  
  size_t num_preds = 0;  
  auto *zone_map_manager = storage::ZoneMapManager::GetInstance();
  if (predicate != nullptr && zone_map_manager->ZoneMapTableExists()) {
    if (predicate->IsZoneMappable()) {
      num_preds = predicate->GetNumberofParsedPredicates();
    }
  }

  // Generate the scan
  ScanConsumer scan_consumer{*this, sel_vec};
  table_.GenerateIndexScan(codegen, table_ptr, sel_vec.GetCapacity(),
                           scan_consumer, predicate_ptr, num_preds,
                           csp, index_ptr);
#endif /* notdef */

  auto producer = [this](ConsumerContext &ctx) {
    auto &codegen = GetCodeGen();

    const index::ConjunctionScanPredicate *csp =
    &(index_scan_.GetIndexPredicate().GetConjunctionList()[0]);

    // get pointer to data table
    storage::DataTable &table = *index_scan_.GetTable();
  
    llvm::Value *storage_manager_ptr = GetStorageManagerPtr();
    llvm::Value *db_oid = codegen.Const32(table.GetDatabaseOid());
    llvm::Value *table_oid = codegen.Const32(table.GetOid());
    // get llvm's ptr to the data table  
    llvm::Value *table_ptr =
    codegen.Call(StorageManagerProxy::GetTableWithOid,
    {storage_manager_ptr, db_oid, table_oid});

    // get llvm's handle to the index
    llvm::Value *index_oid = codegen.Const32(index_scan_.GetIndexId());
    llvm::Value *index_ptr =
    codegen.Call(StorageManagerProxy::GetIndexWithOid,
    {storage_manager_ptr, db_oid, table_oid, index_oid});

    // The selection vector for the scan
    /*
      auto *raw_vec = codegen.AllocateBuffer(
      codegen.Int32Type(), Vector::kDefaultVectorSize, "scanSelVector");
      Vector sel_vec{raw_vec, Vector::kDefaultVectorSize, codegen.Int32Type()};
    */
    auto *i32_type = codegen.Int32Type();
    auto vec_size = Vector::kDefaultVectorSize.load();  
    auto *raw_vec = codegen.AllocateBuffer(i32_type, vec_size, "scanSelVector");
    Vector sel_vec{raw_vec, vec_size, i32_type};  

    // tracks the number of results from the scan
    llvm::Value *out_idx = codegen.Const32(0);  
    sel_vec.SetNumElements(out_idx);

    // TODO - examine
    auto predicate = const_cast<expression::AbstractExpression *>(
      GetIndexScanPlan().GetPredicate());
    
    llvm::Value *predicate_ptr = codegen->CreateIntToPtr(
      codegen.Const64((int64_t)predicate),
      AbstractExpressionProxy::GetType(codegen)->getPointerTo());

    // zone maps not currently used. Remove?  
    size_t num_preds = 0;  
    auto *zone_map_manager = storage::ZoneMapManager::GetInstance();
    if (predicate != nullptr && zone_map_manager->ZoneMapTableExists()) {
      if (predicate->IsZoneMappable()) {
        num_preds = predicate->GetNumberofParsedPredicates();
      }
    }

    ScanConsumer scan_consumer{ctx, GetScanPlan(), sel_vec};
    table_.GenerateIndexScan(codegen, table_ptr, sel_vec.GetCapacity(),
                             scan_consumer, predicate_ptr, num_preds,
                             csp, index_ptr);
  };
  
  // Execute serially
  GetPipeline().RunSerial(producer);
}

// Get the name of this scan
std::string IndexScanTranslator::GetName() const {
  //std::string name = "Scan('" + GetIndex().GetName() + "'";
  std::string name = "";
  auto *predicate = GetIndexScanPlan().GetPredicate();
  
  if (predicate != nullptr) {
    name.append(", ").append(std::to_string(Vector::kDefaultVectorSize));
  }
  name.append(")");
  return name;
}

const storage::DataTable &IndexScanTranslator::GetTable() const {
  return *index_scan_.GetTable();
}

// TODO - determine if needed. Currently not used
void IndexScanTranslator::SetIndexPredicate(UNUSED_ATTRIBUTE CodeGen &codegen,
                                            UNUSED_ATTRIBUTE llvm::Value *iterator_ptr) const {
  /*
  std::vector<const planner::AttributeInfo *> where_clause_attributes;
  std::vector<const expression::AbstractExpression *>
    constant_value_expressions;
  std::vector<ExpressionType> comparison_type;
  // get predicate from abstract_scan_plan
  const auto *predicate = index_scan_.GetPredicate();
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
  */
}    

/* --------------------
 * ScanConsumer methods 
 * --------------------
 */

/*
// Constructor
IndexScanTranslator::ScanConsumer::ScanConsumer(
    const IndexScanTranslator &translator, Vector &selection_vector)
    : translator_(translator), selection_vector_(selection_vector) {}
*/

// Generate the body of the vectorized scan
void IndexScanTranslator::ScanConsumer::ProcessTuples(
    CodeGen &codegen, llvm::Value *tid_start, llvm::Value *tid_end,
    TileGroup::TileGroupAccess &tile_group_access) {

  // 1. Filter the rows in the range [tid_start, tid_end) by txn visibility
  // Visibility check is done in the runtime proxy
  FilterRowsByVisibility(codegen, tid_start, tid_end, selection_vector_);

  // 2. Filter rows by the given predicate (if one exists)
  //auto *predicate = GetPredicate();
  auto *predicate = index_plan_.GetPredicate();
  if (predicate != nullptr) {
    // First perform a vectorized filter, putting TIDs into the selection vector
    FilterRowsByPredicate(codegen, tile_group_access, tid_start, tid_end,
                          selection_vector_);
  }

  // 3. Setup the (filtered) row batch and setup attribute accessors
  RowBatch batch{ctx_.GetCompilationContext(), tile_group_id_, tid_start,
                 tid_end, selection_vector_, true};

  std::vector<IndexScanTranslator::AttributeAccess> attribute_accesses;
  SetupRowBatch(batch, tile_group_access, attribute_accesses);

  // 4. Push the batch into the pipeline
  /*
  ConsumerContext context{translator_.GetCompilationContext(),
                          translator_.GetPipeline()};
  */
  ctx_.Consume(batch);
}

void IndexScanTranslator::ScanConsumer::SetupRowBatch(
    RowBatch &batch, TileGroup::TileGroupAccess &tile_group_access,
    std::vector<IndexScanTranslator::AttributeAccess> &access) const {
  // Grab a hold of the stuff we need (i.e., the plan, all the attributes, and
  // the IDs of the columns the scan _actually_ produces)
  
  //const auto &scan_plan = translator_.GetScanPlan();
  std::vector<const planner::AttributeInfo *> ais;
  index_plan_.GetAttributes(ais);
  const auto &output_col_ids = index_plan_.GetColumnIds();

  // 1. Put all the attribute accessors into a vector
  access.clear();
  for (oid_t col_idx = 0; col_idx < output_col_ids.size(); col_idx++) {
    access.emplace_back(tile_group_access, ais[output_col_ids[col_idx]]);
  }

  // 2. Add the attribute accessors into the row batch
  for (oid_t col_idx = 0; col_idx < output_col_ids.size(); col_idx++) {
    auto *attribute = ais[output_col_ids[col_idx]];
    LOG_TRACE("Adding attribute '%s.%s' (%p) into row batch",
              index_plan_.GetTable()->GetName().c_str(),
              attribute->name.c_str(),
              attribute);
    batch.AddAttribute(attribute, &access[col_idx]);
  }
}

void IndexScanTranslator::ScanConsumer::FilterRowsByVisibility(
    CodeGen &codegen, llvm::Value *tid_start, llvm::Value *tid_end,
    Vector &selection_vector) const {
  /*
  llvm::Value *executor_context_ptr =
      translator_.GetCompilationContext().GetExecutorContextPtr();
  */
  ExecutionConsumer &ec = ctx_.GetCompilationContext().GetExecutionConsumer();
  llvm::Value *txn = ec.GetTransactionPtr(ctx_.GetCompilationContext());
  /*
  llvm::Value *txn = codegen.Call(ExecutorContextProxy::GetTransaction,
                                  {executor_context_ptr});
  */
  llvm::Value *raw_sel_vec = selection_vector.GetVectorPtr();

  // Invoke TransactionRuntime::PerformRead(...)
  llvm::Value *out_idx =
      codegen.Call(TransactionRuntimeProxy::PerformVectorizedRead,
                   {txn, tile_group_ptr_, tid_start, tid_end, raw_sel_vec});
  selection_vector.SetNumElements(out_idx);
}

  /*
// Get the predicate, if one exists
const expression::AbstractExpression *
IndexScanTranslator::ScanConsumer::GetPredicate() const {
  return translator_.GetScanPlan().GetPredicate();
}
  */

void IndexScanTranslator::ScanConsumer::FilterRowsByPredicate(
    CodeGen &codegen, const TileGroup::TileGroupAccess &access,
    llvm::Value *tid_start, llvm::Value *tid_end,
    Vector &selection_vector) const {
  // The batch we're filtering
  //auto &compilation_ctx = translator_.GetCompilationContext();
  
  RowBatch batch{ctx_.GetCompilationContext(), tile_group_id_, tid_start,
                 tid_end, selection_vector, true};

  const auto *predicate = index_plan_.GetPredicate();
  // First, check if the predicate is SIMDable
  // LOG_DEBUG("Is Predicate SIMDable : %d", predicate->IsSIMDable());
  // Determine the attributes the predicate needs
  std::unordered_set<const planner::AttributeInfo *> used_attributes;
  predicate->GetUsedAttributes(used_attributes);

  // Setup the row batch with attribute accessors for the predicate
  std::vector<AttributeAccess> attribute_accessors;
  for (const auto *ai : used_attributes) {
    attribute_accessors.emplace_back(access, ai);
  }
  for (uint32_t i = 0; i < attribute_accessors.size(); i++) {
    auto &accessor = attribute_accessors[i];
    batch.AddAttribute(accessor.GetAttributeRef(), &accessor);
  }

  // Iterate over the batch using a scalar loop
  batch.Iterate(codegen, [&](RowBatch::Row &row) {
    // Evaluate the predicate to determine row validity
    codegen::Value valid_row = row.DeriveValue(codegen, *predicate);

    // Reify the boolean value since it may be NULL
    PELOTON_ASSERT(valid_row.GetType().GetSqlType() == type::Boolean::Instance());
    llvm::Value *bool_val = type::Boolean::Instance().Reify(codegen, valid_row);

    // Set the validity of the row
    row.SetValidity(codegen, bool_val);
  });
}
  

}  // namespace codegen
}  // namespace peloton
