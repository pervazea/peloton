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
#include "codegen/proxy/runtime_functions_proxy.h"
#include "codegen/proxy/storage_manager_proxy.h"
#include "codegen/proxy/tile_group_proxy.h"
#include "codegen/proxy/transaction_runtime_proxy.h"
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
  void SetupRowBatch(RowBatch &batch,
                     TileGroup::TileGroupAccess &tile_group_access,
                     std::vector<AttributeAccess> &access) const;

  void FilterRowsByVisibility(CodeGen &codegen, llvm::Value *tid_start,
                              llvm::Value *tid_end,
                              Vector &selection_vector) const;

  void PerformReads(CodeGen &codegen, Vector &selection_vector) const;

  // Filter all the rows whose TIDs are in the range [tid_start, tid_end] and
  // store their TIDs in the output TID selection vector
  void FilterRowsByPredicate(CodeGen &codegen,
                             const TileGroup::TileGroupAccess &access,
                             llvm::Value *tid_start, llvm::Value *tid_end,
                             Vector &selection_vector) const;

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

  // obtains the predicate from the AbstractScanPlan.
  const auto *predicate = index_scan_.GetPredicate();
  
  if (predicate != nullptr) {
    context.Prepare(*predicate);
  }

  oid_t index_id = index_scan_.GetIndexId();
  auto index = index_scan_.GetTable()->GetIndexWithOid(index_id);
  PELOTON_ASSERT(index != nullptr);

  // debugging
  // std::vector<ItemPointer *> tuple_location_ptrs;
  // index->ScanAllKeys(tuple_location_ptrs);
}
  
void IndexScanTranslator::Produce() const {
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
    auto *i32_type = codegen.Int32Type();
    auto vec_size = Vector::kDefaultVectorSize.load();  
    auto *raw_vec = codegen.AllocateBuffer(i32_type, vec_size, "scanSelVector");
    Vector sel_vec{raw_vec, vec_size, i32_type};  

    // tracks the number of results from the scan
    llvm::Value *out_idx = codegen.Const32(0);  
    sel_vec.SetNumElements(out_idx);

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
    table_.GenerateIndexScan(codegen, GetCompilationContext(), table_ptr,
                             sel_vec.GetCapacity(),
                             scan_consumer, predicate_ptr, num_preds,
                             csp, index_ptr, index_scan_);
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

/* --------------------
 * ScanConsumer methods 
 * --------------------
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

  PerformReads(codegen, selection_vector_);

  // 3. Setup the (filtered) row batch and setup attribute accessors
  RowBatch batch{ctx_.GetCompilationContext(), tile_group_id_, tid_start,
                 tid_end, selection_vector_, true};

  std::vector<IndexScanTranslator::AttributeAccess> attribute_accesses;
  SetupRowBatch(batch, tile_group_access, attribute_accesses);

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
    LOG_DEBUG("Adding attribute '%s.%s' (%p) into row batch",
              index_plan_.GetTable()->GetName().c_str(),
              attribute->name.c_str(),
              attribute);
    batch.AddAttribute(attribute, &access[col_idx]);
  }
}

void IndexScanTranslator::ScanConsumer::FilterRowsByVisibility(
    CodeGen &codegen, llvm::Value *tid_start, llvm::Value *tid_end,
    Vector &selection_vector) const {
  
  ExecutionConsumer &ec = ctx_.GetCompilationContext().GetExecutionConsumer();
  llvm::Value *txn = ec.GetTransactionPtr(ctx_.GetCompilationContext());
  llvm::Value *raw_sel_vec = selection_vector.GetVectorPtr();

  llvm::Value *out_idx =
    codegen.Call(TransactionRuntimeProxy::PerformVisibilityCheck,
                 {txn, tile_group_ptr_, tid_start, tid_end, raw_sel_vec});
  selection_vector.SetNumElements(out_idx);
}

void IndexScanTranslator::ScanConsumer::FilterRowsByPredicate(
    CodeGen &codegen, const TileGroup::TileGroupAccess &access,
    llvm::Value *tid_start, llvm::Value *tid_end,
    Vector &selection_vector) const {
  // The batch we're filtering
  RowBatch batch{ctx_.GetCompilationContext(), tile_group_id_, tid_start,
                 tid_end, selection_vector, true};

  const auto *predicate = index_plan_.GetPredicate();
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
    PELOTON_ASSERT(valid_row.GetType().GetSqlType() ==
                   type::Boolean::Instance());
    llvm::Value *bool_val = type::Boolean::Instance().Reify(codegen, valid_row);

    // Set the validity of the row
    row.SetValidity(codegen, bool_val);
  });
}

void IndexScanTranslator::ScanConsumer::PerformReads(
    CodeGen &codegen, Vector &selection_vector) const {
  ExecutionConsumer &ec = ctx_.GetCompilationContext().GetExecutionConsumer();
  llvm::Value *txn = ec.GetTransactionPtr(ctx_.GetCompilationContext());
  llvm::Value *raw_sel_vec = selection_vector.GetVectorPtr();

  llvm::Value *is_for_update = codegen.ConstBool(index_plan_.IsForUpdate());
  llvm::Value *end_idx = selection_vector.GetNumElements();

  // Invoke TransactionRuntime::PerformVectorizedRead(...)
  llvm::Value *out_idx =
      codegen.Call(TransactionRuntimeProxy::PerformVectorizedRead,
                   {txn, tile_group_ptr_, raw_sel_vec, end_idx, is_for_update});
  selection_vector.SetNumElements(out_idx);
}

}  // namespace codegen
}  // namespace peloton
