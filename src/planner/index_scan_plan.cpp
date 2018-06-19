//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// index_scan_plan.cpp
//
// Identification: src/planner/index_scan_plan.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "planner/index_scan_plan.h"
#include "common/internal_types.h"
#include "expression/constant_value_expression.h"
#include "expression/expression_util.h"
#include "storage/data_table.h"

namespace peloton {
namespace planner {

IndexScanPlan::IndexScanPlan(storage::DataTable *table,
                             expression::AbstractExpression *predicate,
                             const std::vector<oid_t> &column_ids,
                             const IndexScanDesc &index_scan_desc,
                             bool for_update_flag)
  : AbstractScan(table, nullptr, column_ids, false),
    index_id_(index_scan_desc.index_id),
    // column_ids_(column_ids),
    key_column_ids_(std::move(index_scan_desc.tuple_column_id_list)),
    expr_types_(std::move(index_scan_desc.expr_list)),
    values_with_params_(std::move(index_scan_desc.value_list)),
    runtime_keys_(std::move(index_scan_desc.runtime_key_list)) {
  LOG_TRACE("Creating an Index Scan Plan");

  if (for_update_flag == true) {
    SetForUpdateFlag(true);
  }

  // SetTargetTable(table);

  // Is this redundant also? If we set the predicate via the
  // AbstractScan constructor?
  if (predicate != NULL) {
    SetPredicate(predicate);
  }

  // copy the value over for binding purpose
  for (auto val : values_with_params_) {
    values_.push_back(val.Copy());
  }

  // Check whether the scan range is left/right open. Because the index itself
  // is not able to handle that exactly, we must have extra logic in
  // IndexScanExecutor to handle that case.
  //
  // TODO: We may also need a flag for "IN" expression after we support "IN".
  for (auto expr_type : expr_types_) {
    if (expr_type == ExpressionType::COMPARE_GREATERTHAN) left_open_ = true;

    if (expr_type == ExpressionType::COMPARE_LESSTHAN) right_open_ = true;
  }

  // set the index predicate (conjuction scan predicate)
  oid_t index_id = GetIndexId();
  auto index = GetTable()->GetIndexWithOid(index_id);
  PELOTON_ASSERT(index != nullptr);
  SetIndexPredicate(index.get());
  return;
}

/*
 * SetParameterValues() - Late binding for arguments specified in the
 *                        constructor
 *
 * 1. Do not use this function to change a field in the index key!!!!
 * 2. Only fields specified by the constructor could be modofied
 */
void IndexScanPlan::SetParameterValues(std::vector<type::Value> *values) {
  LOG_INFO("Setting parameter values in Index Scans");

  // Destroy the values of the last plan and copy the original values over for
  // binding
  values_.clear();
  for (auto val : values_with_params_) {
    values_.push_back(val.Copy());
  }

  for (unsigned int i = 0; i < values_.size(); ++i) {
    auto value = values_[i];
    auto column_id = key_column_ids_[i];
    if (value.GetTypeId() == type::TypeId::PARAMETER_OFFSET) {
      int offset = value.GetAs<int32_t>();
      values_[i] =
          (values->at(offset))
              .CastAs(GetTable()->GetSchema()->GetColumn(column_id).GetType());
    }
  }

  // Also bind values to index scan predicate object
  //
  // NOTE: This could only be called by one thread at a time

  oid_t index_id = GetIndexId();
  auto index = GetTable()->GetIndexWithOid(index_id);
  PELOTON_ASSERT(index != nullptr);
  index_predicate_.LateBindValues(index.get(), *values);

  for (auto &child_plan : GetChildren()) {
    child_plan->SetParameterValues(values);
  }
}

void IndexScanPlan::VisitParameters(
  codegen::QueryParametersMap &map,
  std::vector<peloton::type::Value> &values,
  const std::vector<peloton::type::Value> &values_from_user) {
  
  AbstractPlan::VisitParameters(map, values, values_from_user);
  auto *predicate =
    const_cast<expression::AbstractExpression *>(GetPredicate());
  
  if (predicate != nullptr) {
    predicate->VisitParameters(map, values, values_from_user);
  }
}

void IndexScanPlan::SetIndexPredicate(index::Index *index_p) {
  index_predicate_.AddConjunctionScanPredicate(index_p,
                                               GetValues(),
                                               GetKeyColumnIds(),
                                               GetExprTypes());
}

#ifdef notdef  
hash_t IndexScanPlan::Hash() const {
  auto type = GetPlanNodeType();
  hash_t hash = HashUtil::Hash(&type);
  LOG_DEBUG("type hash: %lu", hash);

  auto table_hash = GetTable()->Hash();
  LOG_DEBUG("table hash: %lu", table_hash);

  hash = HashUtil::CombineHashes(hash, GetTable()->Hash());
  LOG_DEBUG(" --hash now: %lu", hash);

  if (GetPredicate() != nullptr) {
    auto pred_hash = GetPredicate()->Hash();
    LOG_DEBUG("pred hash: %lu", pred_hash);  
    hash = HashUtil::CombineHashes(hash, GetPredicate()->Hash());
    LOG_DEBUG(" --hash now: %lu", hash);    
  }

  for (auto &column_id : GetColumnIds()) {
    hash = HashUtil::CombineHashes(hash, HashUtil::Hash(&column_id));
    LOG_DEBUG(" --hash now: %lu", hash);    
  }

  // hash the index id
  hash = HashUtil::CombineHashes(hash, HashUtil::Hash(&index_id_));
  LOG_DEBUG(" --hash now: %lu", hash);

  // hash the type of index scan
  /* TODO: examine
   * 1. Hash type of index scan
   * 2. Hash predicate
   * 3. This hashes only 1 conjunction. Is that all we support? Should
   * assert that we only have one or properly handle as many as there are.
   */
  const index::ConjunctionScanPredicate *csp =
      &(index_predicate_.GetConjunctionList()[0]);
  if ( csp ) {
    // TODO - is it valid for csp to be null?
    auto is_point_query = csp->IsPointQuery();
    hash = HashUtil::CombineHashes(hash, HashUtil::Hash(&is_point_query));
    auto is_full_scan = csp->IsFullIndexScan();
    hash = HashUtil::CombineHashes(hash, HashUtil::Hash(&is_full_scan));
    LOG_DEBUG(" --hash now: %lu", hash);
  }

  auto is_update = IsForUpdate();
  hash = HashUtil::CombineHashes(hash, HashUtil::Hash(&is_update));
  LOG_DEBUG(" --hash now: %lu", hash);            

  hash = HashUtil::CombineHashes(hash, AbstractPlan::Hash());
  LOG_DEBUG(" final hash: %lu", hash);
  return hash;
}  
#endif /* notdef */
  
hash_t IndexScanPlan::Hash() const {
  // start with node type (index scan plan)
  auto type = GetPlanNodeType();
  hash_t hash = HashUtil::Hash(&type);

  // add the (indexed) storage table
  hash = HashUtil::CombineHashes(hash, GetTable()->Hash());

  if (GetPredicate() != nullptr) {
    // add the (non-index) predicate
    hash = HashUtil::CombineHashes(hash, GetPredicate()->Hash());
  }

  for (auto &column_id : GetColumnIds()) {
    // all columns in the table?
    hash = HashUtil::CombineHashes(hash, HashUtil::Hash(&column_id));
  }

  // add the id of the index
  hash = HashUtil::CombineHashes(hash, HashUtil::Hash(&index_id_));

  // hash the type of index scan
  /* TODO: examine
   * 1. Hash type of index scan
   * 2. Hash predicate
   * 3. This hashes only 1 conjunction. Is that all we support? Should
   * assert that we only have one or properly handle as many as there are.
   */
  const index::ConjunctionScanPredicate *csp =
      &(index_predicate_.GetConjunctionList()[0]);
  if ( csp ) {
    // TODO - is it valid for csp to be null?
    auto is_point_query = csp->IsPointQuery();
    hash = HashUtil::CombineHashes(hash, HashUtil::Hash(&is_point_query));
    auto is_full_scan = csp->IsFullIndexScan();
    hash = HashUtil::CombineHashes(hash, HashUtil::Hash(&is_full_scan));
  }

  /*
  auto &index_predicate = GetIndexPredicate();
  (void) index_predicate;
  */
  
  auto is_update = IsForUpdate();
  hash = HashUtil::CombineHashes(hash, HashUtil::Hash(&is_update));

  hash = HashUtil::CombineHashes(hash, AbstractPlan::Hash());
  return false;
}

bool IndexScanPlan::operator==(const AbstractPlan &rhs) const {
  if (GetPlanNodeType() != rhs.GetPlanNodeType())
    return false;

  auto &other = static_cast<const planner::IndexScanPlan &>(rhs);
  auto *table = GetTable();
  auto *other_table = other.GetTable();
  PELOTON_ASSERT(table && other_table);
  if (*table != *other_table) return false;

  //if (GetIndex()->GetOid() != other.GetIndex()->GetOid()) return false;
  if (index_id_ != other.index_id_)
    return false;

  // Predicate
  auto *pred = GetPredicate();
  auto *other_pred = other.GetPredicate();

  if ((pred == nullptr && other_pred != nullptr) ||
      (pred != nullptr && other_pred == nullptr))
    return false;
  
  if (pred && (*pred != *other_pred))
    return false;

  // Column Ids
  size_t column_id_count = GetColumnIds().size();
  if (column_id_count != other.GetColumnIds().size())
    return false;
  for (size_t i = 0; i < column_id_count; i++) {
    if (GetColumnIds()[i] != other.GetColumnIds()[i])
      return false;
  }

  const index::ConjunctionScanPredicate *csp =
      &(index_predicate_.GetConjunctionList()[0]);
  const index::ConjunctionScanPredicate *other_csp =
      &(other.GetIndexPredicate().GetConjunctionList()[0]);
  
  if (csp->IsPointQuery() != other_csp->IsPointQuery())
    return false;
  
  if (csp->IsFullIndexScan() != other_csp->IsFullIndexScan())
    return false;

  if (IsForUpdate() != other.IsForUpdate())
    return false;

  return AbstractPlan::operator==(rhs);
}

}  // namespace planner
}  // namespace peloton
