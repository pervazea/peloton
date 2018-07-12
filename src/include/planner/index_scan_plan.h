//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// index_scan_plan.h
//
// Identification: src/include/planner/index_scan_plan.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <string>
#include <vector>

#include "common/internal_types.h"
#include "expression/abstract_expression.h"
#include "index/scan_optimizer.h"
#include "planner/abstract_scan_plan.h"
#include "storage/data_table.h"
#include "storage/tuple.h"

namespace peloton {

namespace index {
class Index;
}

namespace storage {
class Tuple;
}

namespace planner {

class IndexScanPlan : public AbstractScan {
 public:
  /*
   * class IndexScanDesc - Stores information to do the index scan
   */
  struct IndexScanDesc {
    /*
     * Default Constructor - Set index pointer to empty
     *
     * We need to do this since this might be created even when an index
     * is not required, e.g. inside hybrid scan
     */
    IndexScanDesc() : index_id{INVALID_OID} {}

    /**
     * @brief IndexScanDesc Constructor - index scan descriptor
     *        passes information relating to initialization and 
     *        execution of an index scan from the planner/optimizer to the 
     *        index scan plan. When no index is required, this must still be 
     *        called to set default values.
     * 
     *        The 3 vectors, p_tuple_column_id_list, expr_list_p and 
     *        p_value_list are identical in size. 
     * 
     * @param[in] - p_tuple_column_id_list - ids for indexed columns
     * @param[in] - expr_list_p - (predicate) expressions
     * @param[in] - p_value_list - actual values or placeholders 
     *                             (PARAMETER_VALUES) to be filled in
     *                             by SetParameterValues.
     * @param[in] - runtime_key_list ?
     */
    IndexScanDesc(
        oid_t p_index_id,
        const std::vector<oid_t> &p_tuple_column_id_list,
        const std::vector<ExpressionType> &expr_list_p,
        const std::vector<type::Value> &p_value_list,
        const std::vector<expression::AbstractExpression *> &p_runtime_key_list)
        : index_id(p_index_id),
          tuple_column_id_list(p_tuple_column_id_list),
          expr_list(expr_list_p),
          value_list(p_value_list),
          runtime_key_list(p_runtime_key_list) {}

    ~IndexScanDesc() {
      // for (auto val : value_list)
      //  delete val;
    }

    // The index object for scanning
    //
    // NOTE: For hybrid scan plans, even if there is no index required
    // for a scan, an empty scan descriptor will still be passed in as
    // argument. This is a bad design but currently we have to live with it
    // In order to prevent the scan predicate optimizer from trying to
    // optimizing the index scan while the index pointer is not valid
    // this should be set to INVALID_OID for an empty initialization
    oid_t index_id;

    // A list of columns id in the base table that has a scan predicate
    // (only for indexed column in the base table)
    std::vector<oid_t> tuple_column_id_list;

    // A list of expressions
    std::vector<ExpressionType> expr_list;

    // A list of values either bound or unbound (also referred to as
    // late binding or PARAMETER_VALUEs)
    std::vector<type::Value> value_list;

    // ???
    std::vector<expression::AbstractExpression *> runtime_key_list;
  };

  ///////////////////////////////////////////////////////////////////
  // Members of IndexScanPlan
  ///////////////////////////////////////////////////////////////////

  /**
   * @brief Constructor an index scan plan
   *
   * @param[in] table - for the index
   * @param[in] predicate - the full predicate, i.e. index columns as
   *                        well as non-index columns
   * @param[in] column_ids - indexed columns?
   * @param[in] index_scan_desc - expressions, column ids and values,
   *                              for the index predicate.
   * @param[in] for_update_flag
   * 
   */
  IndexScanPlan(storage::DataTable *table,
                expression::AbstractExpression *predicate,
                const std::vector<oid_t> &column_ids,
                const IndexScanDesc &index_scan_desc,
                bool for_update_flag = false);

  ~IndexScanPlan() {
    for (auto expr : runtime_keys_) {
      delete expr;
    }
    LOG_TRACE("Destroyed a index scan plan!");
  }

  oid_t GetIndexId() const { return index_id_; }

  const std::vector<oid_t> &GetColumnIds() const { return column_ids_; }

  const std::vector<oid_t> &GetKeyColumnIds() const { return key_column_ids_; }

  const std::vector<ExpressionType> &GetExprTypes() const {
    return expr_types_;
  }

  const index::IndexScanPredicate &GetIndexPredicate() const {
    return index_predicate_;
  }

  void SetIndexPredicate(index::Index *index_p);

  const std::vector<type::Value> &GetValues() const { return values_; }

  const std::vector<expression::AbstractExpression *> &GetRunTimeKeys() const {
    return runtime_keys_;
  }

  inline PlanNodeType GetPlanNodeType() const {
    return PlanNodeType::INDEXSCAN;
  }

  inline bool GetLeftOpen() const { return left_open_; }

  inline bool GetRightOpen() const { return right_open_; }

  inline bool GetLimit() const { return limit_; }

  inline int64_t GetLimitNumber() const { return limit_number_; }

  inline int64_t GetLimitOffset() const { return limit_offset_; }

  inline bool GetDescend() const { return descend_; }

  const std::string GetInfo() const;

  void SetLimit(bool limit) { limit_ = limit; }

  void SetLimitNumber(int64_t limit) { limit_number_ = limit; }

  void SetLimitOffset(int64_t offset) { limit_offset_ = offset; }

  void SetDescend(bool descend) { descend_ = descend; }

  void SetParameterValues(std::vector<type::Value> *values);

  std::unique_ptr<AbstractPlan> Copy() const {
    std::vector<expression::AbstractExpression *> new_runtime_keys;
    for (auto *key : runtime_keys_) {
      new_runtime_keys.push_back(key->Copy());
    }

    IndexScanDesc desc(index_id_, key_column_ids_, expr_types_, values_,
                       new_runtime_keys);

    IndexScanPlan *new_plan = new IndexScanPlan(
        GetTable(), GetPredicate()->Copy(), GetColumnIds(), desc, false);
    return std::unique_ptr<AbstractPlan>(new_plan);
  }


  /**
   * Creates a hash value for the plan. Used by the query cache (which
   * contains compiled (codegen) plan code) to determine if the cached,
   * compiled version of the plan may be used, or if recompilation is
   * required.
   *
   * @return hash value for plan
   */
  hash_t Hash() const override;

  bool operator==(const AbstractPlan &rhs) const override;
  bool operator!=(const AbstractPlan &rhs) const override {
    return !(*this == rhs);
  }

  void PerformBinding(BindingContext &binding_context) override;  

  virtual void VisitParameters(
    codegen::QueryParametersMap &map,
    std::vector<peloton::type::Value> &values,
    const std::vector<peloton::type::Value> &values_from_user) override;

 private:
  /** @brief index associated with index scan. */
  oid_t index_id_;

  // IDs of indexed columns. Provided by the optimizer via IndexScanDesc
  const std::vector<oid_t> key_column_ids_;

  // Provided by the optimizer via IndexScanDesc
  const std::vector<ExpressionType> expr_types_;

  // The life time of the scan predicate is as long as the lifetime
  // of this array, since we always use the values in this array to
  // construct low key and high key for the scan plan, it should stay
  // valid until the scan plan is destructed
  //
  // Note that when binding values to the scan plan we copy those values
  // into this array, which means the lifetime of values being bound is
  // also the lifetime of the IndexScanPlan object
  std::vector<type::Value> values_;
  
  // PARAMETER_VALUEs. Actual values provided later by SetParameterValues
  std::vector<type::Value> values_with_params_;

  const std::vector<expression::AbstractExpression *> runtime_keys_;

  // TODO: correct comment
  // Currently we just support single conjunction predicate
  //
  // In the future this might be extended into an array of conjunctive
  // predicates connected by disjunction
  index::IndexScanPredicate index_predicate_;
  
  // whether the index scan range is left open
  bool left_open_ = false;

  // whether the index scan range is right open
  bool right_open_ = false;

  // whether it is an order by + limit plan
  bool limit_ = false;

  // how many tuples should be returned
  int64_t limit_number_ = 0;

  // offset means from which point
  int64_t limit_offset_ = 0;

  // whether order by is descending
  bool descend_ = false;

 private:
  DISALLOW_COPY_AND_MOVE(IndexScanPlan);
};

}  // namespace planner
}  // namespace peloton
