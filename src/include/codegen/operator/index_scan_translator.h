//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// index_scan_translator.h
//
// Identification: src/include/codegen/operator/index_scan_translator.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/compilation_context.h"
#include "codegen/consumer_context.h"
#include "codegen/operator/operator_translator.h"
#include "codegen/scan_callback.h"
#include "index/index.h"
#include "codegen/table.h"

namespace peloton {

namespace planner {
class IndexScanPlan;
}  // namespace planner

namespace index {
class Index;
}  // namespace index

namespace storage {
class DataTable;
}  // namespace storage

namespace codegen {

//===----------------------------------------------------------------------===//
// A translator for index scans
//===----------------------------------------------------------------------===//
class IndexScanTranslator : public OperatorTranslator {
 public:
  // Constructor
  IndexScanTranslator(const planner::IndexScanPlan &index_scan,
                      CompilationContext &context, Pipeline &pipeline);

  void InitializeQueryState() override {}

  // Index scans don't rely on any auxiliary functions
  void DefineAuxiliaryFunctions() override {}

  // The method that produces new tuples
  void Produce() const override;

  // Scans are leaves in the query plan and, hence, do not consume tuples
  void Consume(ConsumerContext &, RowBatch &) const override {}
  void Consume(ConsumerContext &, RowBatch::Row &) const override {}

  // Similar to InitializeState(), index scans don't have any state
  void TearDownQueryState() override {}

  // Get a stringified version of this translator
  std::string GetName() const;

 private:
  // Helper class declarations (defined in implementation)
  class AttributeAccess;
  class ScanConsumer;
  /*
  //-----------------
  // An attribute accessor that uses the backing tile group to access columns
  //-----------------
  class AttributeAccess : public RowBatch::AttributeAccess {
   public:
    // Constructor
    AttributeAccess(const TileGroup::TileGroupAccess &access,
                    const planner::AttributeInfo *ai);

    // Access an attribute in the given row
    codegen::Value Access(CodeGen &codegen, RowBatch::Row &row) override;

    const planner::AttributeInfo *GetAttributeRef() const { return ai_; }

   private:
    // The accessor we use to load column values
    const TileGroup::TileGroupAccess &tile_group_access_;
    // The attribute we will access
    const planner::AttributeInfo *ai_;
  };
 
  //-----------------
  // Scans the items specified from the index
  //-----------------
 
  class ScanConsumer : public codegen::ScanCallback {
   public:
    // Constructor
    ScanConsumer(const IndexScanTranslator &translator,
                 Vector &selection_vector);

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
    const expression::AbstractExpression *GetPredicate() const;

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

    llvm::Value *SIMDFilterRows(RowBatch &batch,
                                const TileGroup::TileGroupAccess &access) const;

   private:
    // The translator instance the consumer is generating code for
    const IndexScanTranslator &translator_;

    // The selection vector used for vectorized scans
    Vector &selection_vector_;

    // The current tile group id we're scanning over
    llvm::Value *tile_group_id_;

    // The current tile group we're scanning over
    llvm::Value *tile_group_ptr_;
  };
  */

 private:  
  // Plan accessor
  const planner::IndexScanPlan &GetIndexScanPlan() const
  { return index_scan_; }

  // Table accessor
  const storage::DataTable &GetTable() const;

  /*
  // Index accessor
  const index::Index &GetIndex() const;
  */

  // Update the tuples stored in index scan plan with new parameters
  void SetIndexPredicate(CodeGen &codegen, llvm::Value *iterator_ptr) const;

  /*
  void FilterTuplesByPredicate(CodeGen &codegen,
                               Vector &sel_vec,
                               TileGroup::TileGroupAccess &tile_group_access,
                               llvm::Value *tile_group_id) const;
  */

  /* -----
   * helper functions - consider making common
   * -----
   */
  /*
  void SetupRowBatch(RowBatch &batch,
                     TileGroup::TileGroupAccess &tile_group_access,
                     std::vector<AttributeAccess> &access) const;
  */

  // Plan accessor
  const planner::IndexScanPlan &GetScanPlan() const { return index_scan_; }
  
 private:
  // The scan
  const planner::IndexScanPlan &index_scan_;

  // index::IndexScanPredicate index_predicate_;  

  // The ID of the selection vector in runtime state
  //  RuntimeState::StateID selection_vector_id_;

  // The code-generating table instance
  codegen::Table table_;
};

}  // namespace codegen
}  // namespace peloton
