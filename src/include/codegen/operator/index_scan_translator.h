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
  
 private:  
  // Plan accessor
  const planner::IndexScanPlan &GetIndexScanPlan() const
  { return index_scan_; }

  // Table accessor
  const storage::DataTable &GetTable() const;

  // Update the tuples stored in index scan plan with new parameters
  // void SetIndexPredicate(CodeGen &codegen, llvm::Value *iterator_ptr) const;

  // Plan accessor
  const planner::IndexScanPlan &GetScanPlan() const { return index_scan_; }
  
 private:
  // The scan
  const planner::IndexScanPlan &index_scan_;

  // The code-generating table instance
  codegen::Table table_;
};

}  // namespace codegen
}  // namespace peloton
