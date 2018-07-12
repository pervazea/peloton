//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// index_scan_translator_test.cpp
//
// Identification: test/codegen/index_scan_translator_test.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/storage_manager.h"
#include "catalog/catalog.h"
#include "codegen/query_compiler.h"
#include "common/harness.h"
#include "concurrency/transaction_manager_factory.h"
#include "expression/conjunction_expression.h"
#include "expression/operator_expression.h"
#include "planner/index_scan_plan.h"
#include "planner/seq_scan_plan.h"
#include "index/index_factory.h"
#include <vector>
#include <set>
#include <algorithm>

#include "codegen/testing_codegen_util.h"

namespace peloton {
namespace test {

class IndexScanTranslatorTest : public PelotonCodeGenTest {
  std::string table_name_ = "table_with_index";

 public:
  IndexScanTranslatorTest() : PelotonCodeGenTest(), num_rows_to_insert_(64) {
    // Load test table, NOT USED in IndexScanTranslatorTests
    LoadTestTable(TestTableId(), num_rows_to_insert_);

    CreateAndLoadTableWithIndex();
  }

  void CreateAndLoadTableWithIndex() {
    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    auto *txn = txn_manager.BeginTransaction();
    auto *catalog = catalog::Catalog::GetInstance();

    const bool is_inlined = true;
    std::vector<catalog::Column> cols = {
      {type::TypeId::INTEGER,
       type::Type::GetTypeSize(type::TypeId::INTEGER),
       "COL_A", is_inlined},
      
      {type::TypeId::DECIMAL,
       type::Type::GetTypeSize(type::TypeId::DECIMAL),
       "COL_B", is_inlined},
      
      {type::TypeId::TIMESTAMP, 
       type::Type::GetTypeSize(type::TypeId::TIMESTAMP),
       "COL_C", is_inlined},
      
      {type::TypeId::VARCHAR, 25, "COL_D", !is_inlined}};
    
    std::unique_ptr<catalog::Schema> schema{new catalog::Schema(cols)};

    // Insert table in catalog
    catalog->CreateTable(txn, test_db_name, DEFAULT_SCHEMA_NAME,
                         std::move(schema), 
                         table_name_, false);
    txn_manager.CommitTransaction(txn);

    storage::DataTable *table = GetTableWithIndex();

    // add index
    // This holds column ID in the underlying table that are being indexed
    std::vector<oid_t> key_attrs;

    // This holds schema of the underlying table, which stays all the same
    // for all indices on the same underlying table
    auto tuple_schema = table->GetSchema();

    // This points to the schema of only columns indiced by the index
    // This is basically selecting tuple_schema() with key_attrs as index
    // but the order inside tuple schema is preserved - the order of schema
    // inside key_schema is not the order of real key
    catalog::Schema *key_schema;

    // This will be created for each index on the table
    // and the metadata is passed as part of the index construction paratemter
    // list
    index::IndexMetadata *index_metadata;

    /////////////////////////////////////////////////////////////////
    // Add index on column 0
    /////////////////////////////////////////////////////////////////
    key_attrs = {0};
    key_schema = catalog::Schema::CopySchema(tuple_schema, key_attrs);

    // This is not redundant
    // since the key schema always follows the ordering of the base table
    // schema, we need real ordering of the key columns
    key_schema->SetIndexedColumns(key_attrs);

    index_metadata = new index::IndexMetadata(
        "bwtree_index", 123, INVALID_OID, INVALID_OID, IndexType::BWTREE,
        IndexConstraintType::PRIMARY_KEY, tuple_schema, key_schema, key_attrs,
        false);

    std::shared_ptr<index::Index> pkey_index(
        index::IndexFactory::GetIndex(index_metadata));

    table->AddIndex(pkey_index);

    // populate the table
    txn = txn_manager.BeginTransaction();
    std::srand(std::time(nullptr));
    const bool allocate = true;
    auto testing_pool = TestingHarness::GetInstance().GetTestingPool();
    std::set<int> key_set;
    for (uint32_t rowid = 0; rowid < test_table_size_; rowid++) {
      storage::Tuple tuple(tuple_schema, allocate);

      int key = rowid;
      /*
      int key = std::rand();
      std::set<int>::iterator it = key_set.find(key);
      while (it != key_set.end()) {
        key = std::rand();
      }
      */
      key_set.insert(key);
      keys_.push_back(key);
      // LOG_INFO("key = %d", key);

      tuple.SetValue(0,
        type::ValueFactory::GetIntegerValue(key),
        testing_pool);
      
      tuple.SetValue(1,
        type::ValueFactory::GetDecimalValue((double)std::rand() / 10000.0),
        testing_pool);
      
      tuple.SetValue(2,
        type::ValueFactory::GetTimestampValue((int64_t)std::rand()),
        testing_pool);
      
      tuple.SetValue(3,
        type::ValueFactory::GetVarcharValue(std::to_string(std::rand())),
        testing_pool);

      ItemPointer *index_entry_ptr = nullptr;
      ItemPointer tuple_slot_id =
          table->InsertTuple(&tuple, txn, &index_entry_ptr);

      auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
      txn_manager.PerformInsert(txn, tuple_slot_id, index_entry_ptr);
    }

    txn_manager.CommitTransaction(txn);

    // sort the keys
    std::sort(keys_.begin(), keys_.end());
  }

  oid_t TestTableId() { return test_table_oids[0]; }

  storage::DataTable *GetTableWithIndex() const {
    //return *GetDatabase().GetTableWithName(table_name_);
    auto *catalog = catalog::Catalog::GetInstance();
    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    auto *txn = txn_manager.BeginTransaction();
    auto table = catalog->GetTableWithName(txn, test_db_name,
                                           DEFAULT_SCHEMA_NAME,
                                           table_name_);
    txn_manager.CommitTransaction(txn);
    return table;
  }

  uint32_t GetTestTableSize() { return test_table_size_; }

  int GetKey(uint32_t idx) {
    PELOTON_ASSERT(idx < test_table_size_);
    return keys_[idx];
  }

 private:
  // not used
  uint32_t num_rows_to_insert_ = 64;
  
  //uint32_t test_table_size_ = 1000;
  uint32_t test_table_size_ = 20;
  std::vector<int> keys_;
};

TEST_F(IndexScanTranslatorTest, IndexPointQuery) {
  //
  // SELECT a, b, c, d FROM table where a = x;
  //

  storage::DataTable *data_table = GetTableWithIndex();

  // Column ids to be added to logical tile after scan.
  std::vector<oid_t> column_ids({0, 1, 2, 3});

  // int key = GetKey(std::rand() % GetTestTableSize());
  // int key = 20;
  int key = 0;
  //===--------------------------------------------------------------------===//
  // ATTR 0 == key
  //===--------------------------------------------------------------------===//
  auto index = data_table->GetIndex(0);
  std::vector<oid_t> key_column_ids;
  std::vector<ExpressionType> expr_types;
  std::vector<type::Value> values;
  std::vector<expression::AbstractExpression *> runtime_keys;

  // only used in interpreted engine?
  key_column_ids.push_back(0);
  expr_types.push_back(ExpressionType::COMPARE_EQUAL);
  values.push_back(type::ValueFactory::GetIntegerValue(key).Copy());

  // Create index scan desc
  planner::IndexScanPlan::IndexScanDesc index_scan_desc(
      index->GetOid(), key_column_ids, expr_types, values, runtime_keys);

  // expression::AbstractExpression *predicate = nullptr;
  /*
  ExpressionPtr a_eq_20 =
    CmpEqExpr(ColRefExpr(type::TypeId::INTEGER, 0), ConstIntExpr(20));
  */
  ExpressionPtr a_eq_0 =
    CmpEqExpr(ColRefExpr(type::TypeId::INTEGER, 0), ConstIntExpr(0));
  

  // Create plan node.
  planner::IndexScanPlan scan(data_table, a_eq_0.release(), column_ids,
                              index_scan_desc);
  
  // Do binding
  planner::BindingContext context;
  scan.PerformBinding(context);

  // Printing consumer
  codegen::BufferingConsumer buffer{{0, 1, 2, 3}, context};

  // COMPILE and execute
  CompileAndExecute(scan, buffer);

  // Check that we got all the results
  const auto &results = buffer.GetOutputTuples();
  EXPECT_EQ(1, results.size());
}

TEST_F(IndexScanTranslatorTest, IndexRangeScan) {
  // PA - comment incorrect
  // SELECT a, b, c, d FROM table where a = x;
  //

  storage::DataTable *data_table = GetTableWithIndex();

  // Column ids to be added to logical tile after scan.
  std::vector<oid_t> column_ids({0, 1, 2, 3});

  //int key1_idx = std::rand() % (GetTestTableSize() / 2);
  //int key2_idx = std::rand() % (GetTestTableSize() / 2) + key1_idx;
  int key1_idx = 4;
  int key2_idx = 6;
  
  int key1 = GetKey(key1_idx);
  int key2 = GetKey(key2_idx);
  //===--------------------------------------------------------------------===//
  // ATTR 0 >= key1 and ATTR 0 <= key2
  //===--------------------------------------------------------------------===//
  auto index = data_table->GetIndex(0);
  std::vector<oid_t> key_column_ids;
  std::vector<ExpressionType> expr_types;
  std::vector<type::Value> values;
  std::vector<expression::AbstractExpression *> runtime_keys;

  key_column_ids.push_back(0);
  expr_types.push_back(ExpressionType::COMPARE_GREATERTHANOREQUALTO);
  values.push_back(type::ValueFactory::GetIntegerValue(key1).Copy());

  key_column_ids.push_back(0);
  expr_types.push_back(ExpressionType::COMPARE_LESSTHANOREQUALTO);
  values.push_back(type::ValueFactory::GetIntegerValue(key2).Copy());

  // Create index scan desc
  planner::IndexScanPlan::IndexScanDesc index_scan_desc(
      index->GetOid(), key_column_ids, expr_types, values, runtime_keys);

  expression::AbstractExpression *predicate = nullptr;

  // Create plan node.
  planner::IndexScanPlan scan(data_table, predicate, column_ids,
                              index_scan_desc);

  // Do binding
  planner::BindingContext context;
  scan.PerformBinding(context);

  // Printing consumer
  codegen::BufferingConsumer buffer{{0, 1, 2, 3}, context};

  // COMPILE and execute
  CompileAndExecute(scan, buffer);

  // Check that we got all the results
  const auto &results = buffer.GetOutputTuples();

  LOG_INFO("\nIndexRangeScan: Results");
  for (uint32_t ti = 0; ti < results.size(); ti++) {
    auto &tuple = results[ti];
    
    auto col0 = tuple.GetValue(0);
    std::string st = col0.GetInfo();
    LOG_INFO("tuple %d, col 0: %s", ti, st.c_str());
  }
  
  EXPECT_EQ(key2_idx - key1_idx + 1, results.size());  
}

TEST_F(IndexScanTranslatorTest, IndexFullScan) {
  //
  // SELECT a, b, c, d FROM table;
  //

  auto data_table = GetTableWithIndex();

  // Column ids to be added to logical tile after scan.
  std::vector<oid_t> column_ids({0, 1, 2, 3});

  auto index = data_table->GetIndex(0);
  std::vector<oid_t> key_column_ids;
  std::vector<ExpressionType> expr_types;
  std::vector<type::Value> values;
  std::vector<expression::AbstractExpression *> runtime_keys;

  // Create index scan desc
  planner::IndexScanPlan::IndexScanDesc index_scan_desc(
      index->GetOid(), key_column_ids, expr_types, values, runtime_keys);

  expression::AbstractExpression *predicate = nullptr;

  // Create plan node.
  planner::IndexScanPlan scan(data_table, predicate, column_ids,
                              index_scan_desc);

  // Do binding
  planner::BindingContext context;
  scan.PerformBinding(context);

  // Printing consumer
  codegen::BufferingConsumer buffer{{0, 1, 2, 3}, context};

  // COMPILE and execute
  CompileAndExecute(scan, buffer);

  // Check that we got all the results
  const auto &results = buffer.GetOutputTuples();
  EXPECT_EQ(GetTestTableSize(), results.size());
}

}  // namespace test
}  // namespace peloton
