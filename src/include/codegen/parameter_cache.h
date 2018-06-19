//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// parameter_cache.h
//
// Identification: src/include/codegen/parameter_cache.h
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/codegen.h"
#include "codegen/query_parameters_map.h"
#include "codegen/value.h"
#include "expression/parameter.h"
#include "type/type.h"

namespace peloton {

namespace expression {
class AbstractExpression;
}  // namespace expression

namespace codegen {

//===----------------------------------------------------------------------===//
// A parameter cache which stores the parameterized values for codegen runtime
//===----------------------------------------------------------------------===//
class ParameterCache {
 public:
  // Constructor
  explicit ParameterCache(const QueryParametersMap &map)
      : parameters_map_(map) {}

  /**
   *  Set values for all the parameters
   */
  void Populate(CodeGen &codegen, llvm::Value *query_parameters_ptr);

  /**
   * @brief Return the cached value, specified by index
   * 
   * @param  index
   * @return value from index location
   */
  codegen::Value GetValue(uint32_t index) const;

  /**
   * @brief Return the cached value, specified by expression
   * 
   * @param  expr to look up
   * @return value for the supplied expr
   */
  codegen::Value GetValue(const expression::AbstractExpression *expr) const;

  /** 
   * Clear all cached parameter values
   */
  void Reset();
  
  const QueryParametersMap &GetQueryParametersMap() const {
    return parameters_map_;
  }

 private:
  static codegen::Value DeriveParameterValue(CodeGen &codegen,
                                             llvm::Value *query_parameters_ptr,
                                             uint32_t index,
                                             peloton::type::TypeId type_id,
                                             bool is_nullable);

 private:
  // Parameter information
  const QueryParametersMap &parameters_map_;

  // Parameter value storage
  std::vector<codegen::Value> values_;
};

}  // namespace codegen
}  // namespace peloton
