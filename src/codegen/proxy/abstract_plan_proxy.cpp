//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// abstract_plan_proxy.cpp
//
// Identification: src/codegen/proxy/abstract_plan_proxy.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/proxy/abstract_plan_proxy.h"

namespace peloton {
namespace codegen {

DEFINE_TYPE(AbstractPlan, "planner::AbstractPlan", opaque);

}  // namespace codegen
}  // namespace peloton
