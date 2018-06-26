//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// abstract_plan_context_proxy.h
//
// Identification: src/include/codegen/proxy/abstract_plan_context_proxy.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/proxy/proxy.h"
#include "planner/abstract_plan.h"

namespace peloton {
namespace codegen {

PROXY(AbstractPlan) {
  DECLARE_MEMBER(0, char[sizeof(planner::AbstractPlan)], opaque);
  DECLARE_TYPE;
};

TYPE_BUILDER(AbstractPlan, planner::AbstractPlan);

}  // namespace codegen
}  // namespace peloton
