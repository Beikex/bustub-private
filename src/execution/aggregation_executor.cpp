//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <utility>
#include <vector>

#include "catalog/schema.h"
#include "execution/executors/aggregation_executor.h"
#include "storage/table/tuple.h"
#include "type/type.h"
#include "type/type_id.h"
#include "type/value_factory.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_(std::move(child)),
      aht_(plan->GetAggregates(), plan->GetAggregateTypes()),
      aht_iterator_(aht_.Begin()) {}

void AggregationExecutor::Init() {
  child_->Init();
  Tuple tuple;
  RID rid;
  while (child_->Next(&tuple, &rid)) {
    auto aggre_key = MakeAggregateKey(&tuple);
    auto aggre_val = MakeAggregateValue(&tuple);
    aht_.InsertCombine(aggre_key, aggre_val);
  }
  aht_iterator_ = aht_.Begin();
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  Schema sche(plan_->OutputSchema());
  if (aht_iterator_ != aht_.End()) {
    std::vector<Value> value(aht_iterator_.Key().group_bys_);
    for (const auto &aggre : aht_iterator_.Val().aggregates_) {
      value.emplace_back(aggre);
    }
    *tuple = {value, &sche};
    ++aht_iterator_;
    successful_ = true;
    return true;
  }
  if (!successful_) {
    successful_ = true;
    if (plan_->group_bys_.empty()) {
      std::vector<Value> value;
      for (auto aggre : plan_->agg_types_) {
        switch (aggre) {
          case AggregationType::CountStarAggregate:
            value.push_back(ValueFactory::GetIntegerValue(0));
            break;
          case AggregationType::CountAggregate:
          case AggregationType::SumAggregate:
          case AggregationType::MinAggregate:
          case AggregationType::MaxAggregate:
            value.push_back(ValueFactory::GetNullValueByType(TypeId::INTEGER));
            break;
        }
      }
      *tuple = {value, &sche};
      successful_ = true;
      return true;
    }
    return false;
  }
  return false;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_.get(); }

}  // namespace bustub
