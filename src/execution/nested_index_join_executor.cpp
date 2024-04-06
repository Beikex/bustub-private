//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.cpp
//
// Identification: src/execution/nested_index_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_index_join_executor.h"
#include <cstdint>
#include <vector>
#include "binder/table_ref/bound_join_ref.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

NestIndexJoinExecutor::NestIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
  is_left_ = (plan_->GetJoinType() == JoinType::LEFT);
  index_info_ = exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid());
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetInnerTableOid());
}

void NestIndexJoinExecutor::Init() { child_executor_->Init(); }

auto NestIndexJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  Tuple left_tuple;
  RID left_rid;
  while (child_executor_->Next(&left_tuple, &left_rid)) {
    auto key_schema = index_info_->index_->GetKeySchema();
    auto value = plan_->KeyPredicate()->Evaluate(&left_tuple, child_executor_->GetOutputSchema());
    std::vector<Value> values;
    values.push_back(value);
    Tuple key(values, key_schema);
    std::vector<RID> results;
    index_info_->index_->ScanKey(key, &results, exec_ctx_->GetTransaction());
    if (!results.empty()) {
      for (auto rid_b : results) {
        Tuple rigth_tuple;
        if (table_info_->table_->GetTuple(rid_b, &rigth_tuple, exec_ctx_->GetTransaction())) {
          std::vector<Value> tuple_values;
          for (uint32_t i = 0; i < child_executor_->GetOutputSchema().GetColumnCount(); i++) {
            tuple_values.push_back(left_tuple.GetValue(&child_executor_->GetOutputSchema(), i));
          }
          for (uint32_t i = 0; i < table_info_->schema_.GetColumnCount(); i++) {
            tuple_values.push_back(rigth_tuple.GetValue(&table_info_->schema_, i));
          }
          *tuple = {tuple_values, &plan_->OutputSchema()};
          return true;
        }
      }
    }
    if (is_left_) {
      std::vector<Value> tuple_values;
      for (uint32_t i = 0; i < child_executor_->GetOutputSchema().GetColumnCount(); i++) {
        tuple_values.push_back(left_tuple.GetValue(&child_executor_->GetOutputSchema(), i));
      }
      for (uint i = 0; i < table_info_->schema_.GetColumnCount(); i++) {
        tuple_values.push_back(ValueFactory::GetNullValueByType(table_info_->schema_.GetColumn(i).GetType()));
      }
      *tuple = {tuple_values, &plan_->OutputSchema()};
      return true;
    }
  }
  return false;
}

}  // namespace bustub
