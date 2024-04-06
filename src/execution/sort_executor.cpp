#include "execution/executors/sort_executor.h"
#include "binder/bound_order_by.h"
#include "storage/table/tuple.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_(std::move(child_executor)) {}

void SortExecutor::Init() {
  Tuple tuple;
  RID rid;
  child_->Init();
  while (child_->Next(&tuple, &rid)) {
    sorted_tuple_.push_back(tuple);
  }
  std::sort(sorted_tuple_.begin(), sorted_tuple_.end(), [this](const Tuple &a, const Tuple &b) {
    for (auto [order_by_type, expr] : plan_->GetOrderBy()) {
      bool asc_flag = (order_by_type == OrderByType::DEFAULT || order_by_type == OrderByType::ASC);
      if (asc_flag) {
        if (expr->Evaluate(&a, child_->GetOutputSchema())
                .CompareLessThan(expr->Evaluate(&b, child_->GetOutputSchema())) == CmpBool::CmpTrue) {
          return true;
        }
        if (expr->Evaluate(&a, child_->GetOutputSchema())
                .CompareGreaterThan(expr->Evaluate(&b, child_->GetOutputSchema())) == CmpBool::CmpTrue) {
          return false;
        }
      } else {
        if (expr->Evaluate(&a, child_->GetOutputSchema())
                .CompareLessThan(expr->Evaluate(&b, child_->GetOutputSchema())) == CmpBool::CmpTrue) {
          return false;
        }
        if (expr->Evaluate(&a, child_->GetOutputSchema())
                .CompareGreaterThan(expr->Evaluate(&b, child_->GetOutputSchema())) == CmpBool::CmpTrue) {
          return true;
        }
      }
    }
    return false;
  });
  iterator_ = sorted_tuple_.begin();
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (iterator_ != sorted_tuple_.end()) {
    *tuple = *iterator_;
    // *rid=iterator_->GetRid();
    iterator_++;
    return true;
  }
  return false;
}

}  // namespace bustub
