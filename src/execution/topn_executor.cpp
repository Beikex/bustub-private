#include "execution/executors/topn_executor.h"
#include <memory>
#include <queue>
#include <vector>
#include "binder/bound_order_by.h"
#include "storage/table/tuple.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_(std::move(child_executor)) {}

void TopNExecutor::Init() {
  child_->Init();
  auto cmp = [ordet_bys = plan_->order_bys_, schema = child_->GetOutputSchema()](const Tuple &a, const Tuple &b) {
    for (const auto &order_by : ordet_bys) {
      switch (order_by.first) {
        case OrderByType::INVALID:
        case OrderByType::DEFAULT:
        case OrderByType::ASC:
          if (static_cast<bool>(
                  order_by.second->Evaluate(&a, schema).CompareLessThan(order_by.second->Evaluate(&b, schema)))) {
            return true;
          } else if (static_cast<bool>(order_by.second->Evaluate(&a, schema)
                                           .CompareGreaterThan(order_by.second->Evaluate(&b, schema)))) {
            return false;
          }
          break;
        case OrderByType::DESC:
          if (static_cast<bool>(
                  order_by.second->Evaluate(&a, schema).CompareLessThan(order_by.second->Evaluate(&b, schema)))) {
            return false;
          } else if (static_cast<bool>(order_by.second->Evaluate(&a, schema)
                                           .CompareGreaterThan(order_by.second->Evaluate(&b, schema)))) {
            return true;
          }
          break;
      }
    }
    return false;
  };
  std::priority_queue<Tuple, std::vector<Tuple>, decltype(cmp)> pq(cmp);
  Tuple child_tuple{};
  RID chile_rid;
  while (child_->Next(&child_tuple, &chile_rid)) {
    pq.push(child_tuple);
    if (pq.size() > plan_->GetN()) {
      pq.pop();
    }
  }
  while (!pq.empty()) {
    child_tuples_.push(pq.top());
    pq.pop();
  }
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (child_tuples_.empty()) {
    return false;
  }
  *tuple = child_tuples_.top();
  *rid = tuple->GetRid();
  child_tuples_.pop();
  return true;
}

}  // namespace bustub
