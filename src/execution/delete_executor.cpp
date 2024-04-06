//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <algorithm>
#include <memory>

#include "common/rid.h"
#include "execution/executors/delete_executor.h"
#include "type/type.h"
#include "type/type_id.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
  table_index_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
  child_executor_->Init();
}

void DeleteExecutor::Init() {}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (is_end_) {
    return false;
  }
  Tuple delete_tuple{};
  RID dele_rid;
  int count = 0;
  while (child_executor_->Next(&delete_tuple, &dele_rid)) {
    bool deleted = table_info_->table_->MarkDelete(dele_rid, exec_ctx_->GetTransaction());
    if (deleted) {
      std::for_each(table_index_.begin(), table_index_.end(),
                    [&delete_tuple, &rid, &table_info = table_info_, &exec_ctx = exec_ctx_](IndexInfo *index) {
                      index->index_->DeleteEntry(delete_tuple.KeyFromTuple(table_info->schema_, index->key_schema_,
                                                                           index->index_->GetKeyAttrs()),
                                                 *rid, exec_ctx->GetTransaction());
                    });
      count++;
    }
  }
  std::vector<Value> valuses;
  valuses.reserve(GetOutputSchema().GetColumnCount());
  valuses.emplace_back(TypeId::INTEGER, count);
  *tuple = Tuple{valuses, &GetOutputSchema()};
  is_end_ = true;
  return true;
}

}  // namespace bustub
