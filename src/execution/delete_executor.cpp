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

#include <memory>

#include "execution/executors/delete_executor.h"
#include "type/value.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),plan_(plan),child_executor_(std::move(child_executor)) {
        table_info_=exec_ctx->GetCatalog()->GetTable(plan->TableOid());
    }

void DeleteExecutor::Init() {
    child_executor_->Init();
    table_indexes_=exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool { 
    if(is_end_){
        return false;
    }
    while(child_executor_->Next(tuple, rid)){
        table_info_->table_->MarkDelete(*rid, exec_ctx_->GetTransaction());
        for(auto index_info:table_indexes_){
            auto key=tuple->KeyFromTuple(table_info_->schema_, index_info->key_schema_,index_info->index_->GetKeyAttrs());
            index_info->index_->DeleteEntry(key, *rid, exec_ctx_->GetTransaction());
        }
        cnt_++;
    }
    *tuple=Tuple{std::vector<Value>{Value{TypeId::INTEGER,cnt_}},&GetOutputSchema()};
    is_end_=true;
    return true; 
    
}

}  // namespace bustub
