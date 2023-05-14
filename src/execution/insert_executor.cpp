//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"
#include "storage/table/tuple.h"
#include "type/type_id.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),plan_(plan),child_executor_(std::move(child_executor)) {
        table_info_=GetExecutorContext()->GetCatalog()->GetTable(plan_->TableOid());
    }

void InsertExecutor::Init() { 
    // throw NotImplementedException("InsertExecutor is not implemented"); 
    child_executor_->Init();
    table_indexes_=GetExecutorContext()->GetCatalog()->GetTableIndexes(table_info_->name_);
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool { 
    if(is_end_){
        return false;
    }
    cnt_=0;
    while(child_executor_->Next(tuple,rid)){
        table_info_->table_->InsertTuple(*tuple, rid, exec_ctx_->GetTransaction());
        for(auto index_info:table_indexes_){
            auto key=tuple->KeyFromTuple(table_info_->schema_, index_info->key_schema_,index_info->index_->GetKeyAttrs());
            index_info->index_->InsertEntry(key, *rid, exec_ctx_->GetTransaction());
        }
        cnt_++;
    }
    *tuple=Tuple{std::vector<Value>{Value{TypeId::INTEGER,cnt_}},&GetOutputSchema()};
    is_end_=true;
    return true; 
}

}  // namespace bustub
