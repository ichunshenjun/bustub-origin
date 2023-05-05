//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "storage/page/b_plus_tree_page.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageType(IndexPageType::LEAF_PAGE);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetMaxSize(max_size);
  SetNextPageId(INVALID_PAGE_ID);
  SetSize(0);
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t { 
  return next_page_id_; 
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) {
  this->next_page_id_=next_page_id;
}

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  // replace with your own code
  return array_[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
  array_[index].first=key;
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::ValueAt(int index) const -> ValueType { 
  return array_[index].second; 
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetValueAt(int index, const ValueType &value) {
  array_[index].second=value;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value,KeyComparator &comparator)->bool{
  int left=0;
  int right=GetSize()-1;
  if(GetSize()==0){
    SetKeyAt(0,key);
    SetValueAt(0,value);
    IncreaseSize(1);
    return true;
  }
  if(GetSize()==1){
    if(comparator(key,KeyAt(left))<0){
      SetKeyAt(1,KeyAt(0));
      SetValueAt(1,ValueAt(0));
      SetKeyAt(left,key);
      SetValueAt(left,value);
      IncreaseSize(1);
    }
    else if(comparator(key,KeyAt(left))>0){
      SetKeyAt(1,key);
      SetValueAt(1,value);
      IncreaseSize(1);
    }
    else if(comparator(key,KeyAt(left))==0){
      return false;
    }
    return true;
  }
  while(left<=right){
    int mid=left+(right-left)/2;
    if(comparator(key,KeyAt(mid))<0){
      right=mid-1;
    }
    else if(comparator(key,KeyAt(mid))>0){
      left=mid+1;
    }
    else if(comparator(key,KeyAt(mid))==0){
      return false;
    }
  }
  for(int i=GetSize();i>=left+1;i--){
    SetKeyAt(i,KeyAt(i-1));
    SetValueAt(i,ValueAt(i-1));
  }
  SetKeyAt(left,key);
  SetValueAt(left,value);
  IncreaseSize(1);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveTo(BPlusTreeLeafPage* other_node){
  for(int i=GetMaxSize()/2;i<GetMaxSize();i++){
    other_node->SetKeyAt(i-GetMaxSize()/2,KeyAt(i));
    other_node->SetValueAt(i-GetMaxSize()/2, ValueAt(i));
    other_node->IncreaseSize(1);
    this->IncreaseSize(-1);
  }
}
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyIndex(const KeyType &key,KeyComparator &comparator)->int{
  for(int i=0;i<GetSize();i++){
    if(comparator(array_[i].first,key)==0){
      return i;
    }
  }
  return -1;
}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
