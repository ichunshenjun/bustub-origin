//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "buffer/buffer_pool_manager.h"
#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetMaxSize(max_size);
  SetSize(0);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  // replace with your own code
  return array_[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) { array_[index].first = key; }

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType { return array_[index].second; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetValueAt(int index, const ValueType &value) { array_[index].second = value; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::FindKey(const KeyType &key, KeyComparator &comparator) -> ValueType {
  int left = 1;
  int right = GetSize() - 1;
  if (comparator(key, KeyAt(1)) < 0) {
    return ValueAt(0);
  }
  while (left <= right) {
    int mid = left + (right - left) / 2;
    if (comparator(key, KeyAt(mid)) < 0) {
      right = mid - 1;
    } else if (comparator(key, KeyAt(mid)) > 0) {
      left = mid + 1;
    } else if (comparator(key, KeyAt(mid)) == 0) {
      return ValueAt(mid);
    }
  }
  return ValueAt(right);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) -> int {
  for (int i = 0; i < GetSize(); i++) {
    if (ValueAt(i) == value) {
      return i;
    }
  }
  return -1;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyIndex(const KeyType &key, KeyComparator &comparator) -> int {
  for (int i = 0; i < GetSize(); i++) {
    if (comparator(array_[i].first, key) == 0) {
      return i;
    }
  }
  return -1;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Insert(int index, const KeyType &key, const ValueType &value) {
  for (int i = GetSize(); i >= index + 1; i--) {
    SetKeyAt(i, KeyAt(i - 1));
    SetValueAt(i, ValueAt(i - 1));
  }
  SetKeyAt(index + 1, key);
  SetValueAt(index + 1, value);
  IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveTo(BPlusTreeInternalPage *other_node, BufferPoolManager *bpm) {
  for (int i = GetMinSize(); i < GetMaxSize() + 1; i++) {
    other_node->SetKeyAt(i - GetMinSize(), KeyAt(i));
    other_node->SetValueAt(i - GetMinSize(), ValueAt(i));
    auto child_node = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(ValueAt(i))->GetData());
    child_node->SetParentPageId(other_node->GetPageId());
    other_node->IncreaseSize(1);
    this->IncreaseSize(-1);
    bpm->UnpinPage(child_node->GetPageId(), true);
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Delete(const KeyType &key, KeyComparator &comparator) -> bool {
  int index = KeyIndex(key, comparator);
  if (index == -1) {
    return false;
  }
  for (int i = index; i <= GetSize() - 2; i++) {
    array_[i] = array_[i + 1];
  }
  IncreaseSize(-1);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveFrom(BPlusTreeInternalPage *other_node, BufferPoolManager *bpm) {
  int size = GetSize();
  for (int i = 0; i < other_node->GetSize(); i++) {
    SetKeyAt(i + size, other_node->KeyAt(i));
    SetValueAt(i + size, other_node->ValueAt(i));
    auto child_node = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(ValueAt(i + size))->GetData());
    child_node->SetParentPageId(this->GetPageId());
    IncreaseSize(1);
    other_node->IncreaseSize(-1);
    bpm->UnpinPage(child_node->GetPageId(), true);
  }
}
// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
