/**
 * index_iterator.cpp
 */
#include <cassert>

#include "common/config.h"
#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator() { this->leaf_node_ = nullptr; }

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *leaf_node, int pos)
    : leaf_node_(leaf_node), pos_(pos) {}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() = default;  // NOLINT

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool {
  return static_cast<bool>(leaf_node_->GetNextPageId() == INVALID_PAGE_ID && pos_ == leaf_node_->GetSize() - 1);
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & { return leaf_node_->ArrayAt(pos_); }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  if ((pos_ == leaf_node_->GetSize() - 1 && leaf_node_->GetNextPageId() == INVALID_PAGE_ID) ||
      pos_ + 1 <= leaf_node_->GetSize() - 1) {
    pos_++;
  } else {
    auto leaf_next_node = reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(
        buffer_pool_manager_->FetchPage(leaf_node_->GetNextPageId())->GetData());
    leaf_node_ = leaf_next_node;
    pos_ = 0;
  }
  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
