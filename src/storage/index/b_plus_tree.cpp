#include <string>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/b_plus_tree_page.h"
#include "storage/page/header_page.h"
#include "storage/page/page.h"

namespace bustub {
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool { return root_page_id_ == INVALID_PAGE_ID; }
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) -> bool {
  auto leaf_node = FindLeafPage(key);
  int index = leaf_node->KeyIndex(key, comparator_);
  result->emplace_back(leaf_node->ValueAt(index));
  buffer_pool_manager_->UnpinPage(leaf_node->GetPageId(), false);
  return index >= 0;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool {
  if (IsEmpty()) {
    auto root_node = reinterpret_cast<LeafPage *>(buffer_pool_manager_->NewPage(&root_page_id_)->GetData());
    root_node->Init(root_page_id_, INVALID_PAGE_ID, leaf_max_size_);
    UpdateRootPageId(1);
    auto success = static_cast<bool>(root_node->Insert(key, value, comparator_));
    buffer_pool_manager_->UnpinPage(root_node->GetPageId(), true);
    return success;
  }
  auto leaf_node = FindLeafPage(key);
  auto success = leaf_node->Insert(key, value, comparator_);
  if (leaf_node->GetSize() == leaf_node->GetMaxSize()) {
    Split(leaf_node);
  }
  buffer_pool_manager_->UnpinPage(leaf_node->GetPageId(), true);
  return success;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeafPage(const KeyType &key) -> LeafPage * {
  page_id_t cur_node_id = root_page_id_;
  auto cur_node = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(cur_node_id)->GetData());
  while (!cur_node->IsLeafPage()) {
    auto cur_internal_node = reinterpret_cast<InternalPage *>(cur_node);
    auto new_node_id = cur_internal_node->FindKey(key, comparator_);
    buffer_pool_manager_->UnpinPage(cur_node_id, false);
    cur_node_id = new_node_id;
    cur_node = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(cur_node_id)->GetData());
  }
  return reinterpret_cast<LeafPage *>(cur_node);
}

INDEX_TEMPLATE_ARGUMENTS
template <typename ClassType>
auto BPLUSTREE_TYPE::Split(ClassType *origin_node) -> ClassType * {
  // auto origin_node=reinterpret_cast<BPlusTreePage*>(buffer_pool_manager_->FetchPage(origin_node_id)->GetData());
  page_id_t new_node_id;
  auto new_node = reinterpret_cast<ClassType *>(buffer_pool_manager_->NewPage(&new_node_id)->GetData());
  if (origin_node->IsLeafPage()) {
    auto origin_leaf_node = reinterpret_cast<LeafPage *>(origin_node);
    auto new_leaf_node = reinterpret_cast<LeafPage *>(new_node);
    new_leaf_node->Init(new_node_id, origin_leaf_node->GetParentPageId(), origin_leaf_node->GetMaxSize());
    origin_leaf_node->MoveTo(new_leaf_node);
    InsertIntoParent(origin_leaf_node, new_leaf_node->KeyAt(0), new_leaf_node);
    new_leaf_node->SetParentPageId(origin_leaf_node->GetParentPageId());
    new_leaf_node->SetNextPageId(origin_leaf_node->GetNextPageId());
    origin_leaf_node->SetNextPageId(new_leaf_node->GetPageId());
    buffer_pool_manager_->UnpinPage(new_leaf_node->GetPageId(), true);
    return reinterpret_cast<ClassType *>(new_leaf_node);
  }
  auto origin_internal_node = reinterpret_cast<InternalPage *>(origin_node);
  auto new_internal_node = reinterpret_cast<InternalPage *>(new_node);
  new_internal_node->Init(new_node_id, origin_internal_node->GetParentPageId(), origin_internal_node->GetMaxSize());
  origin_internal_node->MoveTo(new_internal_node, buffer_pool_manager_);
  InsertIntoParent(origin_internal_node, origin_internal_node->KeyAt(origin_internal_node->GetMinSize()),
                   new_internal_node);
  new_internal_node->SetParentPageId(origin_internal_node->GetParentPageId());
  buffer_pool_manager_->UnpinPage(new_internal_node->GetPageId(), true);
  return reinterpret_cast<ClassType *>(new_internal_node);
}

INDEX_TEMPLATE_ARGUMENTS
template <typename ClassType>
void BPLUSTREE_TYPE::InsertIntoParent(ClassType *origin_node, const KeyType &key, ClassType *new_node) {
  if (origin_node->IsRootPage()) {
    page_id_t internal_node_id;
    auto internal_node = reinterpret_cast<InternalPage *>(buffer_pool_manager_->NewPage(&internal_node_id)->GetData());
    internal_node->Init(internal_node_id, INVALID_PAGE_ID, internal_max_size_);  // NOLINT
    internal_node->SetValueAt(0, origin_node->GetPageId());
    internal_node->SetKeyAt(1, key);
    internal_node->SetValueAt(1, new_node->GetPageId());
    internal_node->IncreaseSize(2);
    root_page_id_ = internal_node_id;
    UpdateRootPageId(0);
    origin_node->SetParentPageId(internal_node_id);
    buffer_pool_manager_->UnpinPage(root_page_id_, true);
    return;
  }
  page_id_t parent_node_id = origin_node->GetParentPageId();
  auto parent_node = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(parent_node_id)->GetData());
  int index = parent_node->ValueIndex(origin_node->GetPageId());
  parent_node->Insert(index, key, new_node->GetPageId());
  if (parent_node->GetSize() == parent_node->GetMaxSize() + 1) {
    Split(parent_node);
  }
  buffer_pool_manager_->UnpinPage(parent_node_id, true);
}
/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  auto leaf_node = FindLeafPage(key);
  DeleteEntry(key, leaf_node);
  buffer_pool_manager_->UnpinPage(leaf_node->GetPageId(), true);
}

INDEX_TEMPLATE_ARGUMENTS
template <typename ClassType>
void BPLUSTREE_TYPE::DeleteEntry(const KeyType &key, ClassType *delete_node) {
  delete_node->Delete(key, comparator_);
  if (delete_node->IsRootPage() && delete_node->GetSize() == 1 && !delete_node->IsLeafPage()) {
    auto delete_internal_node = reinterpret_cast<InternalPage *>(delete_node);
    auto new_root_node =
        reinterpret_cast<LeafPage *>(buffer_pool_manager_->FetchPage(delete_internal_node->ValueAt(0))->GetData());
    root_page_id_ = new_root_node->GetPageId();
    UpdateRootPageId(0);
    new_root_node->SetParentPageId(INVALID_PAGE_ID);
    buffer_pool_manager_->UnpinPage(new_root_node->GetPageId(), true);
  } else if (!delete_node->IsRootPage() && delete_node->GetSize() < delete_node->GetMinSize()) {
    // 获得左边和右边的兄弟节点
    auto parent_page_id = delete_node->GetParentPageId();
    auto parent_node = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(parent_page_id)->GetData());
    auto index = parent_node->ValueIndex(delete_node->GetPageId());
    page_id_t left_sibling_node_id;
    if (index == 0) {
      left_sibling_node_id = INVALID_PAGE_ID;
    } else {
      left_sibling_node_id = parent_node->ValueAt(index - 1);
    }
    ClassType *left_sibling_node = nullptr;
    if (left_sibling_node_id != INVALID_PAGE_ID) {
      left_sibling_node =
          reinterpret_cast<ClassType *>(buffer_pool_manager_->FetchPage(left_sibling_node_id)->GetData());
    }
    page_id_t right_sibling_node_id;
    if (index == parent_node->GetSize() - 1) {
      right_sibling_node_id = INVALID_PAGE_ID;
    } else {
      right_sibling_node_id = parent_node->ValueAt(index + 1);
    }
    ClassType *right_sibling_node = nullptr;
    if (right_sibling_node_id != INVALID_PAGE_ID) {
      right_sibling_node =
          reinterpret_cast<ClassType *>(buffer_pool_manager_->FetchPage(right_sibling_node_id)->GetData());
    }
    buffer_pool_manager_->UnpinPage(parent_node->GetPageId(), true);
    // 判断要合并还是要借
    auto max_node_size = delete_node->GetMaxSize();
    if (left_sibling_node != nullptr) {
      if (delete_node->GetSize() + left_sibling_node->GetSize() < max_node_size) {
        Merge(left_sibling_node, delete_node);
      }
      if (delete_node->GetSize() + left_sibling_node->GetSize() >= max_node_size) {
        Borrow(left_sibling_node, delete_node);
      }
      buffer_pool_manager_->UnpinPage(left_sibling_node->GetPageId(), true);
    }
    if (right_sibling_node != nullptr) {
      if (delete_node->GetSize() + right_sibling_node->GetSize() < max_node_size) {
        Merge(delete_node, right_sibling_node);
      }
      if (delete_node->GetSize() + right_sibling_node->GetSize() >= max_node_size) {
        Borrow(delete_node, right_sibling_node);
      }
      buffer_pool_manager_->UnpinPage(right_sibling_node->GetPageId(), true);
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
template <typename ClassType>
void BPLUSTREE_TYPE::Borrow(ClassType *left_node, ClassType *right_node) {
  auto min_size = left_node->GetMinSize();
  auto parent_node =
      reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(left_node->GetParentPageId())->GetData());
  if (left_node->IsLeafPage()) {
    auto left_leaf_node = reinterpret_cast<LeafPage *>(left_node);
    auto right_leaf_node = reinterpret_cast<LeafPage *>(right_node);
    if (left_node->GetSize() < min_size) {
      // left <- right
      auto key = right_leaf_node->KeyAt(0);
      auto value = right_leaf_node->ValueAt(0);
      right_leaf_node->Delete(key, comparator_);
      left_leaf_node->SetKeyAt(left_leaf_node->GetSize(), key);
      left_leaf_node->SetValueAt(left_leaf_node->GetSize(), value);
      left_leaf_node->IncreaseSize(1);
      auto index = parent_node->ValueIndex(right_leaf_node->GetPageId());
      parent_node->SetKeyAt(index, right_leaf_node->KeyAt(0));
    } else {
      // left -> right
      KeyType key = left_leaf_node->KeyAt(left_leaf_node->GetSize() - 1);
      auto value = left_leaf_node->ValueAt(left_leaf_node->GetSize() - 1);
      left_leaf_node->Delete(key, comparator_);
      right_leaf_node->Insert(key, value, comparator_);
      auto index = parent_node->ValueIndex(right_leaf_node->GetPageId());
      parent_node->SetKeyAt(index, key);
    }
  } else if (!left_node->IsLeafPage()) {
    auto left_internal_node = reinterpret_cast<InternalPage *>(left_node);
    auto right_internal_node = reinterpret_cast<InternalPage *>(right_node);
    if (left_node->GetSize() < min_size) {
      // left <- right
      auto key = right_internal_node->KeyAt(0);
      auto value = right_internal_node->ValueAt(0);
      auto child_node = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(value)->GetData());
      child_node->SetParentPageId(left_internal_node->GetPageId());
      buffer_pool_manager_->UnpinPage(value, true);
      right_internal_node->Delete(key, comparator_);
      left_internal_node->SetKeyAt(left_node->GetSize(), key);
      left_internal_node->SetValueAt(left_node->GetSize(), value);
      left_internal_node->IncreaseSize(1);
      auto index = parent_node->ValueIndex(right_node->GetPageId());
      parent_node->SetKeyAt(index, right_node->KeyAt(0));
    } else {
      // left -> right
      auto key = left_internal_node->KeyAt(left_node->GetSize() - 1);
      auto value = left_internal_node->ValueAt(left_node->GetSize() - 1);
      left_internal_node->Delete(key, comparator_);
      right_internal_node->Insert(0, key, value);
      auto child_node = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(value)->GetData());
      child_node->SetParentPageId(right_internal_node->GetPageId());
      buffer_pool_manager_->UnpinPage(value, true);
      auto index = parent_node->ValueIndex(right_node->GetPageId());
      parent_node->SetKeyAt(index, key);
    }
  }
  buffer_pool_manager_->UnpinPage(parent_node->GetPageId(), true);
}

INDEX_TEMPLATE_ARGUMENTS
template <typename ClassType>
void BPLUSTREE_TYPE::Merge(ClassType *left_node, ClassType *right_node) {
  auto parent_node =
      reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(left_node->GetParentPageId())->GetData());
  if (left_node->IsLeafPage()) {
    // left <-right
    auto left_leaf_node = reinterpret_cast<LeafPage *>(left_node);
    auto right_leaf_node = reinterpret_cast<LeafPage *>(right_node);
    left_leaf_node->MoveFrom(right_leaf_node);
    int index = parent_node->ValueIndex(right_leaf_node->GetPageId());
    DeleteEntry(parent_node->KeyAt(index), parent_node);
  } else {
    // left <- right
    auto left_internal_node = reinterpret_cast<InternalPage *>(left_node);
    auto right_internal_node = reinterpret_cast<InternalPage *>(right_node);
    left_internal_node->MoveFrom(right_internal_node, buffer_pool_manager_);
    int index = parent_node->ValueIndex(right_internal_node->GetPageId());
    DeleteEntry(parent_node->KeyAt(index), parent_node);
  }
  buffer_pool_manager_->UnpinPage(parent_node->GetPageId(), true);
}
/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeafPage(bool left) -> LeafPage * {
  page_id_t cur_node_id = root_page_id_;
  page_id_t new_node_id;
  if(cur_node_id==INVALID_PAGE_ID){
    return nullptr;
  }
  auto cur_node = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(cur_node_id)->GetData());
  while (!cur_node->IsLeafPage()) {
    auto cur_internal_node = reinterpret_cast<InternalPage *>(cur_node);
    if (left) {
      new_node_id = cur_internal_node->ValueAt(0);
    } else {
      new_node_id = cur_internal_node->ValueAt(cur_internal_node->GetSize() - 1);
    }
    buffer_pool_manager_->UnpinPage(cur_node_id, false);
    cur_node_id = new_node_id;
    cur_node = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(cur_node_id)->GetData());
  }
  return reinterpret_cast<LeafPage *>(cur_node);
}
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  if (IsEmpty()) {
    return INDEXITERATOR_TYPE(nullptr, 0);
  }
  auto leaf_node = FindLeafPage(true);
  return INDEXITERATOR_TYPE(leaf_node, 0);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  if (IsEmpty()) {
    return INDEXITERATOR_TYPE(nullptr, 0);
  }
  auto leaf_node = FindLeafPage(key);
  return INDEXITERATOR_TYPE(leaf_node, leaf_node->KeyIndex(key, comparator_));
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE {
  if (IsEmpty()) {
    return INDEXITERATOR_TYPE(nullptr, 0);
  }
  auto leaf_node = FindLeafPage(false);
  return INDEXITERATOR_TYPE(leaf_node, leaf_node->GetSize());
}

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t { return root_page_id_; }

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  auto *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Draw an empty tree");
    return;
  }
  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  ToGraph(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm, out);
  out << "}" << std::endl;
  out.flush();
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  if (IsEmpty()) {
    LOG_WARN("Print an empty tree");
    return;
  }
  ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm);
}

/**
 * This method is used for debug only, You don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
