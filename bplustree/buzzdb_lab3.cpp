#include <algorithm>
#include <atomic>
#include <cassert>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <exception>
#include <fstream>
#include <iostream>
#include <limits>
#include <list>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <queue>
#include <random>
#include <set>
#include <shared_mutex>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

#define UNUSED(p) ((void)(p))

#define ASSERT_WITH_MESSAGE(condition, message)                                                                            \
  do {                                                                                                                     \
    if (!(condition)) {                                                                                                    \
      std::cerr << "Assertion \033[1;31mFAILED\033[0m: " << message << " at " << __FILE__ << ":" << __LINE__ << std::endl; \
      std::abort();                                                                                                        \
    }                                                                                                                      \
  } while (0)

enum FieldType { INT,
                 FLOAT,
                 STRING };

// Define a basic Field variant class that can hold different types
class Field {
 public:
  FieldType type;
  std::unique_ptr<char[]> data;
  size_t data_length;

 public:
  Field(int i) : type(INT) {
    data_length = sizeof(int);
    data = std::make_unique<char[]>(data_length);
    std::memcpy(data.get(), &i, data_length);
  }

  Field(float f) : type(FLOAT) {
    data_length = sizeof(float);
    data = std::make_unique<char[]>(data_length);
    std::memcpy(data.get(), &f, data_length);
  }

  Field(const std::string& s) : type(STRING) {
    data_length = s.size() + 1;  // include null-terminator
    data = std::make_unique<char[]>(data_length);
    std::memcpy(data.get(), s.c_str(), data_length);
  }

  Field& operator=(const Field& other) {
    if (&other == this) {
      return *this;
    }
    type = other.type;
    data_length = other.data_length;
    std::memcpy(data.get(), other.data.get(), data_length);
    return *this;
  }

  Field(Field&& other) {
    type = other.type;
    data_length = other.data_length;
    std::memcpy(data.get(), other.data.get(), data_length);
  }

  FieldType getType() const { return type; }
  int asInt() const {
    return *reinterpret_cast<int*>(data.get());
  }
  float asFloat() const {
    return *reinterpret_cast<float*>(data.get());
  }
  std::string asString() const {
    return std::string(data.get());
  }

  std::string serialize() {
    std::stringstream buffer;
    buffer << type << ' ' << data_length << ' ';
    if (type == STRING) {
      buffer << data.get() << ' ';
    } else if (type == INT) {
      buffer << *reinterpret_cast<int*>(data.get()) << ' ';
    } else if (type == FLOAT) {
      buffer << *reinterpret_cast<float*>(data.get()) << ' ';
    }
    return buffer.str();
  }

  void serialize(std::ofstream& out) {
    std::string serializedData = this->serialize();
    out << serializedData;
  }

  static std::unique_ptr<Field> deserialize(std::istream& in) {
    int type;
    in >> type;
    size_t length;
    in >> length;
    if (type == STRING) {
      std::string val;
      in >> val;
      return std::make_unique<Field>(val);
    } else if (type == INT) {
      int val;
      in >> val;
      return std::make_unique<Field>(val);
    } else if (type == FLOAT) {
      float val;
      in >> val;
      return std::make_unique<Field>(val);
    }
    return nullptr;
  }

  void print() const {
    switch (getType()) {
      case INT:
        std::cout << asInt();
        break;
      case FLOAT:
        std::cout << asFloat();
        break;
      case STRING:
        std::cout << asString();
        break;
    }
  }
};

class Tuple {
 public:
  std::vector<std::unique_ptr<Field>> fields;

  void addField(std::unique_ptr<Field> field) {
    fields.push_back(std::move(field));
  }

  size_t getSize() const {
    size_t size = 0;
    for (const auto& field : fields) {
      size += field->data_length;
    }
    return size;
  }

  std::string serialize() {
    std::stringstream buffer;
    buffer << fields.size() << ' ';
    for (const auto& field : fields) {
      buffer << field->serialize();
    }
    return buffer.str();
  }

  void serialize(std::ofstream& out) {
    std::string serializedData = this->serialize();
    out << serializedData;
  }

  static std::unique_ptr<Tuple> deserialize(std::istream& in) {
    auto tuple = std::make_unique<Tuple>();
    size_t fieldCount;
    in >> fieldCount;
    for (size_t i = 0; i < fieldCount; ++i) {
      tuple->addField(Field::deserialize(in));
    }
    return tuple;
  }

  void print() const {
    for (const auto& field : fields) {
      field->print();
      std::cout << " ";
    }
    std::cout << "\n";
  }
};

static constexpr size_t PAGE_SIZE = 4096;                       // Fixed page size
static constexpr size_t MAX_SLOTS = 512;                        // Fixed number of slots
static constexpr size_t MAX_PAGES = 1000;                       // Total Number of pages that can be stored
uint16_t INVALID_VALUE = std::numeric_limits<uint16_t>::max();  // Sentinel value

struct Slot {
  bool empty = true;                // Is the slot empty?
  uint16_t offset = INVALID_VALUE;  // Offset of the slot within the page
  uint16_t length = INVALID_VALUE;  // Length of the slot
};

// Slotted Page class
class SlottedPage {
 public:
  std::unique_ptr<char[]> page_data = std::make_unique<char[]>(PAGE_SIZE);
  size_t metadata_size = sizeof(Slot) * MAX_SLOTS;

  SlottedPage() {
    // Empty page -> initialize slot array inside page
    Slot* slot_array = reinterpret_cast<Slot*>(page_data.get());
    for (size_t slot_itr = 0; slot_itr < MAX_SLOTS; slot_itr++) {
      slot_array[slot_itr].empty = true;
      slot_array[slot_itr].offset = INVALID_VALUE;
      slot_array[slot_itr].length = INVALID_VALUE;
    }
  }

  // Add a tuple, returns true if it fits, false otherwise.
  bool addTuple(std::unique_ptr<Tuple> tuple) {
    // Serialize the tuple into a char array
    auto serializedTuple = tuple->serialize();
    size_t tuple_size = serializedTuple.size();

    // std::cout << "Tuple size: " << tuple_size << " bytes\n";
    assert(tuple_size == 38);

    // Check for first slot with enough space
    size_t slot_itr = 0;
    Slot* slot_array = reinterpret_cast<Slot*>(page_data.get());
    for (; slot_itr < MAX_SLOTS; slot_itr++) {
      if (slot_array[slot_itr].empty == true and
          slot_array[slot_itr].length >= tuple_size) {
        break;
      }
    }
    if (slot_itr == MAX_SLOTS) {
      // std::cout << "Page does not contain an empty slot with sufficient space to store the tuple.";
      return false;
    }

    // Identify the offset where the tuple will be placed in the page
    // Update slot meta-data if needed
    slot_array[slot_itr].empty = false;
    size_t offset = INVALID_VALUE;
    if (slot_array[slot_itr].offset == INVALID_VALUE) {
      if (slot_itr != 0) {
        auto prev_slot_offset = slot_array[slot_itr - 1].offset;
        auto prev_slot_length = slot_array[slot_itr - 1].length;
        offset = prev_slot_offset + prev_slot_length;
      } else {
        offset = metadata_size;
      }

      slot_array[slot_itr].offset = offset;
    } else {
      offset = slot_array[slot_itr].offset;
    }

    if (offset + tuple_size >= PAGE_SIZE) {
      slot_array[slot_itr].empty = true;
      slot_array[slot_itr].offset = INVALID_VALUE;
      return false;
    }

    assert(offset != INVALID_VALUE);
    assert(offset >= metadata_size);
    assert(offset + tuple_size < PAGE_SIZE);

    if (slot_array[slot_itr].length == INVALID_VALUE) {
      slot_array[slot_itr].length = tuple_size;
    }

    // Copy serialized data into the page
    std::memcpy(page_data.get() + offset,
                serializedTuple.c_str(),
                tuple_size);

    return true;
  }

  void deleteTuple(size_t index) {
    Slot* slot_array = reinterpret_cast<Slot*>(page_data.get());
    size_t slot_itr = 0;
    for (; slot_itr < MAX_SLOTS; slot_itr++) {
      if (slot_itr == index and
          slot_array[slot_itr].empty == false) {
        slot_array[slot_itr].empty = true;
        break;
      }
    }

    // std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  void print() const {
    Slot* slot_array = reinterpret_cast<Slot*>(page_data.get());
    for (size_t slot_itr = 0; slot_itr < MAX_SLOTS; slot_itr++) {
      if (slot_array[slot_itr].empty == false) {
        assert(slot_array[slot_itr].offset != INVALID_VALUE);
        const char* tuple_data = page_data.get() + slot_array[slot_itr].offset;
        std::istringstream iss(tuple_data);
        auto loadedTuple = Tuple::deserialize(iss);
        std::cout << "Slot " << slot_itr << " : [";
        std::cout << (uint16_t)(slot_array[slot_itr].offset) << "] :: ";
        loadedTuple->print();
      }
    }
    std::cout << "\n";
  }
};

const std::string database_filename = "buzzdb.dat";

class StorageManager {
 public:
  std::fstream fileStream;
  size_t num_pages = 0;
  std::mutex io_mutex;

 public:
  StorageManager(bool truncate_mode = true) {
    auto flags = truncate_mode ? std::ios::in | std::ios::out | std::ios::trunc
                               : std::ios::in | std::ios::out;
    fileStream.open(database_filename, flags);
    if (!fileStream) {
      // If file does not exist, create it
      fileStream.clear();  // Reset the state
      fileStream.open(database_filename, truncate_mode ? (std::ios::out | std::ios::trunc) : std::ios::out);
    }
    fileStream.close();
    fileStream.open(database_filename, std::ios::in | std::ios::out);

    fileStream.seekg(0, std::ios::end);
    num_pages = fileStream.tellg() / PAGE_SIZE;

    if (num_pages == 0) {
      extend();
    }
  }

  ~StorageManager() {
    if (fileStream.is_open()) {
      fileStream.close();
    }
  }

  // Read a page from disk
  std::unique_ptr<SlottedPage> load(uint16_t page_id) {
    if (page_id >= num_pages) {
      // Find the next power of 2.
      uint32_t cap = page_id + 1;
      if (cap != (cap & -cap)) {
        cap <<= 1;
        do {
          cap &= cap - 1;
        } while (cap != (cap & -cap));
      }
      extend(cap);
      assert(page_id < num_pages);
    }

    fileStream.seekg(page_id * PAGE_SIZE, std::ios::beg);
    auto page = std::make_unique<SlottedPage>();

    // Read the content of the file into the page
    if (fileStream.read(page->page_data.get(), PAGE_SIZE)) {
      // std::cout << "Page read successfully from file." << std::endl;
    } else {
      std::cerr << "Error: Unable to read data from the file. \n";
    }
    return page;
  }

  // Write a page to disk
  void flush(uint16_t page_id, const SlottedPage& page) {
    size_t page_offset = page_id * PAGE_SIZE;

    // Move the write pointer
    fileStream.seekp(page_offset, std::ios::beg);
    fileStream.write(page.page_data.get(), PAGE_SIZE);
    fileStream.flush();
  }

  // Extend database file by one page
  void extend() {
    // Create a slotted page
    auto empty_slotted_page = std::make_unique<SlottedPage>();

    // Move the write pointer
    fileStream.seekp(0, std::ios::end);

    // Write the page to the file, extending it
    fileStream.write(empty_slotted_page->page_data.get(), PAGE_SIZE);
    fileStream.flush();

    // Update number of pages
    num_pages += 1;
  }

  void extend(uint64_t till_page_id) {
    std::lock_guard<std::mutex> io_guard(io_mutex);
    uint64_t write_size = std::max(static_cast<uint64_t>(0), till_page_id + 1 - num_pages) * PAGE_SIZE;
    if (write_size > 0) {
      // std::cout << "Extending database file till page id : "<<till_page_id<<" \n";
      char* buffer = new char[write_size];
      std::memset(buffer, 0, write_size);

      fileStream.seekp(0, std::ios::end);
      fileStream.write(buffer, write_size);
      fileStream.flush();

      num_pages = till_page_id + 1;
    }
  }
};

using PageID = uint16_t;

class Policy {
 public:
  virtual bool touch(PageID page_id) = 0;
  virtual PageID evict() = 0;
  virtual ~Policy() = default;
};

void printList(std::string list_name, const std::list<PageID>& myList) {
  std::cout << list_name << " :: ";
  for (const PageID& value : myList) {
    std::cout << value << ' ';
  }
  std::cout << '\n';
}

class LruPolicy : public Policy {
 private:
  // List to keep track of the order of use
  std::list<PageID> lruList;

  // Map to find a page's iterator in the list efficiently
  std::unordered_map<PageID, std::list<PageID>::iterator> map;

  size_t cacheSize;

 public:
  LruPolicy(size_t cacheSize) : cacheSize(cacheSize) {}

  bool touch(PageID page_id) override {
    // printList("LRU", lruList);

    bool found = false;
    // If page already in the list, remove it
    if (map.find(page_id) != map.end()) {
      found = true;
      lruList.erase(map[page_id]);
      map.erase(page_id);
    }

    // If cache is full, evict
    if (lruList.size() == cacheSize) {
      evict();
    }

    if (lruList.size() < cacheSize) {
      // Add the page to the front of the list
      lruList.emplace_front(page_id);
      map[page_id] = lruList.begin();
    }

    return found;
  }

  PageID evict() override {
    // Evict the least recently used page
    PageID evictedPageId = INVALID_VALUE;
    if (lruList.size() != 0) {
      evictedPageId = lruList.back();
      map.erase(evictedPageId);
      lruList.pop_back();
    }
    return evictedPageId;
  }
};

constexpr size_t MAX_PAGES_IN_MEMORY = 10;

class BufferManager {
 private:
  using PageMap = std::unordered_map<PageID, SlottedPage>;

  StorageManager storage_manager;
  PageMap pageMap;
  std::unique_ptr<Policy> policy;

 public:
  BufferManager(bool storage_manager_truncate_mode = true) : storage_manager(storage_manager_truncate_mode),
                                                             policy(std::make_unique<LruPolicy>(MAX_PAGES_IN_MEMORY)) {
  }

  ~BufferManager() {
    for (auto& pair : pageMap) {
      flushPage(pair.first);
    }
  }

  SlottedPage& fix_page(int page_id) {
    auto it = pageMap.find(page_id);
    if (it != pageMap.end()) {
      policy->touch(page_id);
      return pageMap.find(page_id)->second;
    }

    if (pageMap.size() >= MAX_PAGES_IN_MEMORY) {
      auto evictedPageId = policy->evict();
      if (evictedPageId != INVALID_VALUE) {
        // std::cout << "Evicting page " << evictedPageId << "\n";
        storage_manager.flush(evictedPageId,
                              pageMap[evictedPageId]);
      }
    }

    auto page = storage_manager.load(page_id);
    policy->touch(page_id);
    // std::cout << "Loading page: " << page_id << "\n";
    pageMap[page_id] = std::move(*page);
    return pageMap[page_id];
  }

  void flushPage(int page_id) {
    storage_manager.flush(page_id, pageMap[page_id]);
  }

  void extend() {
    storage_manager.extend();
  }

  size_t getNumPages() {
    return storage_manager.num_pages;
  }
};

template <typename KeyT, typename ValueT, typename ComparatorT, size_t PageSize>
class BTree {
 public:
  struct Node {
    /// The level in the tree.
    uint16_t level;

    /// The number of children current.
    uint16_t count;

    /// TODO: Add additional members as needed

    // Constructor
    Node(uint16_t level, uint16_t count)
        : level(level), count(count) {}

    /// Is the node a leaf node?
    bool is_leaf() const { return level == 0; }
  };

  struct InnerNode : public Node {
    /// The capacity of a node.
    /// TODO think about the capacity that the nodes have.
    static constexpr uint32_t kMemoryCapacity = 5;
    static constexpr uint32_t kCapacity = kMemoryCapacity - 1;

    /// The keys.
    KeyT keys[kMemoryCapacity - 1];

    /// The children.
    /// comp(keys in children[i], keys[i]) == true
    /// comp(keys in children[i + 1], keys[i]) == false
    uint64_t children[kMemoryCapacity];

    /// Constructor.
    InnerNode(uint16_t level) : Node(level, 0) {}

    std::pair<uint32_t, bool> lower_bound(const KeyT& key, ComparatorT comp) const {
      uint32_t index = std::find_if(keys, keys + this->count - 1, [&key, comp](auto& x) {
                         return !comp(x, key);
                       }) -
                       keys;
      return {index, index + 1 < this->count && keys[index] == key};
    }

    /// Insert a key.
    /// @param[in] index        The position of insertion.
    /// @param[in] key          The separator that should be inserted.
    /// @param[in] split_page   The id of the split page that should be inserted.
    /// @return true if need to split
    bool try_insert(uint32_t index, const KeyT& key, uint64_t split_page) {
      // 可以直接假设节点未满，插入
      for (uint16_t i = this->count - 1; i > index; --i) {
        keys[i] = keys[i - 1];
        children[i + 1] = children[i];
      }
      keys[index] = key;
      children[index + 1] = split_page;
      this->count++;

      return this->count == kMemoryCapacity;
    }

    /// Split the inner node.
    /// @param[in] new_node       The inner node to save the latter part.
    /// @return                   The separator key.
    KeyT split(InnerNode* new_node) {
      // 前半部分最多比后半部分多一个（偶数情况）
      // 这里算的是key的数量，最后得加回去
      this->count = kMemoryCapacity / 2;
      new_node->count = (kMemoryCapacity - 1) - this->count - 1;
      for (uint32_t i = this->count + 1; i < kMemoryCapacity - 1; ++i) {
        new_node->keys[i - this->count - 1] = keys[i];
        new_node->children[i - this->count] = children[i + 1];
      }
      new_node->children[0] = children[this->count + 1];

      ++this->count;
      ++new_node->count;

      // 返回中间键值
      return keys[this->count - 1];
    }
  };

  struct LeafNode : public Node {
    /// The capacity of a node.
    /// TODO think about the capacity that the nodes have.
    static constexpr uint32_t kMemoryCapacity = 5;
    static constexpr uint32_t kCapacity = kMemoryCapacity - 1;

    /// The keys.
    KeyT keys[kMemoryCapacity];

    /// The values.
    ValueT values[kMemoryCapacity];

    /// Constructor.
    LeafNode() : Node(0, 0) {}

    std::pair<uint32_t, bool> lower_bound(const KeyT& key, ComparatorT comp) const {
      uint32_t index = std::find_if(keys, keys + this->count, [&key, comp](auto& x) {
                         return !comp(x, key);
                       }) -
                       keys;
      return {index, index < this->count && keys[index] == key};
    }

    /// Insert a key.
    /// @param[in] key          The key that should be inserted.
    /// @param[in] value        The value that should be inserted.
    /// @return true if need to split.
    bool try_insert(const KeyT& key, const ValueT& value, ComparatorT comp) {
      // 可以直接假设节点未满，插入
      auto [index, found] = lower_bound(key, comp);
      if (found) {
        values[index] = value;
        return false;
      }

      for (uint16_t i = this->count; i > index; --i) {
        keys[i] = keys[i - 1];
        values[i] = values[i - 1];
      }
      keys[index] = key;
      values[index] = value;
      this->count++;

      return this->count == kMemoryCapacity;
    }

    /// Erase a key.
    void erase(const KeyT& key, ComparatorT comp) {
      auto [index, found] = lower_bound(key, comp);
      if (found) {
        // 找到了键值，开始移除
        for (uint16_t i = index; i < this->count - 1; ++i) {
          keys[i] = keys[i + 1];
          values[i] = values[i + 1];
        }
        this->count--;
      }
    }

    /// Split the leaf node.
    /// @param[in] new_node       The leaf node that save the latter part.
    /// @return                    The separator key.
    KeyT split(LeafNode* new_node) {
      // 左边最多比右边多一个元素（奇数情况）
      // middle key 保存在右半部分:
      // [0, 1, ..., previous of middle key] [middle key, ..., n - 1, n]
      this->count = (kMemoryCapacity + 1) / 2;
      KeyT middle_key = keys[this->count];
      new_node->count = kMemoryCapacity - this->count;
      for (uint32_t i = this->count; i < kMemoryCapacity; ++i) {
        new_node->keys[i - this->count] = keys[i];
        new_node->values[i - this->count] = values[i];
      }
      // 返回中间键值
      return middle_key;
    }
  };

  /// The root.
  std::optional<uint64_t> root;

  /// The buffer manager
  BufferManager& buffer_manager;

  /// Next page id.
  /// You don't need to worry about about the page allocation.
  /// (Neither fragmentation, nor persisting free-space bitmaps)
  /// Just increment the next_page_id whenever you need a new page.
  uint64_t next_page_id;

  ComparatorT comp;

  struct MetaInfo {
    static constexpr uint32_t kMagic = 0xb00ee;
    static constexpr uint32_t kVersion = 0;
    uint32_t magic;
    uint32_t version;
    uint64_t root_page;
    uint64_t next_page_id;
  };

  /// Constructor.
  BTree(BufferManager& buffer_manager) : buffer_manager(buffer_manager) {
    if (buffer_manager.getNumPages() > 1) {
      auto meta = reinterpret_cast<MetaInfo*>(buffer_manager.fix_page(0).page_data.get());
      if (meta->magic != MetaInfo::kMagic) {
        throw std::runtime_error("BTree file has broken");
      }
      if (meta->version > MetaInfo::kVersion) {
        throw std::runtime_error("Can not open a newer version dat");
      }
      if (meta->root_page != 0) {
        root = meta->root_page;
      }

      next_page_id = meta->next_page_id;
    } else {
      next_page_id = 1;
    }
  }

  ~BTree() {
    auto meta = reinterpret_cast<MetaInfo*>(buffer_manager.fix_page(0).page_data.get());
    meta->magic = MetaInfo::kMagic;
    meta->version = MetaInfo::kVersion;
    meta->root_page = root.value_or(0);
    meta->next_page_id = next_page_id;
  }

 private:
  std::pair<SlottedPage*, uint64_t> allocate_page() noexcept {
    auto page_id = next_page_id++;
    return {&buffer_manager.fix_page(page_id), page_id};
  }

  void dellocate_page(uint64_t page_id) noexcept {
    // DO NOTHING...
  }

  Node* get_node(uint64_t page_id) {
    SlottedPage* page = &buffer_manager.fix_page(page_id);
    return reinterpret_cast<Node*>(page->page_data.get());
  }

 public:
  /// Lookup an entry in the tree.
  /// @param[in] key      The key that should be searched.
  std::optional<ValueT> lookup(const KeyT& key) {
    if (!root.has_value()) {
      return std::nullopt;
    }
    return lookup_impl(get_node(*root), key);
  }

 private:
  std::optional<ValueT> lookup_impl(Node* node, const KeyT& key) {
    if (node->is_leaf()) {
      auto leaf_node = static_cast<LeafNode*>(node);
      auto [index, found] = leaf_node->lower_bound(key, comp);
      if (found) {
        return leaf_node->values[index];
      }
      return std::nullopt;
    }

    auto inner_node = static_cast<InnerNode*>(node);
    auto [index, found] = inner_node->lower_bound(key, comp);
    if (found) {
      ++index;
    }
    return lookup_impl(get_node(inner_node->children[index]), key);
  }

 public:
  /// Erase an entry in the tree.
  /// @param[in] key      The key that should be searched.
  void erase(const KeyT& key) {
    if (!root.has_value()) {
      return;
    }
    erase_recursive(get_node(*root), key);
  }

 private:
  void erase_recursive(Node* node, const KeyT& key) {
    if (node->is_leaf()) {
      erase_leaf(static_cast<LeafNode*>(node), key);
    } else {
      erase_inner(static_cast<InnerNode*>(node), key);
    }
  }

  void erase_leaf(LeafNode* node, const KeyT& key) {
    // 如果是叶子节点，直接删除
    node->erase(key, comp);
    // TODO: merge
  }

  void erase_inner(InnerNode* node, const KeyT& key) {
    // 如果是内部节点，递归删除
    auto [index, found] = node->lower_bound(key, comp);
    if (found) {
      ++index;
    }
    erase_recursive(get_node(node->children[index]), key);
    // TODO: merge
  }

 public:
  /// Inserts a new entry into the tree.
  /// @param[in] key      The key that should be inserted.
  /// @param[in] value    The value that should be inserted.
  void insert(const KeyT& key, const ValueT& value) {
    if (!root.has_value()) {
      // 空树，创建一个新的根节点作为叶子节点
      auto [page, id] = allocate_page();
      auto node = new (page->page_data.get()) LeafNode;
      auto ret = insert_leaf(node, key, value);
      assert(!ret.has_value());
      root = id;
      return;
    }

    auto root_node = get_node(*root);

    auto ret = insert_recursive(root_node, key, value);
    if (ret.has_value()) {
      // 根节点分裂
      // [new root] ------
      //     |           |
      //     V           V
      // [old root] [ret->second]
      auto [page, id] = allocate_page();
      auto node = new (page->page_data.get()) InnerNode(root_node->level + 1);
      node->count = 2;
      node->keys[0] = ret->first;
      node->children[0] = *root;
      node->children[1] = ret->second;
      root = id;
    }
  }

 private:
  std::optional<std::pair<KeyT, uint64_t>> insert_recursive(Node* node, const KeyT& key, const ValueT& value) {
    if (node->is_leaf()) {
      return insert_leaf(static_cast<LeafNode*>(node), key, value);
    }
    return insert_inner(static_cast<InnerNode*>(node), key, value);
  }

  std::optional<std::pair<KeyT, uint64_t>> insert_leaf(LeafNode* node, const KeyT& key, const ValueT& value) {
    // 如果是叶子节点，直接插入
    if (node->try_insert(key, value, comp)) {
      // 触发分裂
      auto [latter_page, latter] = allocate_page();
      auto latter_node = new (latter_page->page_data.get()) LeafNode;
      KeyT split_key = node->split(latter_node);

      return std::pair{split_key, latter};
    }
    return std::nullopt;
  }

  std::optional<std::pair<KeyT, uint64_t>> insert_inner(InnerNode* node, const KeyT& key, const ValueT& value) {
    // 如果是内部节点，递归插入
    auto [index, found] = node->lower_bound(key, comp);
    if (found) {
      ++index;
    }
    auto ret = insert_recursive(get_node(node->children[index]), key, value);
    if (!ret.has_value()) {
      return std::nullopt;
    }

    // 子节点发生分裂，尝试直接插入
    if (!node->try_insert(index, ret->first, ret->second)) {
      return std::nullopt;
    }

    // 当前节点再次触发分裂
    auto [latter_page, latter] = allocate_page();
    auto latter_node = new (latter_page->page_data.get()) InnerNode(node->level);

    return std::pair{node->split(latter_node), latter};
  }

 public:
  void print() {
    std::ofstream f("tree.txt");
    std::queue<std::pair<int, int>> q;
    q.emplace(*root, -1);
    auto level = -1;
    while (q.size()) {
      auto [x, cur_level] = q.front();
      q.pop();
      if (cur_level != level) {
        level = cur_level;
        f << '\n';
      }
      print_rec(f, get_node(x), q);
    }
  }

  void print_rec(std::ostream& out, Node* node, std::queue<std::pair<int, int>>& q) {
    if (node->is_leaf()) {
      print_leaf(out, static_cast<LeafNode*>(node));
    } else {
      print_inner(out, static_cast<InnerNode*>(node), q);
    }
  }

  void print_inner(std::ostream& out, InnerNode* node, std::queue<std::pair<int, int>>& q) {
    out << '(';
    bool comma = false;
    for (int i = 0; i < node->count - 1; ++i) {
      if (comma) {
        out << ", ";
      }
      out << node->keys[i];
      comma = true;
    }
    out << ") ";
    // for (auto x : std::span(node->children, node->count)) {
    //   q.emplace(x, node->level);
    // }
    for (int i = 0; i < node->count; ++i) {
      q.emplace(node->children[i], node->level);
    }
  }

  void print_leaf(std::ostream& out, LeafNode* node) {
    out << '(';
    bool comma = false;
    for (int i = 0; i != node->count; ++i) {
      if (comma) {
        out << ", ";
      }
      out << "(" << node->keys[i] << ' ' << node->values[i] << ')';
      comma = true;
    }
    out << ") ";
  }
};

int main(int argc, char* argv[]) {
  bool execute_all = false;
  std::string selected_test = "-1";

  if (argc < 2) {
    execute_all = true;
  } else {
    selected_test = argv[1];
  }

  using BTree = BTree<uint64_t, uint64_t, std::less<uint64_t>, 1024>;

  // Test 1: InsertEmptyTree
  if (execute_all || selected_test == "1") {
    std::cout << "...Starting Test 1" << std::endl;
    BufferManager buffer_manager;
    BTree tree(buffer_manager);

    ASSERT_WITH_MESSAGE(tree.root.has_value() == false,
                        "tree.root is not nullptr");

    tree.insert(42, 21);

    ASSERT_WITH_MESSAGE(tree.root.has_value(),
                        "tree.root is still nullptr after insertion");

    std::string test = "inserting an element into an empty B-Tree";

    // Fix root page and obtain root node pointer
    SlottedPage* root_page = &buffer_manager.fix_page(*tree.root);
    auto root_node = reinterpret_cast<BTree::Node*>(root_page->page_data.get());

    ASSERT_WITH_MESSAGE(root_node->is_leaf() == true,
                        test + " does not create a leaf node.");
    ASSERT_WITH_MESSAGE(root_node->count == 1,
                        test + " does not create a leaf node with count = 1.");

    std::cout << "\033[1m\033[32mPassed: Test 1\033[0m" << std::endl;
  }

  // Test 2: InsertLeafNode
  if (execute_all || selected_test == "2") {
    std::cout << "...Starting Test 2" << std::endl;
    BufferManager buffer_manager;
    BTree tree(buffer_manager);

    ASSERT_WITH_MESSAGE(tree.root.has_value() == false,
                        "tree.root is not nullptr");

    for (auto i = 0ul; i < BTree::LeafNode::kCapacity; ++i) {
      tree.insert(i, 2 * i);
    }
    ASSERT_WITH_MESSAGE(tree.root.has_value(),
                        "tree.root is still nullptr after insertion");

    std::string test = "inserting BTree::LeafNode::kCapacity elements into an empty B-Tree";

    SlottedPage* root_page = &buffer_manager.fix_page(*tree.root);
    auto root_node = reinterpret_cast<BTree::Node*>(root_page->page_data.get());
    // This is a leaf node, why cast it to BTree::InnerNode? The behavior is undefined (Although it works)
    // See also: https://en.cppreference.com/w/cpp/language/static_cast
    // We don't need to visit BTree::InnerNode's member.
    // auto root_inner_node = static_cast<BTree::InnerNode*>(root_node);

    ASSERT_WITH_MESSAGE(root_node->is_leaf() == true,
                        test + " creates an inner node as root.");
    ASSERT_WITH_MESSAGE(root_node->count == BTree::LeafNode::kCapacity,
                        test + " does not store all elements.");

    std::cout << "\033[1m\033[32mPassed: Test 2\033[0m" << std::endl;
  }

  // Test 3: InsertLeafNodeSplit
  if (execute_all || selected_test == "3") {
    std::cout << "...Starting Test 3" << std::endl;
    BufferManager buffer_manager;
    BTree tree(buffer_manager);

    ASSERT_WITH_MESSAGE(tree.root.has_value() == false,
                        "tree.root is not nullptr");

    for (auto i = 0ul; i < BTree::LeafNode::kCapacity; ++i) {
      tree.insert(i, 2 * i);
    }
    ASSERT_WITH_MESSAGE(tree.root.has_value(),
                        "tree.root is still nullptr after insertion");

    SlottedPage* root_page = &buffer_manager.fix_page(*tree.root);
    auto root_node = reinterpret_cast<BTree::Node*>(root_page->page_data.get());
    // ditto.
    // auto root_inner_node = static_cast<BTree::InnerNode*>(root_node);

    assert(root_node->is_leaf());
    assert(root_node->count == BTree::LeafNode::kCapacity);

    // Let there be a split...
    tree.insert(424242, 42);

    std::string test =
        "inserting BTree::LeafNode::kCapacity + 1 elements into an empty B-Tree";

    ASSERT_WITH_MESSAGE(tree.root.has_value() != false, test + " removes the root :-O");

    SlottedPage* root_page1 = &buffer_manager.fix_page(*tree.root);
    root_node = reinterpret_cast<BTree::Node*>(root_page1->page_data.get());
    // ditto.
    // root_inner_node = static_cast<BTree::InnerNode*>(root_node);

    ASSERT_WITH_MESSAGE(root_node->is_leaf() == false,
                        test + " does not create a root inner node");
    ASSERT_WITH_MESSAGE(root_node->count == 2,
                        test + " creates a new root with count != 2");

    std::cout << "\033[1m\033[32mPassed: Test 3\033[0m" << std::endl;
  }

  // Test 4: LookupEmptyTree
  if (execute_all || selected_test == "4") {
    std::cout << "...Starting Test 4" << std::endl;
    BufferManager buffer_manager;
    BTree tree(buffer_manager);

    std::string test = "searching for a non-existing element in an empty B-Tree";

    ASSERT_WITH_MESSAGE(tree.lookup(42).has_value() == false,
                        test + " seems to return something :-O");

    std::cout << "\033[1m\033[32mPassed: Test 4\033[0m" << std::endl;
  }

  // Test 5: LookupSingleLeaf
  if (execute_all || selected_test == "5") {
    std::cout << "...Starting Test 5" << std::endl;
    BufferManager buffer_manager;
    BTree tree(buffer_manager);

    // Fill one page
    for (auto i = 0ul; i < BTree::LeafNode::kCapacity; ++i) {
      tree.insert(i, 2 * i);
      ASSERT_WITH_MESSAGE(tree.lookup(i).has_value(),
                          "searching for the just inserted key k=" + std::to_string(i) + " yields nothing");
    }

    // Lookup all values
    for (auto i = 0ul; i < BTree::LeafNode::kCapacity; ++i) {
      auto v = tree.lookup(i);
      ASSERT_WITH_MESSAGE(v.has_value(), "key=" + std::to_string(i) + " is missing");
      ASSERT_WITH_MESSAGE(*v == 2 * i, "key=" + std::to_string(i) + " should have the value v=" + std::to_string(2 * i));
    }

    std::cout << "\033[1m\033[32mPassed: Test 5\033[0m" << std::endl;
  }

  // Test 6: LookupSingleSplit
  if (execute_all || selected_test == "6") {
    std::cout << "...Starting Test 6" << std::endl;
    BufferManager buffer_manager;
    BTree tree(buffer_manager);

    // Insert values
    for (auto i = 0ul; i < BTree::LeafNode::kCapacity; ++i) {
      tree.insert(i, 2 * i);
    }

    tree.insert(BTree::LeafNode::kCapacity, 2 * BTree::LeafNode::kCapacity);
    ASSERT_WITH_MESSAGE(tree.lookup(BTree::LeafNode::kCapacity).has_value(),
                        "searching for the just inserted key k=" + std::to_string(BTree::LeafNode::kCapacity + 1) + " yields nothing");

    // Lookup all values
    for (auto i = 0ul; i < BTree::LeafNode::kCapacity + 1; ++i) {
      auto v = tree.lookup(i);
      ASSERT_WITH_MESSAGE(v.has_value(), "key=" + std::to_string(i) + " is missing");
      ASSERT_WITH_MESSAGE(*v == 2 * i,
                          "key=" + std::to_string(i) + " should have the value v=" + std::to_string(2 * i));
    }

    std::cout << "\033[1m\033[32mPassed: Test 6\033[0m" << std::endl;
  }

  // Test 7: LookupMultipleSplitsIncreasing
  if (execute_all || selected_test == "7") {
    std::cout << "...Starting Test 7" << std::endl;
    BufferManager buffer_manager;
    BTree tree(buffer_manager);
    auto n = 40 * BTree::LeafNode::kCapacity;

    // Insert values
    for (auto i = 0ul; i < n; ++i) {
      tree.insert(i, 2 * i);
      ASSERT_WITH_MESSAGE(tree.lookup(i).has_value(),
                          "searching for the just inserted key k=" + std::to_string(i) + " yields nothing");
    }

    // Lookup all values
    for (auto i = 0ul; i < n; ++i) {
      auto v = tree.lookup(i);
      ASSERT_WITH_MESSAGE(v.has_value(), "key=" + std::to_string(i) + " is missing");
      ASSERT_WITH_MESSAGE(*v == 2 * i,
                          "key=" + std::to_string(i) + " should have the value v=" + std::to_string(2 * i));
    }
    std::cout << "\033[1m\033[32mPassed: Test 7\033[0m" << std::endl;
  }

  // Test 8: LookupMultipleSplitsDecreasing
  if (execute_all || selected_test == "8") {
    std::cout << "...Starting Test 8" << std::endl;
    BufferManager buffer_manager;
    BTree tree(buffer_manager);
    auto n = 10 * BTree::LeafNode::kCapacity;

    // Insert values
    for (auto i = n; i > 0; --i) {
      tree.insert(i, 2 * i);
      ASSERT_WITH_MESSAGE(tree.lookup(i).has_value(),
                          "searching for the just inserted key k=" + std::to_string(i) + " yields nothing");
    }

    // Lookup all values
    for (auto i = n; i > 0; --i) {
      auto v = tree.lookup(i);
      ASSERT_WITH_MESSAGE(v.has_value(), "key=" + std::to_string(i) + " is missing");
      ASSERT_WITH_MESSAGE(*v == 2 * i,
                          "key=" + std::to_string(i) + " should have the value v=" + std::to_string(2 * i));
    }

    std::cout << "\033[1m\033[32mPassed: Test 8\033[0m" << std::endl;
  }

  // Test 9: LookupRandomNonRepeating
  if (execute_all || selected_test == "9") {
    std::cout << "...Starting Test 9" << std::endl;
    BufferManager buffer_manager;
    BTree tree(buffer_manager);
    auto n = 10 * BTree::LeafNode::kCapacity;

    // Generate random non-repeating key sequence
    std::vector<uint64_t> keys(n);
    std::iota(keys.begin(), keys.end(), n);
    std::mt19937_64 engine(0);
    std::shuffle(keys.begin(), keys.end(), engine);

    // Insert values
    for (auto i = 0ul; i < n; ++i) {
      tree.insert(keys[i], 2 * keys[i]);
      ASSERT_WITH_MESSAGE(tree.lookup(keys[i]).has_value(),
                          "searching for the just inserted key k=" + std::to_string(keys[i]) +
                              " after i=" + std::to_string(i) + " inserts yields nothing");
    }

    // Lookup all values
    for (auto i = 0ul; i < n; ++i) {
      auto v = tree.lookup(keys[i]);
      ASSERT_WITH_MESSAGE(v.has_value(), "key=" + std::to_string(keys[i]) + " is missing");
      ASSERT_WITH_MESSAGE(*v == 2 * keys[i],
                          "key=" + std::to_string(keys[i]) + " should have the value v=" + std::to_string(2 * keys[i]));
    }

    std::cout << "\033[1m\033[32mPassed: Test 9\033[0m" << std::endl;

    tree.print();
  }

  // Test 10: LookupRandomRepeating
  if (execute_all || selected_test == "10") {
    std::cout << "...Starting Test 10" << std::endl;
    BufferManager buffer_manager;
    BTree tree(buffer_manager);
    auto n = 10 * BTree::LeafNode::kCapacity;

    // Insert & updated 100 keys at random
    std::mt19937_64 engine{0};
    std::uniform_int_distribution<uint64_t> key_distr(0, 99);
    std::vector<uint64_t> values(100);
    std::vector<uint64_t> cnt(100);

    for (auto i = 1ul; i < n; ++i) {
      uint64_t rand_key = key_distr(engine);
      values[rand_key] = i;
      cnt[rand_key]++;
      tree.insert(rand_key, i);

      auto v = tree.lookup(rand_key);
      ASSERT_WITH_MESSAGE(v.has_value(),
                          "searching for the just inserted key k=" + std::to_string(rand_key) +
                              " after i=" + std::to_string(i - 1) + " inserts yields nothing");
      ASSERT_WITH_MESSAGE(*v == i,
                          "overwriting k=" + std::to_string(rand_key) + " with value v=" + std::to_string(i) +
                              " failed");
    }

    // Lookup all values
    for (auto i = 0ul; i < 100; ++i) {
      if (values[i] == 0) {
        continue;
      }
      auto v = tree.lookup(i);
      ASSERT_WITH_MESSAGE(v.has_value(), "key=" + std::to_string(i) + " is missing");
      ASSERT_WITH_MESSAGE(*v == values[i],
                          "key=" + std::to_string(i) + " should have the value v=" + std::to_string(values[i]));
    }

    std::cout << "\033[1m\033[32mPassed: Test 10\033[0m" << std::endl;
  }

  // Test 11: Erase
  if (execute_all || selected_test == "11") {
    std::cout << "...Starting Test 11" << std::endl;
    BufferManager buffer_manager;
    BTree tree(buffer_manager);

    // Insert values
    for (auto i = 0ul; i < 2 * BTree::LeafNode::kCapacity; ++i) {
      tree.insert(i, 2 * i);
    }

    // Iteratively erase all values
    for (auto i = 0ul; i < 2 * BTree::LeafNode::kCapacity; ++i) {
      ASSERT_WITH_MESSAGE(tree.lookup(i).has_value(), "k=" + std::to_string(i) + " was not in the tree");
      tree.erase(i);
      ASSERT_WITH_MESSAGE(!tree.lookup(i), "k=" + std::to_string(i) + " was not removed from the tree");
    }
    std::cout << "\033[1m\033[32mPassed: Test 11\033[0m" << std::endl;
  }

  // Test 12: Persistant Btree
  if (execute_all || selected_test == "12") {
    std::cout << "...Starting Test 12" << std::endl;
    unsigned long n = 10 * BTree::LeafNode::kCapacity;

    // Build a tree
    {
      BufferManager buffer_manager;
      BTree tree(buffer_manager);

      // Insert values
      for (auto i = 0ul; i < n; ++i) {
        tree.insert(i, 2 * i);
        ASSERT_WITH_MESSAGE(tree.lookup(i).has_value(),
                            "searching for the just inserted key k=" + std::to_string(i) + " yields nothing");
      }

      // Lookup all values
      for (auto i = 0ul; i < n; ++i) {
        auto v = tree.lookup(i);
        ASSERT_WITH_MESSAGE(v.has_value(), "key=" + std::to_string(i) + " is missing");
        ASSERT_WITH_MESSAGE(*v == 2 * i,
                            "key=" + std::to_string(i) + " should have the value v=" + std::to_string(2 * i));
      }
    }

    // recreate the buffer manager and check for existence of the tree
    {
      BufferManager buffer_manager(false);
      BTree tree(buffer_manager);

      // Lookup all values
      for (auto i = 0ul; i < n; ++i) {
        auto v = tree.lookup(i);
        ASSERT_WITH_MESSAGE(v.has_value(), "key=" + std::to_string(i) + " is missing");
        ASSERT_WITH_MESSAGE(*v == 2 * i,
                            "key=" + std::to_string(i) + " should have the value v=" + std::to_string(2 * i));
      }
    }

    std::cout << "\033[1m\033[32mPassed: Test 12\033[0m" << std::endl;
  }

  return 0;
}
