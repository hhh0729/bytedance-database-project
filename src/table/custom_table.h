#pragma once
#include <map>
#include <utility>
#include <vector>
#include "table.h"

namespace bytedance_db_project {
//
// Custom table implementation to adapt to provided query mix.
//
class CustomTable : Table {
 public:
  CustomTable();
  ~CustomTable();

  // Loads data into the table through passed-in data loader. Is not timed.
  void Load(BaseDataLoader *loader) override;

  // Returns the int32_t field at row `row_id` and column `col_id`.
  int32_t GetIntField(int32_t row_id, int32_t col_id) override;

  // Inserts the passed-in int32_t field at row `row_id` and column `col_id`.
  void PutIntField(int32_t row_id, int32_t col_id, int32_t field) override;

  // Implements the query
  // SELECT SUM(col0) FROM table;
  // Returns the sum of all elements in the first column of the table.
  int64_t ColumnSum() override;

  // Implements the query
  // SELECT SUM(col0) FROM table WHERE col1 > threshold1 AND col2 < threshold2;
  // Returns the sum of all elements in the first column of the table,
  // subject to the passed-in predicates.
  int64_t PredicatedColumnSum(int32_t threshold1, int32_t threshold2) override;

  // Implements the query
  // SELECT SUM(col0) + SUM(col1) + ... + SUM(coln) FROM table WHERE col0 >
  // threshold; Returns the sum of all elements in the rows which pass the
  // predicate.
  int64_t PredicatedAllColumnsSum(int32_t threshold) override;

  // Implements the query
  // UPDATE(col3 = col3 + col2) WHERE col0 < threshold;
  // Returns the number of rows updated.
  int64_t PredicatedUpdate(int32_t threshold) override;

  void AddIndex_0(int32_t key, int32_t row_id);

  void DeleteIndex_0(int32_t key, int32_t row_id);

  void AddIndex_1_2(std::pair<int32_t, int32_t> key_pair, int32_t row_id);

  void DeleteIndex_1_2(std::pair<int32_t, int32_t> key_pair, int32_t row_id);

 private:
  int32_t num_cols_{0};
  int32_t num_rows_{0};
  int32_t sum_of_col0{0};
  std::map<int32_t, std::vector<int32_t>> index_0;
  std::map<std::pair<int32_t, int32_t>, std::vector<int32_t>> index_1_2;
  char *rows_{nullptr};
};
}  // namespace bytedance_db_project