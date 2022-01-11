#include "indexed_row_table.h"
#include <cstring>

namespace bytedance_db_project {
IndexedRowTable::IndexedRowTable(int32_t index_column) {
  // not used
  index_column_ = index_column;
}

void IndexedRowTable::Load(BaseDataLoader *loader) {
  // TODO: Implement this!
  num_cols_ = loader->GetNumCols();
  auto rows = loader->GetRows();
  num_rows_ = rows.size();
  rows_ = new char[FIXED_FIELD_LEN * num_rows_ * num_cols_];
  for (auto row_id = 0; row_id < num_rows_; row_id++) {
    auto cur_row = rows.at(row_id);
    std::memcpy(rows_ + row_id * (FIXED_FIELD_LEN * num_cols_), cur_row,
                FIXED_FIELD_LEN * num_cols_);

    // add index of col0
    int32_t key = GetIntField(row_id, 0);
    if (index_.find(key) != index_.end()) {
      index_[key].push_back(row_id);
    } else {
      std::vector<int32_t> val;
      val.push_back(row_id);
      index_[key] = val;
    }

    sum_of_col0 += GetIntField(row_id, 0);
  }
}

int32_t IndexedRowTable::GetIntField(int32_t row_id, int32_t col_id) {
  // TODO: Implement this!
  return *(rows_ + 4 * (row_id * num_cols_ + col_id));
  // return 0;
}

void IndexedRowTable::PutIntField(int32_t row_id, int32_t col_id,
                                  int32_t field) {
  // TODO: Implement this!
  if (col_id == 0) {
    // delete old index
    int32_t key = GetIntField(row_id, 0);
    std::vector<int32_t> val1 = index_[key];
    std::vector<int32_t>::iterator it;
    for (it = val1.begin(); it != val1.end(); it++) {
      if (*it == key) {
        val1.erase(it);
        break;
      }
    }

    // add new index
    if (index_.find(field) != index_.end()) {
      index_[field].push_back(row_id);
    } else {
      std::vector<int32_t> val2;
      val2.push_back(row_id);
      index_[field] = val2;
    }
  }
  *(rows_ + 4 * (row_id * num_cols_ + col_id)) = field;
}

int64_t IndexedRowTable::ColumnSum() {
  // TODO: Implement this!
  return sum_of_col0;
}

int64_t IndexedRowTable::PredicatedColumnSum(int32_t threshold1,
                                             int32_t threshold2) {
  // TODO: Implement this!
  int64_t sum = 0;
  for (int row_id = 0; row_id < num_rows_; row_id++) {
    if (GetIntField(row_id, 1) > threshold1 &&
        GetIntField(row_id, 2) < threshold2)
      sum += GetIntField(row_id, 0);
  }
  return sum;
}

int64_t IndexedRowTable::PredicatedAllColumnsSum(int32_t threshold) {
  // TODO: Implement this!
  int64_t sum = 0;
  std::map<int32_t, std::vector<int32_t>>::iterator it =
      index_.upper_bound(threshold);
  for (; it != index_.end(); it++) {
    std::vector<int32_t> val = it->second;
    for (unsigned int i = 0; i < val.size(); i++) {
      for (int col_id = 0; col_id < num_cols_; col_id++) {
        sum += GetIntField(val[i], col_id);
      }
    }
  }
  return sum;
}

int64_t IndexedRowTable::PredicatedUpdate(int32_t threshold) {
  // TODO: Implement this!
  int64_t changed = 0;
  std::map<int32_t, std::vector<int32_t>>::iterator it,
      end = index_.lower_bound(threshold);
  for (it = index_.begin(); it != end; it++) {
    std::vector<int32_t> val = it->second;
    for (unsigned int i = 0; i < val.size(); i++) {
      changed++;
      PutIntField(val[i], 3, GetIntField(val[i], 3) + GetIntField(val[i], 2));
    }
  }
  return changed;
}
}  // namespace bytedance_db_project