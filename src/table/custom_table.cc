#include "custom_table.h"
#include <cstring>

namespace bytedance_db_project {
CustomTable::CustomTable() {}

CustomTable::~CustomTable() {}

void CustomTable::Load(BaseDataLoader *loader) {
  // TODO: Implement this!
  num_cols_ = loader->GetNumCols();
  auto rows = loader->GetRows();
  num_rows_ = rows.size();
  rows_ = new char[FIXED_FIELD_LEN * num_rows_ * num_cols_];
  for (auto row_id = 0; row_id < num_rows_; row_id++) {
    auto cur_row = rows.at(row_id);
    std::memcpy(rows_ + row_id * (FIXED_FIELD_LEN * num_cols_), cur_row,
                FIXED_FIELD_LEN * num_cols_);

    // col0
    AddIndex_0(GetIntField(row_id, 0), row_id);
    sum_of_col0 += GetIntField(row_id, 0);

    // col1 and col2
    AddIndex_1_2(std::make_pair(GetIntField(row_id, 1), GetIntField(row_id, 2)),
                 row_id);
  }
}

int32_t CustomTable::GetIntField(int32_t row_id, int32_t col_id) {
  // TODO: Implement this!
  return *(rows_ + 4 * (row_id * num_cols_ + col_id));
}

void CustomTable::PutIntField(int32_t row_id, int32_t col_id, int32_t field) {
  // TODO: Implement this!
  if (col_id == 0) {
    DeleteIndex_0(GetIntField(row_id, 0), row_id);
    AddIndex_0(field, row_id);
    sum_of_col0 += field - GetIntField(row_id, 0);
  } else if (col_id == 1) {
    DeleteIndex_1_2(
        std::make_pair(GetIntField(row_id, 1), GetIntField(row_id, 2)), row_id);
    AddIndex_1_2(std::make_pair(field, GetIntField(row_id, 2)), row_id);
  } else if (col_id == 2) {
    DeleteIndex_1_2(
        std::make_pair(GetIntField(row_id, 1), GetIntField(row_id, 2)), row_id);
    AddIndex_1_2(std::make_pair(GetIntField(row_id, 1), field), row_id);
  }
  *(rows_ + 4 * (row_id * num_cols_ + col_id)) = field;
}

int64_t CustomTable::ColumnSum() {
  // TODO: Implement this!
  return sum_of_col0;
}

int64_t CustomTable::PredicatedColumnSum(int32_t threshold1,
                                         int32_t threshold2) {
  // TODO: Implement this!
  int64_t sum = 0;
  std::map<std::pair<int32_t, int32_t>, std::vector<int32_t>>::iterator it =
      index_1_2.upper_bound(std::make_pair(threshold1, 1024));
  for (; it != index_1_2.end(); it++) {
    std::pair<int32_t, int32_t> key = it->first;
    std::vector<int32_t> val = it->second;
    if (key.second >= threshold2) break;
    for (unsigned int i = 0; i < val.size(); i++) {
      sum += GetIntField(val[i], 0);
    }
  }

  return sum;
}

int64_t CustomTable::PredicatedAllColumnsSum(int32_t threshold) {
  // TODO: Implement this!
  int64_t sum = 0;
  std::map<int32_t, std::vector<int32_t>>::iterator it =
      index_0.upper_bound(threshold);
  for (; it != index_0.end(); it++) {
    std::vector<int32_t> v = it->second;
    for (unsigned int i = 0; i < v.size(); i++) {
      for (int col_id = 0; col_id < num_cols_; col_id++) {
        sum += GetIntField(v[i], col_id);
      }
    }
  }
  return sum;
}

int64_t CustomTable::PredicatedUpdate(int32_t threshold) {
  // TODO: Implement this!
  int64_t changed = 0;
  std::map<int32_t, std::vector<int32_t>>::iterator it,
      end = index_0.lower_bound(threshold);
  for (it = index_0.begin(); it != end; it++) {
    std::vector<int32_t> v = it->second;
    for (unsigned int i = 0; i < v.size(); i++) {
      changed++;
      PutIntField(v[i], 3, GetIntField(v[i], 3) + GetIntField(v[i], 2));
    }
  }
  return changed;
}

void CustomTable::AddIndex_0(int32_t key, int32_t row_id) {
  if (index_0.find(key) != index_0.end()) {
    index_0[key].push_back(row_id);
  } else {
    std::vector<int32_t> val;
    val.push_back(row_id);
    index_0[key] = val;
  }
}

void CustomTable::DeleteIndex_0(int32_t key, int32_t row_id) {
  std::vector<int32_t> val = index_0[key];
  std::vector<int32_t>::iterator it;
  for (it = val.begin(); it != val.end(); it++) {
    if (*it == row_id) {
      val.erase(it);
      break;
    }
  }
}

void CustomTable::AddIndex_1_2(std::pair<int32_t, int32_t> key_pair,
                               int32_t row_id) {
  if (index_1_2.find(key_pair) != index_1_2.end()) {
    index_1_2[key_pair].push_back(row_id);
  } else {
    std::vector<int32_t> val;
    val.push_back(row_id);
    index_1_2[key_pair] = val;
  }
}

void CustomTable::DeleteIndex_1_2(std::pair<int32_t, int32_t> key_pair,
                                  int32_t row_id) {
  std::vector<int32_t> val = index_1_2[key_pair];
  std::vector<int32_t>::iterator it;
  for (it = val.begin(); it != val.end(); it++) {
    if (*it == row_id) {
      val.erase(it);
      break;
    }
  }
}
}  // namespace bytedance_db_project