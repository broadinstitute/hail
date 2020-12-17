#include "lsm.h"
#include <iostream>
#include <string>
#include <variant>
#include <vector>
#include <fstream>
#include <limits>
#include <bitset>
#include <filesystem>
#include "MurmurHash3.h"

void BloomFilter::insert_key(int32_t k) {
  uint32_t seed = 1;
  uint32_t key_hash;
  MurmurHash3_x86_32(&k, sizeof k, seed, &key_hash);
  auto last_d = key_hash % 10;
  bset[last_d] = 1;
}
char BloomFilter::contains_key(int32_t k) {
  uint32_t seed = 1;
  uint32_t key_hash;
  MurmurHash3_x86_32(&k, sizeof k, seed, &key_hash);
  auto last_d = key_hash % 10;
  return bset[last_d];
}
int Level::size() {
  std::cerr << "size of level " << index << " is " << files.size() << "\n";
  return files.size();
}
void Level::add_file(File f) {
  std::cerr << "add file " << f.filename << " to level " << index << "\n";
  files.push_back(f);
}
std::string Level::file_path(std::filesystem::path directory) {
  std::cerr << "file_path: " << directory / std::to_string(index) / std::to_string(files.size()) << "\n";
//  return directory / std::to_string(index) / std::to_string(files.size());
  return level_directory / std::to_string(files.size());
}
File Level::write_to_file(std::map<int32_t, maybe_value> m, std::string filename) {
  std::ofstream ostrm(filename, std::ios::binary);
  BloomFilter bloomFilter;
  int min = std::numeric_limits<int>::max();
  int max = std::numeric_limits<int>::lowest();
  for (auto const&x : m) {
    bloomFilter.insert_key(x.first);
    ostrm.write(reinterpret_cast<const char*>(&x.first), sizeof x.first);
    ostrm.write(reinterpret_cast<const char*>(&x.second.v), sizeof x.second.v);
    ostrm.write(reinterpret_cast<const char*>(&x.second.is_deleted), sizeof x.second.is_deleted);
    if (x.first > max) {max = x.first;}
    if (x.first < min) {min = x.first;}
  }
  std::cerr << "write to file " << filename << "\n";
  return File(filename, bloomFilter, min, max);
}
void Level::read_to_map(File f, std::map<int32_t, maybe_value> &m) {
  if (auto istrm = std::ifstream(f.filename, std::ios::binary)) {
    int k;
    while (istrm.read(reinterpret_cast<char *>(&k), sizeof k)) {
      int v;
      char d;
      istrm.read(reinterpret_cast<char *>(&v), sizeof v);
      istrm.read(reinterpret_cast<char *>(&d), sizeof d);
      m.insert_or_assign(k, maybe_value(v,d));
    }
  } else {
    std::cerr << "could not open " << f.filename << "\n";
    exit(3);
  }
  std::cerr << "read file " << f.filename << " to map" << "\n";
}
File Level::merge(File older_f, File newer_f, std::string merged_filename) {
  std::map<int32_t, maybe_value> m;
  read_to_map(older_f, m);
  read_to_map(newer_f, m);
  std::cerr << "merge files " << older_f.filename << "and" << newer_f.filename << " to new file " << merged_filename << "\n";
  return write_to_file(m, merged_filename);
}

void LSM::add_to_level(File f, size_t l_index) {
  if (l_index >= levels.size()) {
    Level l = Level(l_index, directory);
    l.add_file(f);
    levels.push_back(l);
    std::cerr << "add file " << f.filename << " to level " << l_index << "\n";
  } else if (levels[l_index].size() + 1 >= levels[l_index].max_size) {
    assert(levels[l_index].max_size == 2);
    File merged_f = levels[l_index].merge(levels[l_index].files.back(), f,
                                          //std::to_string(levels[l_index].files.size()));
                                          levels[l_index].file_path(directory));
    add_to_level(merged_f, l_index + 1);
    levels[l_index].files.pop_back();
  } else {
    levels[l_index].add_file(f);
    std::cerr << "add file " << f.filename << " to level " << l_index << "\n";
  }
}
void LSM::put(int32_t k, int32_t v, char deleted) {
  if (m.size() >= 4) {
    std::string f_name = "0";
    if (levels.size() > 0) {
      std::string f_name = std::to_string(levels[0].files.size());
    }
    std::string filename = directory / std::to_string(0) / f_name;
    File f = write_to_file(filename);
    //files.push_back(f);
    add_to_level(f, 0);
    m.clear();
    for (auto l : levels) {
      assert(l.size() <= l.max_size);
    }
  }
  m.insert_or_assign(k,maybe_value(v, deleted));
}
std::optional<int32_t> LSM::get(int32_t k) {
  auto it = m.find(k);

  if (it != m.end()) {
    if(!it->second.is_deleted) {
      return it->second.v;
    } else {
      return std::nullopt;
    }
  } else {
    for (auto i = levels.rbegin(); i != levels.rend(); ++i ) {
      Level level = *i;
      for (auto j = level.files.rbegin(); j != level.files.rend(); ++j ) {
        File file = *j;
        if (file.bloomFilter.contains_key(k) && k >= file.min && k <= file.max) {
          std::cout << "exists? " << std::filesystem::exists(file.filename);
          std::map <int32_t, maybe_value> file_map = read_from_file(file.filename);
          auto it_m = file_map.find(k);
          if (it_m != m.end()) {
            if (!it_m->second.is_deleted) {
              return it_m->second.v;
            } else {
              return std::nullopt;
            }
          }
        }
      }
    }

//    for (auto i = files.rbegin(); i != files.rend(); ++i ) {
//      File file = *i;
//      if (file.bloomFilter.contains_key(k) && k >= file.min && k <= file.max) {
//        std::map<int32_t, maybe_value> file_map = read_from_file(file.filename);
//        auto it_m = file_map.find(k);
//        if (it_m != m.end()) {
//          if(!it_m->second.is_deleted) {
//            return it_m->second.v;
//          } else {
//            return std::nullopt;
//          }
//        }
//      }
//    }
  }
  return std::nullopt;
}
std::vector<std::pair<int32_t, int32_t>> LSM::range(int32_t l, int32_t r) {
  std::vector<std::pair<int32_t, int32_t>> res;
  std::map<int32_t,int32_t>  res_map;
  for (auto level : levels) {
    for (auto file : level.files) {
      if (r >= file.min && l <= file.max) {
        std::map <int32_t, maybe_value> file_map = read_from_file(file.filename);
        auto it_ml = file_map.lower_bound(l);
        auto it_mu = file_map.lower_bound(r);
        for (auto it = it_ml; it != it_mu; ++it) {
          if (!it->second.is_deleted) {
            res_map.insert_or_assign(it->first, it->second.v);
          } else {
            res_map.erase(it->first);
          }
        }
      }
    }
  }

  auto it_l = m.lower_bound(l);
  auto it_u = m.lower_bound(r);
  for (auto it=it_l; it!=it_u; ++it) {
    if (!it->second.is_deleted) {
      res_map.insert_or_assign(it->first, it->second.v);
    } else {
      res_map.erase(it->first);
    }
  }

  auto rit_l = res_map.lower_bound(l);
  auto rit_u = res_map.lower_bound(r);
  for (auto it=rit_l; it!=rit_u; ++it) {
    res.push_back(std::make_pair(it->first, it->second));
  }
  return res;
}
void LSM::del(int32_t k) {
  put(k,0, 1);
}
File LSM::write_to_file(std::string filename) {
  std::ofstream ostrm(filename, std::ios::binary);
  if (!ostrm.good()) {
    std::cerr << "could not open " << filename;
    exit(4);
  }
  BloomFilter bloomFilter;
  int min = std::numeric_limits<int>::max();
  int max = std::numeric_limits<int>::lowest();
  for (auto const&x : m) {
    bloomFilter.insert_key(x.first);
    ostrm.write(reinterpret_cast<const char*>(&x.first), sizeof x.first);
    ostrm.write(reinterpret_cast<const char*>(&x.second.v), sizeof x.second.v);
    ostrm.write(reinterpret_cast<const char*>(&x.second.is_deleted), sizeof x.second.is_deleted);
    if (x.first > max) {max = x.first;}
    if (x.first < min) {min = x.first;}
  }
  std::cerr << "write to file " << filename << "\n";
  return File(filename, bloomFilter, min, max);
}
std::map<int32_t, maybe_value> LSM::read_from_file(std::string filename) {
  std::map<int32_t, maybe_value> new_m;
  if (auto istrm = std::ifstream(filename, std::ios::binary)) {
    int k;
    while (istrm.read(reinterpret_cast<char *>(&k), sizeof k)) {
      int v;
      char d;
      istrm.read(reinterpret_cast<char *>(&v), sizeof v);
      istrm.read(reinterpret_cast<char *>(&d), sizeof d);
      new_m.insert_or_assign(k, maybe_value(v,d));
    }
  } else {
    std::cerr << "could not open " << filename << "\n";
    exit(3);
  }
  return new_m;
}
void LSM::dump_map() {
  for (auto const&x : m) {
    std::cout << x.first << "\n";
  }
}
