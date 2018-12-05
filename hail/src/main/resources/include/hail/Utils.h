#ifndef HAIL_UTILS_H
#define HAIL_UTILS_H 1

#define LIKELY(condition)   __builtin_expect(static_cast<bool>(condition), 1)
#define UNLIKELY(condition) __builtin_expect(static_cast<bool>(condition), 0)

inline char load_byte(char const* off) { return *off; }
inline bool load_bool(char const* off) { return *off; }
inline int load_int(char const* off) { return *reinterpret_cast<int const*>(off); }
inline long load_long(char const* off) { return *reinterpret_cast<long const*>(off); }
inline float load_float(char const* off) { return *reinterpret_cast<float const*>(off); }
inline double load_double(char const* off) { return *reinterpret_cast<double const*>(off); }
inline int load_length(char const* off) { return *reinterpret_cast<int const*>(off); }
inline char * load_address(char const* off) { return reinterpret_cast<char *>(*reinterpret_cast<long const*>(off)); }
inline bool load_bit(char const* byte_offset, unsigned int bit_offset) {
  return byte_offset[bit_offset >> 3] & (1 << (bit_offset & 0x7));
}


inline void store_byte(char * off, char b) { *off = b; }
inline void store_bool(char * off, bool b) { *off = b; }
inline void store_int(char * off, int i) { *reinterpret_cast<int *>(off) = i; }
inline void store_long(char * off, long l) { *reinterpret_cast<long *>(off) = l; }
inline void store_float(char * off, float f) { *reinterpret_cast<float *>(off) = f; }
inline void store_double(char * off, double d) { *reinterpret_cast<double *>(off) = d; }
inline void store_length(char * off, int len) { *reinterpret_cast<int *>(off) = len; }
inline void store_address(char * off, const char * addr) { *reinterpret_cast<long *>(off) = reinterpret_cast<long>(addr); }

inline void set_bit(char * byte_offset, unsigned int bit_offset) {
  char * off = byte_offset + (bit_offset >> 3);
  char new_byte = *off | (1 << (bit_offset & 0x7));
  *off = new_byte;
}

inline void clear_bit(char * byte_offset, unsigned int bit_offset) {
  char * off = byte_offset + (bit_offset >> 3);
  char new_byte = *off & ~(1 << (bit_offset & 0x7));
  *off = new_byte;
}
inline void store_bit(char * byte_offset, unsigned int bit_offset, bool b) {
  b ? set_bit(byte_offset, bit_offset) : clear_bit(byte_offset, bit_offset);
}

constexpr int n_missing_bytes(int array_len) {
  return ((unsigned long) array_len + 7L) >> 3;
}

constexpr long round_up_offset(long off, long alignment) {
  return (off + (alignment - 1)) & ~(alignment - 1);
}

inline char * round_up_alignment(char const* off, long alignment) {
  return reinterpret_cast<char *>(round_up_offset(reinterpret_cast<long>(off), alignment));
}

inline long round_up_alignment(long off, long alignment) {
  return (off + (alignment - 1)) & ~(alignment - 1);
}

inline int floordiv(int n, int d) {
  int q = n / d;
  int r = n - q * d;
  if (r < 0)
    --q;
  return q;
}

inline long lfloordiv(long n, long d) {
  long q = n / d;
  long r = n - q * d;
  if (r < 0)
    --q;
  return q;
}

template<bool elem_required, size_t elem_size, size_t elem_align>
class BaseArrayImpl {
private:
  static constexpr size_t array_elem_size = round_up_offset(elem_size, elem_align);
  
public:
  static int load_length(const char *a) {
    return load_int(a);
  }
  
  static bool is_element_missing(const char *a, int i) {
    if (elem_required)
      return false;
    else
      return load_bit(a + 4, i);
  }

  static constexpr int elements_offset(int len) {
    return round_up_alignment(4 + (elem_required ? 0 : n_missing_bytes(len)), elem_align);
  }
  
  static const char *elements_address(const char *a, int len) {
    return a + elements_offset(len);
  }
  
  static const char *elements_address(const char *a) {
    return elements_address(a, load_length(a));
  }
  
  static const char *element_address(const char *a, int i) {
    return elements_address(a) + i * array_elem_size;
  }
};

template<bool elem_required, size_t elem_size, size_t elem_align>
class ArrayAddrImpl : public BaseArrayImpl<elem_required, elem_size, elem_align> {
public:
  using Base = BaseArrayImpl<elem_required, elem_size, elem_align>;
  using T = const char *;
  
  static const char *load_element(const char *a, int i) {
    return Base::element_address(a, i);
  }
};

template<typename ElemT, bool elem_required, size_t elem_size, size_t elem_align>
class ArrayLoadImpl : public BaseArrayImpl<elem_required, elem_size, elem_align> {
public:
  using Base = BaseArrayImpl<elem_required, elem_size, elem_align>;
  using T = ElemT;
  
  static ElemT load_element(const char *a, int i) {
    return *reinterpret_cast<const ElemT *>(Base::element_address(a, i));
  }
};

#endif
