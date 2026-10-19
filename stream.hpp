#ifndef __STREAM_HPP__
#define __STREAM_HPP__

// C++ implementations of the stream module's native functions.
//
// A producer receives its sink as a std::function; the sink type is read off
// the producer's own parameter type.

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <queue>
#include <stdexcept>
#include <string>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

template <class F> struct stream_fn;
template <class R, class... A> struct stream_fn<std::function<R(A...)>> {
    using ret = R;
    using arg0 = std::tuple_element_t<0, std::tuple<A...>>;
};

template <class T> struct stream_is_function : std::false_type {};
template <class R, class... A> struct stream_is_function<std::function<R(A...)>> : std::true_type {};

// A sink's result: converts to whatever unit type the producer's sink
// returns, so a sink can be handed to a producer whose sink type is unnamed
// (a bound function rather than a std::function).
struct StreamUnit {
    template <class T>
    operator T() const {
        return T{};
    }
};

// Hand `f` to `produce` as its sink.
template <class Producer, class F>
void stream_drain(Producer& produce, F f) {
    produce([&f](const auto& batch) {
        f(batch);
        return StreamUnit{};
    });
}

template <class F, class B, class Producer>
B stream_fold_source(F step, B init, Producer produce) {
    B acc = std::move(init);
    stream_drain(produce, [&](const auto& batch) { acc = step(acc, batch); });
    return acc;
}

// Collects a producer's batches into the list type it is converted to.
template <class Producer>
struct StreamCollected {
    Producer produce;
    template <class V>
    operator V() {
        V all;
        stream_drain(produce, [&](const V& batch) { all.insert(all.end(), batch.begin(), batch.end()); });
        return all;
    }
};

template <class Producer>
auto stream_collect_source(Producer produce) {
    if constexpr (stream_is_function<Producer>::value) {
        using SinkFn = typename stream_fn<Producer>::arg0;
        using Batch = std::decay_t<typename stream_fn<std::decay_t<SinkFn>>::arg0>;
        Batch all;
        stream_drain(produce, [&](const Batch& batch) { all.insert(all.end(), batch.begin(), batch.end()); });
        return all;
    } else {
        return StreamCollected<Producer>{std::move(produce)};
    }
}

// The element type of a pull's batches.
template <class Pull>
using stream_elem_t = typename std::decay_t<std::tuple_element_t<2, std::invoke_result_t<Pull&>>>::value_type;

// Summaries merged in stream order as a binary counter: two adjacent results
// of equal rank merge into one of the next rank, so each summary takes part
// in O(log n) merges. Only associativity is used.
template <class B>
class StreamMerges {
  public:
    template <class G>
    void push(const G& merge, B b) {
        uint32_t r = 0;
        while (!stack_.empty() && stack_.back().first == r) {
            b = merge(stack_.back().second, b);
            stack_.pop_back();
            ++r;
        }
        stack_.emplace_back(r, std::move(b));
    }

    template <class G>
    B finish(const G& merge, B empty) {
        if (stack_.empty()) return empty;
        B acc = std::move(stack_[0].second);
        for (size_t i = 1; i < stack_.size(); ++i) acc = merge(acc, stack_[i].second);
        return acc;
    }

  private:
    std::vector<std::pair<uint32_t, B>> stack_;
};

template <class F, class G, class Pull>
auto stream_reduce_native(F summarize, G merge, Pull pull) {
    using A = stream_elem_t<Pull>;
    using B = std::decay_t<decltype(summarize(std::vector<A>{}))>;
    StreamMerges<B> acc;
    for (;;) {
        auto r = pull();
        if (!std::get<0>(r)) {
            return std::tuple<bool, std::string, B>(false, std::get<1>(r), acc.finish(merge, summarize(std::vector<A>{})));
        }
        if (std::get<2>(r).empty()) {
            return std::tuple<bool, std::string, B>(true, "", acc.finish(merge, summarize(std::vector<A>{})));
        }
        acc.push(merge, summarize(std::get<2>(r)));
    }
}

template <class Pull>
auto stream_take_pull_native(int64_t n, Pull pull) {
    using A = stream_elem_t<Pull>;
    const size_t want = n > 0 ? static_cast<size_t>(n) : 0;
    std::vector<A> out;
    while (out.size() < want) {
        auto r = pull();
        if (!std::get<0>(r)) return std::tuple<bool, std::string, std::vector<A>>(false, std::get<1>(r), std::move(out));
        auto& xs = std::get<2>(r);
        if (xs.empty()) break;
        const size_t k = std::min(want - out.size(), xs.size());
        out.insert(out.end(), std::make_move_iterator(xs.begin()), std::make_move_iterator(xs.begin() + k));
    }
    return std::tuple<bool, std::string, std::vector<A>>(true, "", std::move(out));
}

// External sorting

constexpr size_t STREAM_SORTED_BATCH = size_t(1) << 16;
constexpr size_t STREAM_MAX_RUNS = 64;

using StreamRecord = std::array<uint64_t, 4>;

template <size_t W>
void stream_sort_by(std::vector<uint64_t>& xs) {
    std::vector<std::array<uint64_t, W>> rs(xs.size() / W);
    std::memcpy(rs.data(), xs.data(), xs.size() * sizeof(uint64_t));
    std::sort(rs.begin(), rs.end());
    std::memcpy(xs.data(), rs.data(), xs.size() * sizeof(uint64_t));
}

inline void stream_sort_records(std::vector<uint64_t>& xs, size_t width) {
    switch (width) {
        case 1: std::sort(xs.begin(), xs.end()); break;
        case 2: stream_sort_by<2>(xs); break;
        case 3: stream_sort_by<3>(xs); break;
        default: stream_sort_by<4>(xs); break;
    }
}

struct StreamFile {
    std::FILE* f = nullptr;
    explicit StreamFile(std::FILE* p) : f(p) {}
    StreamFile(const StreamFile&) = delete;
    StreamFile& operator=(const StreamFile&) = delete;
    StreamFile(StreamFile&& o) noexcept : f(o.f) { o.f = nullptr; }
    StreamFile& operator=(StreamFile&& o) noexcept {
        if (this != &o) {
            if (f) std::fclose(f);
            f = o.f;
            o.f = nullptr;
        }
        return *this;
    }
    ~StreamFile() {
        if (f) std::fclose(f);
    }
};

class StreamSorter {
  public:
    StreamSorter(std::string dir, size_t width, size_t budget) : dir_(std::move(dir)), width_(width), budget_(budget) {}

    std::string push(const std::vector<uint64_t>& xs) {
        if (reading_) return "pushSorter: the sorter is already being read";
        if (xs.size() % width_ != 0) {
            return "pushSorter: " + std::to_string(xs.size()) + " words are not whole records of " + std::to_string(width_);
        }
        buf_.insert(buf_.end(), xs.begin(), xs.end());
        if (buf_.size() >= budget_) spill();
        return "";
    }

    std::vector<uint64_t> next() {
        if (!reading_) start();
        std::vector<uint64_t> out;
        out.reserve(STREAM_SORTED_BATCH * width_);
        if (runs_.empty()) {
            const size_t k = std::min(STREAM_SORTED_BATCH * width_, buf_.size() - at_);
            out.insert(out.end(), buf_.begin() + at_, buf_.begin() + at_ + k);
            at_ += k;
            return out;
        }
        while (out.size() < STREAM_SORTED_BATCH * width_ && !heap_.empty()) {
            auto [r, i] = heap_.top();
            heap_.pop();
            out.insert(out.end(), r.begin(), r.begin() + width_);
            StreamRecord n;
            if (read_record(runs_[i], n)) heap_.emplace(n, i);
        }
        return out;
    }

  private:
    using Heap = std::priority_queue<std::pair<StreamRecord, size_t>, std::vector<std::pair<StreamRecord, size_t>>, std::greater<>>;

    void spill() {
        if (buf_.empty()) return;
        stream_sort_records(buf_, width_);
        StreamFile f = new_run();
        if (std::fwrite(buf_.data(), sizeof(uint64_t), buf_.size(), f.f) != buf_.size()) {
            throw std::runtime_error("cannot write a sort run in " + dir_);
        }
        runs_.push_back(std::move(f));
        buf_.clear();
        if (runs_.size() >= STREAM_MAX_RUNS) compact();
    }

    // An empty file in the directory, already unlinked.
    StreamFile new_run() {
        static std::atomic<uint64_t> next{0};
        const std::string path = dir_ + "/.sorter-" + std::to_string(::getpid()) + "-c" + std::to_string(next++);
        int fd = ::open(path.c_str(), O_RDWR | O_CREAT | O_EXCL, 0600);
        if (fd < 0) throw std::runtime_error("cannot create a sort run in " + dir_ + ": " + std::strerror(errno));
        if (::unlink(path.c_str()) != 0) {
            ::close(fd);
            throw std::runtime_error("cannot unlink " + path + ": " + std::strerror(errno));
        }
        std::FILE* f = ::fdopen(fd, "w+b");
        if (!f) {
            ::close(fd);
            throw std::runtime_error("cannot open a sort run in " + dir_);
        }
        return StreamFile(f);
    }

    bool read_record(StreamFile& run, StreamRecord& r) {
        r = StreamRecord{0, 0, 0, 0};
        size_t got = std::fread(r.data(), sizeof(uint64_t), width_, run.f);
        if (got == width_) return true;
        if (got == 0 && std::feof(run.f)) return false;
        throw std::runtime_error("cannot read a sort run in " + dir_);
    }

    Heap merger() {
        Heap heap;
        for (size_t i = 0; i < runs_.size(); ++i) {
            if (std::fseek(runs_[i].f, 0, SEEK_SET) != 0) throw std::runtime_error("cannot read a sort run in " + dir_);
            StreamRecord r;
            if (read_record(runs_[i], r)) heap.emplace(r, i);
        }
        return heap;
    }

    // Merge every run into one, so that at most STREAM_MAX_RUNS files are open.
    void compact() {
        Heap heap = merger();
        StreamFile out = new_run();
        while (!heap.empty()) {
            auto [r, i] = heap.top();
            heap.pop();
            if (std::fwrite(r.data(), sizeof(uint64_t), width_, out.f) != width_) {
                throw std::runtime_error("cannot write a sort run in " + dir_);
            }
            StreamRecord n;
            if (read_record(runs_[i], n)) heap.emplace(n, i);
        }
        if (std::fflush(out.f) != 0) throw std::runtime_error("cannot write a sort run in " + dir_);
        runs_.clear();
        runs_.push_back(std::move(out));
    }

    void start() {
        reading_ = true;
        if (runs_.empty()) {
            stream_sort_records(buf_, width_);
            return;
        }
        spill();
        if (std::fflush(runs_.back().f) != 0) throw std::runtime_error("cannot write a sort run in " + dir_);
        heap_ = merger();
    }

    std::string dir_;
    size_t width_;
    size_t budget_;
    std::vector<uint64_t> buf_;
    std::vector<StreamFile> runs_;
    bool reading_ = false;
    size_t at_ = 0;
    Heap heap_;
};

struct StreamSorters {
    std::mutex m;
    std::map<int64_t, std::unique_ptr<StreamSorter>> table;
    int next = 1;
};

inline StreamSorters& stream_sorters() {
    static StreamSorters s;
    return s;
}

inline int stream_open_sorter(const std::string& dir, int64_t width, int64_t kib) {
    if (width < 1 || width > 4) throw std::runtime_error("openSorter: record width " + std::to_string(width) + " is not 1 to 4 words");
    if (kib < 1) throw std::runtime_error("openSorter: a memory budget of " + std::to_string(kib) + " KiB is less than 1");
    struct stat st;
    if (::stat(dir.c_str(), &st) != 0 || !S_ISDIR(st.st_mode)) throw std::runtime_error("openSorter: " + dir + " is not a directory");
    const size_t w = static_cast<size_t>(width);
    const size_t words = static_cast<size_t>(kib) * 1024 / 8;
    const size_t budget = std::max(words - words % w, w);
    auto& s = stream_sorters();
    std::lock_guard<std::mutex> g(s.m);
    const int h = s.next++;
    s.table.emplace(h, std::make_unique<StreamSorter>(dir, w, budget));
    return h;
}

inline std::string stream_sorter_push(int64_t h, const std::vector<uint64_t>& xs) {
    auto& s = stream_sorters();
    std::lock_guard<std::mutex> g(s.m);
    auto it = s.table.find(h);
    if (it == s.table.end()) return "pushSorter: no open sorter " + std::to_string(h);
    return it->second->push(xs);
}

inline void stream_push_sorter(int64_t h, const std::vector<uint64_t>& xs) {
    std::string e = stream_sorter_push(h, xs);
    if (!e.empty()) throw std::runtime_error(e);
}

inline std::tuple<bool, std::string, std::vector<uint64_t>> stream_next_sorted(int64_t h) {
    auto& s = stream_sorters();
    std::lock_guard<std::mutex> g(s.m);
    auto it = s.table.find(h);
    if (it == s.table.end()) return {false, "nextSorted: no open sorter " + std::to_string(h), {}};
    try {
        return {true, "", it->second->next()};
    } catch (const std::exception& e) {
        return {false, e.what(), {}};
    }
}

inline void stream_close_sorter(int64_t h) {
    auto& s = stream_sorters();
    std::lock_guard<std::mutex> g(s.m);
    s.table.erase(h);
}

#endif
