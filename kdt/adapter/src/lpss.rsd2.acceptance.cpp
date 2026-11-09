#include <atomic>
#include <charconv>
#include <chrono>
#include <deque>
#include <functional>
#include <future>
#include <iostream>
#include <mutex>
#include <shared_mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>

#include <rmvl/lpss/node.hpp>
#include <rmvlmsg/std/string.hpp>

using namespace std::chrono_literals;
using Message = rm::msg::String;
using Callback = std::function<void(const Message &)>;

namespace {

struct State {
    std::atomic_int peer_stage{-1};
    std::atomic_int local_stage{-1};
    std::atomic_bool data_received{};
};

void receiveStage(State &state, const std::string &prefix, const Message &msg) {
    if (msg.data.compare(0, prefix.size(), prefix) != 0)
        return;
    const auto text = std::string_view(msg.data).substr(prefix.size());
    int stage{};
    const auto [end, error] = std::from_chars(text.data(), text.data() + text.size(), stage);
    if (error != std::errc{} || end != text.data() + text.size())
        return;
    int current = state.peer_stage.load();
    while (current < stage && !state.peer_stage.compare_exchange_weak(current, stage)) {}
}

class SyncProbe final : public rm::lpss::Node {
public:
    using Node::Node;

    bool hasPeer() {
        std::shared_lock lock(_nodes_mtx);
        return _discovered_nodes.size() == 1;
    }

    std::pair<size_t, size_t> counts(const std::string &writer, const std::string &reader) {
        std::shared_lock lock(_discovered_mtx);
        const auto wi = _discovered_writers.find(writer);
        const auto ri = _discovered_readers.find(reader);
        return {
            wi == _discovered_writers.end() ? 0 : wi->second.writers.size(),
            ri == _discovered_readers.end() ? 0 : ri->second.readers.size()
        };
    }
};

struct SyncBackend {
    SyncProbe node;
    rm::lpss::Publisher<Message> control_pub{nullptr};
    rm::lpss::Subscriber<Message> control_sub{nullptr};
    rm::lpss::Publisher<Message> data_pub{nullptr};
    rm::lpss::Subscriber<Message> data_sub{nullptr};
    std::string peer_writer;
    std::string peer_reader;

    SyncBackend(char role, uint8_t domain) : node(role == 'A' ? "rsd2_accept_sync_A" : "rsd2_accept_sync_B", domain) {}

    void startMonitoring() {}
    bool hasPeer() { return node.hasPeer(); }
    std::pair<size_t, size_t> counts() { return node.counts(peer_writer, peer_reader); }

    bool startControl(const std::string &own, const std::string &peer, Callback callback) {
        control_pub = node.createPublisher<Message>(own);
        control_sub = node.createSubscriber<Message>(peer, std::move(callback));
        return !control_pub.invalid() && !control_sub.invalid();
    }
    void sendControl(const std::string &payload) {
        Message msg{};
        msg.data = payload;
        control_pub.publish(msg);
    }
    bool createData(const std::string &own, const std::string &peer, Callback callback) {
        data_pub = node.createPublisher<Message>(own);
        data_sub = node.createSubscriber<Message>(peer, std::move(callback));
        return !data_pub.invalid() && !data_sub.invalid();
    }
    void sendData(const Message &msg) { data_pub.publish(msg); }
    void removeData() {
        node.destroyPublisher(data_pub);
        node.destroySubscriber(data_sub);
    }
};

#if __cplusplus >= 202002L
class AsyncProbe final : public rm::lpss::async::Node {
public:
    using Node::Node;

    std::atomic_bool monitoring{true};
    std::atomic_bool peer_found{};
    std::atomic_size_t writers{}, readers{};

    // 异步 Node 操作及端点状态访问均在 IO 线程执行
    void start(std::string writer, std::string reader) {
        co_spawn(_ctx, &AsyncProbe::watchTask, this, std::move(writer), std::move(reader));
        co_spawn(_ctx, &AsyncProbe::runCommands, this);
    }

    void submit(std::function<void()> command) {
        std::lock_guard lock(_commands_mtx);
        _commands.push_back(std::move(command));
    }

private:
    std::mutex _commands_mtx;
    std::deque<std::function<void()>> _commands;

    rm::async::Task<> runCommands() {
        rm::async::Timer timer(_ctx);
        while (monitoring.load()) {
            std::deque<std::function<void()>> pending;
            {
                std::lock_guard lock(_commands_mtx);
                pending.swap(_commands);
            }
            for (auto &command : pending)
                command();
            co_await timer.sleep_for(1ms);
        }
    }

    rm::async::Task<> watchTask(std::string writer, std::string reader) {
        rm::async::Timer timer(_ctx);
        while (monitoring.load()) {
            peer_found.store(_discovered_nodes.size() == 1);
            const auto wi = _discovered_writers.find(writer);
            const auto ri = _discovered_readers.find(reader);
            writers.store(wi == _discovered_writers.end() ? 0 : wi->second.writers.size());
            readers.store(ri == _discovered_readers.end() ? 0 : ri->second.readers.size());
            co_await timer.sleep_for(5ms);
        }
    }
};

struct AsyncBackend {
    AsyncProbe node;
    std::thread worker;
    rm::lpss::async::Publisher<Message>::ptr control_pub{}, data_pub{};
    rm::lpss::async::Subscriber<Message>::ptr control_sub{}, data_sub{};
    std::string peer_writer, peer_reader;

    AsyncBackend(char role, uint8_t domain)
        : node(role == 'A' ? "rsd2_accept_async_A" : "rsd2_accept_async_B", domain) {}

    template <typename Fn>
    auto onIo(Fn fn) -> decltype(fn()) {
        using Result = decltype(fn());
        auto task = std::make_shared<std::packaged_task<Result()>>(std::move(fn));
        auto future = task->get_future();
        node.submit([task] { (*task)(); });
        if (future.wait_for(10s) != std::future_status::ready)
            throw std::runtime_error("RSD2 async Node IO thread is unresponsive");
        return future.get();
    }

    void startMonitoring() {
        node.start(peer_writer, peer_reader);
        worker = std::thread([this] { node.spin(); });
    }

    ~AsyncBackend() {
        if (worker.joinable()) {
            onIo([this] {
                node.monitoring.store(false);
                node.shutdown();
            });
            worker.join();
        }
    }

    bool hasPeer() { return node.peer_found.load(); }
    std::pair<size_t, size_t> counts() { return {node.writers.load(), node.readers.load()}; }

    bool startControl(const std::string &own, const std::string &peer, Callback callback) {
        return onIo([this, own, peer, callback = std::move(callback)]() mutable {
            control_pub = node.createPublisher<Message>(own);
            control_sub = node.createSubscriber<Message>(peer, std::move(callback));
            return control_pub && !control_pub->invalid() && control_sub && !control_sub->invalid();
        });
    }

    void sendControl(const std::string &payload) {
        onIo([this, payload] {
            Message msg{};
            msg.data = payload;
            control_pub->publish(msg);
        });
    }

    bool createData(const std::string &own, const std::string &peer, Callback callback) {
        return onIo([this, own, peer, callback = std::move(callback)]() mutable {
            data_pub = node.createPublisher<Message>(own);
            data_sub = node.createSubscriber<Message>(peer, std::move(callback));
            return data_pub && !data_pub->invalid() && data_sub && !data_sub->invalid();
        });
    }

    void sendData(const Message &msg) {
        onIo([this, msg] { data_pub->publish(msg); });
    }

    void removeData() {
        onIo([this] {
            node.destroyPublisher(data_pub);
            node.destroySubscriber(data_sub);
        });
    }
};
#endif

template <typename Predicate>
bool waitFor(Predicate predicate, std::chrono::milliseconds timeout = 5s) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    do {
        if (predicate())
            return true;
        std::this_thread::sleep_for(5ms);
    } while (std::chrono::steady_clock::now() < deadline);
    return false;
}

template <typename Backend, typename Action>
bool barrier(Backend &node, State &state, const std::string &prefix,
             int stage, int cycle, Action action, std::chrono::milliseconds timeout = 5s) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    const auto payload = prefix + std::to_string(stage);
    state.local_stage.store(stage);
    do {
        action();
        node.sendControl(payload);
        if (state.peer_stage.load() >= stage)
            return true;
        std::this_thread::sleep_for(30ms);
    } while (std::chrono::steady_clock::now() < deadline);
    std::cerr << "FAIL barrier stage=" << stage << " cycle=" << cycle
              << " peer_stage=" << state.peer_stage.load() << '\n';
    return false;
}

template <typename Backend>
bool waitCount(Backend &node, size_t expected, int cycle) {
    if (waitFor([&] { return node.counts() == std::make_pair(expected, expected); }))
        return true;
    const auto [writers, readers] = node.counts();
    std::cerr << "FAIL endpoints cycle=" << cycle << " expected=" << expected
              << " writers=" << writers << " readers=" << readers << '\n';
    return false;
}

template <typename Backend>
int run(Backend &node, State &state, char role, const std::string &mode, int cycles) {
    const char peer = role == 'A' ? 'B' : 'A';
    const std::string base = "/rsd2/accept/" + mode + "/";
    const std::string own_control = base + "control/" + role;
    const std::string peer_control = base + "control/" + peer;
    const std::string own_data = base + "data/" + role;
    const std::string peer_data = base + "data/" + peer;
    const std::string control_prefix = "RSD2-" + mode + ":";
    node.peer_writer = peer_data;
    node.peer_reader = own_data;
    node.startMonitoring();
    if (!node.startControl(own_control, peer_control,
                           [&](const Message &msg) { receiveStage(state, control_prefix, msg); })) {
        std::cerr << "FAIL control endpoints\n";
        return 1;
    }

    // 持续广播当前阶段，等待双方同步
    std::atomic_bool pumping{true};
    std::thread control_pump([&] {
        while (pumping.load()) {
            const int stage = state.local_stage.load();
            if (stage >= 0)
                node.sendControl(control_prefix + std::to_string(stage));
            std::this_thread::sleep_for(30ms);
        }
    });
    struct StopPump {
        std::atomic_bool &running;
        std::thread &worker;
        ~StopPump() {
            running.store(false);
            worker.join();
        }
    } stop_pump{pumping, control_pump};

    if (!waitFor([&] { return node.hasPeer(); }, 20s) ||
        !barrier(node, state, control_prefix, 0, 0, [] {}, 20s)) {
        std::cerr << "FAIL ND02/ED02 peer discovery\n";
        return 1;
    }

    const std::string expected = "from_" + std::string(1, peer);
    if (!node.createData(own_data, peer_data, [&](const Message &msg) {
            if (msg.data == expected)
                state.data_received.store(true);
        }) ||
        !waitCount(node, 1, 0) ||
        !barrier(node, state, control_prefix, 1, 0, [] {}))
        return 1;

    Message sample{};
    sample.data = "from_" + std::string(1, role);
    auto publishData = [&] {
        node.sendData(sample);
        std::this_thread::sleep_for(35ms);
    };
    const auto data_ready = [&] {
        publishData();
        return state.data_received.load();
    };
    if (!waitFor(data_ready, 10s) ||
        !barrier(node, state, control_prefix, 2, 0, publishData)) {
        std::cerr << "FAIL bidirectional MT02 data\n";
        return 1;
    }

    node.removeData();
    if (!waitCount(node, 0, 0) || !barrier(node, state, control_prefix, 3, 0, [] {}))
        return 1;

    for (int cycle = 0; cycle < cycles; ++cycle) {
        if (!node.createData(own_data, peer_data, [](const Message &) {}) ||
            !waitCount(node, 1, cycle + 1) ||
            !barrier(node, state, control_prefix, 4 + 2 * cycle, cycle + 1, [] {}))
            return 1;
        node.removeData();
        if (!waitCount(node, 0, cycle + 1) ||
            !barrier(node, state, control_prefix, 5 + 2 * cycle, cycle + 1, [] {}))
            return 1;
    }
    const int final_stage = 4 + 2 * cycles;
    if (!barrier(node, state, control_prefix, final_stage, cycles, [] {}))
        return 1;
    // 重复发送最终阶段号，供对端完成同步
    const auto until = std::chrono::steady_clock::now() + 500ms;
    while (std::chrono::steady_clock::now() < until) {
        node.sendControl(control_prefix + std::to_string(final_stage));
        std::this_thread::sleep_for(30ms);
    }
    std::cout << "PASS COMPLETE " << role << " mode=" << mode
              << " cycles=" << cycles << " additions=" << cycles
              << " removals=" << cycles
              << " ND02=pass MT02=pass" << std::endl;
    std::cout << "KDT_RESULT: PASS COMPLETE " << role << " " << mode
              << " " << cycles << std::endl;
    return 0;
}

} // namespace

int main(int argc, char **argv) {
    if ((argc != 3 && argc != 4) ||
        (std::string_view(argv[1]) != "A" && std::string_view(argv[1]) != "B") ||
        (std::string_view(argv[2]) != "sync" && std::string_view(argv[2]) != "async")) {
        std::cerr << "Usage: lpss.rsd2.acceptance A|B sync|async [cycles]\n";
        return 2;
    }
    const char role = argv[1][0];
    const std::string mode = argv[2];
    int cycles = 200;
    if (argc == 4) {
        const std::string_view value(argv[3]);
        const auto [end, error] = std::from_chars(value.data(), value.data() + value.size(), cycles);
        if (error != std::errc{} || end != value.data() + value.size() || cycles < 1 || cycles > 200)
            return 2;
    }
    State state{};
    if (mode == "sync") {
        SyncBackend node(role, 121);
        return run(node, state, role, mode, cycles);
    }
#if __cplusplus >= 202002L
    AsyncBackend node(role, 122);
    return run(node, state, role, mode, cycles);
#else
    std::cerr << "Async Node requires C++20\n";
    return 2;
#endif
}
