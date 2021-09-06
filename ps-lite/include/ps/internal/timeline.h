#ifndef PS_INTERNAL_TIMELINE_H_
#define PS_INTERNAL_TIMELINE_H_
#include <atomic>
#include <chrono>
#include <fstream>
#include <iostream>
#include <string>
#include <thread>

#include <boost/lockfree/spsc_queue.hpp>

#include "ps/internal/message.h"
#include "ps/internal/threadsafe_queue.h"

namespace ps {

struct TimelineRecord {
  int key;
  std::string op_name;
  std::string comm_name;
  std::string process_name;
  uint64_t comm_time;
  uint64_t queue_time;
  uint64_t request_begin;
  uint64_t request_end;
  uint64_t response_begin;
  uint64_t response_end;
  uint32_t ts;
  uint32_t sender;
  uint32_t recver;
  uint32_t data_size;
  uint32_t request_data_size;
  int layer_index;
  int server_rank;
  int priority;
  DataType data_type;
};

struct DeviceStats {
  /*! \brief device name */
  std::string dev_name_;
  /*! \brief device rank */
  int my_rank = -1;
  /*! \brief device roles */
  std::string roles[3] = {"worker_rank_", "server_rank_", "scheduler_rank_"};
  /*! \brief app_id */
  int my_app_id = -1;
  /*! \brief customer_id */
  int my_customer_id = -1;
  /*! \brief node_id */
  int my_node_id = -1;
  /*! \brief device role */
  int my_role = -1;

  DeviceStats() {}
  //DeviceStats(int rank, int app_id, int customer_id, int role) : my_rank(rank),my_app_id(app_id),my_customer_id(customer_id),my_role(role) {
    //dev_name_ = roles[role] + std::to_string(rank);
  //}
  ~DeviceStats() {}

  void set_attr(int rank, int app_id, int customer_id, int role) {
    my_rank = rank;
    my_app_id = app_id;
    my_customer_id = customer_id;
    my_role = role;
    //dev_name_ = roles[role] + std::to_string(rank);
  }

  std::string to_string() {
    std::string devicestats = "DeviceStats: my_rank " + std::to_string(my_rank) + ", my_app_id " + std::to_string(my_rank) + ", my_customer_id " + std::to_string(my_customer_id) + ", my_role " + std::to_string(my_role);
    return devicestats;
  }
};

class TimelineWriter {
 public:
  //void Init(int rank, int app_id, int customer_id, std::shared_ptr<DeviceStats>& pstate);
  void Init(int rank, int app_id, int customer_id, DeviceStats& pstate);
  void InitFile(const Meta&);
  inline bool IsRunning() const { return is_running_; }
  inline uint64_t EventCount() const { return timeline_event_count_; }
  void EnqueueWriteRecord(const Meta&);
  void Shutdown();

 private:
  void DoWriteRecord(const TimelineRecord& tr);
  void WriteLoop();
  void EmitPid();
  void EmitEvents(const TimelineRecord& tr);
  void EmitEnd();
  std::ofstream file_;
  std::atomic<bool> is_running_{false};
  boost::lockfree::spsc_queue<TimelineRecord, boost::lockfree::capacity<1048576>> record_queue_;
  std::unique_ptr<std::thread> writer_thread_;
  /*! \brief device statistics */
  //std::shared_ptr<DeviceStats> profile_stat;
  DeviceStats profile_stat;
  /* \brief Number of events written */
  volatile uint64_t timeline_event_count_;
};

class Timeline {
 public:
  void Init(int rank, int app_id, int customer_id, int role);
  void Write(const Meta& meta);
  long long TimeSinceStartMicros() const;
  long long TimeFromUTC() const;
  void Shutdown();
  bool IsInitialized() const { return is_initialized_; }

 private:
  bool is_initialized_{false};
  TimelineWriter writer_;
  std::chrono::high_resolution_clock::time_point start_time_;
  std::recursive_mutex mu_;
  /*! \brief device statistics */
  //std::shared_ptr<DeviceStats> profile_stat;
  DeviceStats profile_stat;
};
}  // namespace ps

#endif