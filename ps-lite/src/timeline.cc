#include "ps/internal/timeline.h"
#include "ps/internal/postoffice.h"
namespace ps {
//void TimelineWriter::Init(int rank, int app_id, int customer_id, std::shared_ptr<DeviceStats>& pstate) {
void TimelineWriter::Init(int rank, int app_id, int customer_id, DeviceStats& pstate) {
  timeline_event_count_ = 0;
  //profile_stat = std::move(pstate);
  profile_stat = pstate;
  //std::string role = profile_stat->my_role == 0 ? "worker_" : "server_";
  // std::string role = profile_stat.my_role == 0 ? "worker_" : "server_";
  // std::string file_name = role + "rank_" + std::to_string(rank) + "-app_id_" + std::to_string(app_id) +
  //                         "-customer_id_" + std::to_string(customer_id);
  // LOG(WARNING) << "BEFORE OPEN FILE"; //test
  // file_.open(file_name, std::ios::out | std::ios::trunc);
  // LOG(WARNING) << "AFTER OPEN FILE"; //test
  // if (file_.good()) {
  //   is_running_ = true;
  //   writer_thread_ =
  //       std::unique_ptr<std::thread>(new std::thread(&TimelineWriter::WriteLoop, this));
  // } else {
  //   LOG(ERROR) << "Error opening PS-Lite timeline file " << file_name
  //              << ", will not write a timeline.";
  // }
}

void TimelineWriter::EnqueueWriteRecord(const Meta& meta) {
  // CHECK_NE(meta.request_begin, 0);
  // CHECK_NE(meta.request_end, 0);
  // CHECK_NE(meta.response_begin, 0);
  // CHECK_NE(meta.response_end, 0);
  TimelineRecord tr;
  tr.sender = meta.sender;
  tr.recver = meta.recver;
  tr.ts = meta.timestamp;
  tr.request_begin = meta.request_begin;
  tr.request_end = meta.request_end;
  tr.response_begin = meta.response_begin;
  tr.response_end = meta.response_end;
  tr.queue_time = meta.response_begin - meta.request_end;
  tr.comm_time = (meta.response_end - meta.request_begin) - tr.queue_time;
  tr.data_size = meta.data_size;
  tr.request_data_size = meta.request_data_size;
  tr.layer_index = meta.layer_index;
  tr.server_rank = meta.server_rank;
  std::string opname = "";
  std::string commname = "";
  std::string processname = "";
  if (meta.push) {
    opname += "push_";
    commname += "push_";
    processname += "push_";
  }
  if (meta.pull) {
    opname += "pull_";
    commname += "pull_";
    processname += "pull_";
  }
  opname += "pslite_comm";
  commname += "comm_time";
  processname += "server_queue_time";
  // if (meta.request) {
  //   ss += "_request";
  // } else {
  //   ss += "_response";
  // }
  tr.op_name = opname;
  tr.comm_name = commname;
  tr.process_name = processname;
  tr.data_size = meta.data_size;
  record_queue_.push(tr);
}

void TimelineWriter::EmitPid() {
  file_ << "        {\n"
        << "            \"ph\": \"" << "M" <<  "\",\n"
        << "            \"args\": {\n"
        //<< "                \"name\": \"" << profile_stat->dev_name_ << "\"\n"
        << "                \"name\": \"" << profile_stat.dev_name_ << "\"\n"
        << "            },\n"
        //<< "            \"pid\": " << profile_stat->my_rank << ",\n"
        << "            \"pid\": " << profile_stat.my_rank << ",\n"
        << "            \"name\": \"process_name\"\n"
        << "        },\n";
}

void TimelineWriter::EmitEvents(const TimelineRecord& tr) {
  if (timeline_event_count_ == 1) {
    file_ << "    {\n";
  } else {
    file_ << ",\n    {\n";
  }
  file_ << "        \"name\": \"" << tr.op_name << "\",\n"
          << "        \"ph\": \"" << "B" << "\",\n"
          << "        \"ts\": " << tr.request_begin << ",\n"
          << "        \"pid\": " << tr.layer_index << ",\n"
          << "        \"tid\": " << tr.server_rank << "\n"
          << "    },\n    {\n";
    file_ << "        \"name\": \"" << tr.comm_name << "\",\n"
          << "        \"ph\": \"" << "B" << "\",\n"
          << "        \"ts\": " << tr.request_begin << ",\n"
          << "        \"pid\": " << tr.layer_index << ",\n"
          << "        \"tid\": " << tr.server_rank << "\n"
          << "    },\n    {\n";
    file_ << "        \"ph\": \"" << "E" << "\",\n"
          << "        \"ts\": " << tr.request_begin+tr.comm_time << ",\n"
          << "        \"pid\": " << tr.layer_index << ",\n"
          << "        \"tid\": " << tr.server_rank << "\n"
          << "    },\n    {\n";
    file_ << "        \"name\": \"" << tr.process_name << "\",\n"
          << "        \"ph\": \"" << "B" << "\",\n"
          << "        \"ts\": " << tr.request_begin+tr.comm_time << ",\n"
          << "        \"pid\": " << tr.layer_index << ",\n"
          << "        \"tid\": " << tr.server_rank << "\n"
          << "    },\n    {\n";
    file_ << "        \"ph\": \"" << "E" << "\",\n"
          << "        \"ts\": " << tr.request_begin+tr.comm_time+tr.queue_time << ",\n"
          << "        \"pid\": " << tr.layer_index << ",\n"
          << "        \"tid\": " << tr.server_rank << "\n"
          << "    },\n    {\n";
    file_ << "        \"ph\": \"" << "E" << "\",\n"
          << "        \"ts\": " << tr.response_end << ",\n"
          << "        \"pid\": " << tr.layer_index << ",\n"
          << "        \"tid\": " << tr.server_rank << ",\n"
          << "        \"args\": {\n"
          << "            \"comm_time\": \"" << tr.comm_time << "\",\n"
          << "            \"queue_time\": \"" << tr.queue_time << "\",\n"
          << "            \"request_data_size\": \"" << tr.request_data_size << "\",\n"
          << "            \"response_data_size\": \"" << tr.data_size << "\"\n"
          << "        }\n"
          << "    }";
  // file_ << "        \"name\": \"" << tr.op_name << "\",\n"
  //       << "        \"ph\": \"" << "X" << "\",\n"
  //       << "        \"ts\": " << tr.request_begin << ",\n"
  //       << "        \"dur\": " << tr.response_end-tr.request_begin << ",\n"
  //       //<< "        \"pid\": " << profile_stat->my_rank << ",\n"
  //       << "        \"pid\": " << profile_stat.my_rank << ",\n"
  //       << "        \"tid\": " << tr.layer_index << ",\n"
  //       << "        \"args\": {\n"
  //       << "            \"request_data_size\": \"" << tr.request_data_size << "\",\n"
  //       << "            \"response_data_size\": \"" << tr.data_size << "\",\n"
  //       << "            \"comm_time\": \"" << tr.comm_time << "\",\n"
  //       << "            \"queue_time\": \"" << tr.queue_time << "\",\n"
  //       << "            \"request_begin\": \"" << tr.request_end << "\",\n"
  //       << "            \"request_end\": \"" << tr.request_end << "\",\n"
  //       << "            \"response_begin\": \"" << tr.response_begin << "\",\n"
  //       << "            \"response_end\": \"" << tr.response_begin << "\"\n"
  //       << "        }\n"
  //       << "    }";
}

void TimelineWriter::EmitEnd() {
  file_ << "\n" << std::endl;
  file_ << "    ]," << std::endl;
  file_ << "    \"displayTimeUnit\": \"ms\"" << std::endl;
  file_ << "}" << std::endl;
}

void TimelineWriter::DoWriteRecord(const TimelineRecord& tr) {
  if (++timeline_event_count_ == 1) {
    file_ << "{" << std::endl;
    file_ << "    \"traceEvents\": [" << std::endl;
    //EmitPid();
  }
  EmitEvents(tr);
}

void TimelineWriter::WriteLoop() {
  while (is_running_) {
    while (is_running_ && !record_queue_.empty()) {
      TimelineRecord tr = record_queue_.front();
      DoWriteRecord(tr);
      if (!file_.good()) {
        LOG(ERROR) << "Error writing to the PS-Lite timeline after it was "
                      "successfully opened, will stop writing the timeline.";
        is_running_ = false;
      }
      record_queue_.pop();
    }
  }
}

void TimelineWriter::Shutdown() {
  is_running_ = false;
  EmitEnd();
  try {
    if (writer_thread_->joinable()) {
      writer_thread_->join();
    }
  } catch (const std::system_error& e) {
    LOG(INFO) << "Caught system_error while joining writer thread. Code " << e.code() << " meaning "
              << e.what();
  }
}

void Timeline::Init(int rank, int app_id, int customer_id, int role) {
  if (is_initialized_) {
    return;
  }
  start_time_ = std::chrono::high_resolution_clock::now();
  LOG(INFO) << "ALL START";
  // if (!Postoffice::Get()->is_worker()) {
  //   return;
  // }
  LOG(WARNING) << "BEFORE INIT profile_stat";
  //profile_stat = std::make_shared<DeviceStats>(rank, app_id, customer_id, role);
  //profile_stat.set_attr(rank, app_id, customer_id, role);
  profile_stat.my_app_id = app_id;
  profile_stat.my_customer_id = customer_id;
  LOG(WARNING) << "after INIT profile_stat";
  LOG(WARNING) << "BEFORE INIT WRITER"; //test
  writer_.Init(rank, app_id, customer_id, profile_stat);
  LOG(WARNING) << "AFTER INIT WRITER"; //test
  //is_initialized_ = writer_.IsRunning();
  is_initialized_ = true;
  CHECK_EQ(is_initialized_, true);
  LOG(INFO) << "START SUCCESS";
}

void TimelineWriter::InitFile(const Meta& meta) {
  if (Postoffice::Get()->is_worker()) {
    profile_stat.my_role = 0;
  } else if (Postoffice::Get()->is_server()) {
    profile_stat.my_role = 1;
  } else if (Postoffice::Get()->is_scheduler()) {
    profile_stat.my_role = 2;
  }
  profile_stat.my_rank = Postoffice::Get()->my_rank();
  profile_stat.my_node_id = profile_stat.my_role == 0 ? Postoffice::Get()->WorkerRankToID(profile_stat.my_rank) : Postoffice::Get()->ServerRankToID(profile_stat.my_rank);
  
  std::string role = profile_stat.my_role == 0 ? "worker_" : "server_";
  profile_stat.dev_name_ = profile_stat.roles[profile_stat.my_role] + std::to_string(profile_stat.my_rank);
  std::string file_name = role + "rank_" + std::to_string(profile_stat.my_rank) + "-node_id_" + std::to_string(profile_stat.my_node_id);
  LOG(WARNING) << "BEFORE OPEN FILE"; //test
  file_.open(file_name, std::ios::out | std::ios::trunc);
  LOG(WARNING) << "AFTER OPEN FILE"; //test
  if (file_.good()) {
    is_running_ = true;
    writer_thread_ =
        std::unique_ptr<std::thread>(new std::thread(&TimelineWriter::WriteLoop, this));
  } else {
    LOG(ERROR) << "Error opening PS-Lite timeline file " << file_name
               << ", will not write a timeline.";
  }
}

void Timeline::Write(const Meta& meta) {
  if (!is_initialized_) {
    return;
  }
  if(writer_.EventCount() == 0) {
    writer_.InitFile(meta);
  }
  if (!writer_.IsRunning()) {
    return;
  }
  //LOG(INFO) << meta.DebugString();
  writer_.EnqueueWriteRecord(meta);
}

long long Timeline::TimeSinceStartMicros() const {
  auto now = std::chrono::high_resolution_clock::now();
  auto ts = now - start_time_;
  return std::chrono::duration_cast<std::chrono::microseconds>(ts).count();
}

long long Timeline::TimeFromUTC() const {
  auto now = std::chrono::high_resolution_clock::now();
  return now.time_since_epoch().count();
}

void Timeline::Shutdown() {
  std::lock_guard<std::recursive_mutex> lk(mu_);
  if (!is_initialized_) {
    return;
  }
  is_initialized_ = false;
  writer_.Shutdown();
}

}  // namespace ps