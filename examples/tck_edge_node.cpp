#include "tck_edge_node.hpp"

#include <chrono>
#include <cstring>
#include <format>
#include <iostream>
#include <sstream>

namespace sparkplug::tck {

namespace {

std::string trim(const std::string& str) {
  size_t first = str.find_first_not_of(" \t\n\r");
  if (first == std::string::npos)
    return "";
  size_t last = str.find_last_not_of(" \t\n\r");
  return str.substr(first, (last - first + 1));
}

std::vector<std::string> split(const std::string& str, char delimiter) {
  std::vector<std::string> tokens;
  std::stringstream ss(str);
  std::string token;
  while (std::getline(ss, token, delimiter)) {
    tokens.push_back(trim(token));
  }
  return tokens;
}

} // namespace

TCKEdgeNode::TCKEdgeNode(TCKEdgeNodeConfig config) : config_(std::move(config)) {
  int rc = MQTTAsync_create(&tck_client_, config_.broker_url.c_str(),
                            (config_.client_id_prefix + "_control").c_str(),
                            MQTTCLIENT_PERSISTENCE_NONE, nullptr);
  if (rc != MQTTASYNC_SUCCESS) {
    throw std::runtime_error(std::format("Failed to create MQTT client: {}", rc));
  }

  rc = MQTTAsync_setCallbacks(tck_client_, this, on_connection_lost, on_message_arrived,
                              on_delivery_complete);
  if (rc != MQTTASYNC_SUCCESS) {
    MQTTAsync_destroy(&tck_client_);
    throw std::runtime_error(std::format("Failed to set callbacks: {}", rc));
  }
}

TCKEdgeNode::~TCKEdgeNode() {
  stop();
  if (tck_client_) {
    MQTTAsync_destroy(&tck_client_);
  }
}

auto TCKEdgeNode::start() -> stdx::expected<void, std::string> {
  MQTTAsync_connectOptions conn_opts = MQTTAsync_connectOptions_initializer;
  conn_opts.keepAliveInterval = 60;
  conn_opts.cleansession = 1;
  conn_opts.onSuccess = on_connect_success;
  conn_opts.onFailure = on_connect_failure;
  conn_opts.context = this;

  if (!config_.username.empty()) {
    conn_opts.username = config_.username.c_str();
    conn_opts.password = config_.password.c_str();
  }

  int rc = MQTTAsync_connect(tck_client_, &conn_opts);
  if (rc != MQTTASYNC_SUCCESS) {
    return stdx::unexpected(std::format("Failed to start connect: {}", rc));
  }

  return {};
}

void TCKEdgeNode::stop() {
  if (running_) {
    running_ = false;

    if (edge_node_) {
      edge_node_->disconnect();
      edge_node_.reset();
    }

    if (connected_) {
      MQTTAsync_disconnectOptions disc_opts = MQTTAsync_disconnectOptions_initializer;
      disc_opts.timeout = 1000;
      MQTTAsync_disconnect(tck_client_, &disc_opts);
      connected_ = false;
    }
  }
}

auto TCKEdgeNode::is_running() const -> bool {
  return running_;
}

void TCKEdgeNode::on_connection_lost(void* context, char* cause) {
  auto* self = static_cast<TCKEdgeNode*>(context);
  std::cout << "[TCK] Connection lost";
  if (cause) {
    std::cout << ": " << cause;
  }
  std::cout << "\n";
  self->connected_ = false;
}

int TCKEdgeNode::on_message_arrived(void* context,
                                    char* topicName,
                                    int /*topicLen*/,
                                    MQTTAsync_message* message) {
  auto* self = static_cast<TCKEdgeNode*>(context);

  std::string topic(topicName);
  std::string payload(static_cast<char*>(message->payload), message->payloadlen);

  std::cout << "[TCK] Received: " << topic << " -> " << payload << "\n";

  if (topic == "SPARKPLUG_TCK/TEST_CONTROL") {
    self->handle_test_control(payload);
  } else if (topic == "SPARKPLUG_TCK/CONSOLE_PROMPT") {
    self->handle_console_prompt(payload);
  } else if (topic == "SPARKPLUG_TCK/CONFIG") {
    self->handle_config(payload);
  } else if (topic == "SPARKPLUG_TCK/RESULT_CONFIG") {
    self->handle_result_config(payload);
  }

  MQTTAsync_freeMessage(&message);
  MQTTAsync_free(topicName);
  return 1;
}

void TCKEdgeNode::on_delivery_complete(void* /*context*/, MQTTAsync_token /*token*/) {
}

void TCKEdgeNode::on_connect_success(void* context, MQTTAsync_successData* /*response*/) {
  auto* self = static_cast<TCKEdgeNode*>(context);
  self->connected_ = true;
  std::cout << "[TCK] Connected to broker\n";

  const char* topics[] = {"SPARKPLUG_TCK/TEST_CONTROL", "SPARKPLUG_TCK/CONSOLE_PROMPT",
                          "SPARKPLUG_TCK/CONFIG", "SPARKPLUG_TCK/RESULT_CONFIG"};
  int qos[] = {1, 1, 1, 1};

  MQTTAsync_responseOptions opts = MQTTAsync_responseOptions_initializer;
  opts.onSuccess = on_subscribe_success;
  opts.onFailure = on_subscribe_failure;
  opts.context = self;

  int rc = MQTTAsync_subscribeMany(self->tck_client_, 4, const_cast<char**>(topics), qos,
                                   &opts);
  if (rc != MQTTASYNC_SUCCESS) {
    std::cerr << "[TCK] Failed to subscribe: " << rc << "\n";
  }
}

void TCKEdgeNode::on_connect_failure(void* context, MQTTAsync_failureData* response) {
  auto* self = static_cast<TCKEdgeNode*>(context);
  std::cerr << "[TCK] Connection failed";
  if (response) {
    std::cerr << ": code=" << response->code;
    if (response->message) {
      std::cerr << ", message=" << response->message;
    }
  }
  std::cerr << "\n";
  self->connected_ = false;
}

void TCKEdgeNode::on_subscribe_success(void* context,
                                       MQTTAsync_successData* /*response*/) {
  auto* self = static_cast<TCKEdgeNode*>(context);
  std::cout << "[TCK] Subscribed to TCK control topics\n";
  self->running_ = true;
  std::cout << "[TCK] TCK Edge Node ready\n";
  std::cout << "[TCK] Waiting for test commands from TCK Console...\n";
}

void TCKEdgeNode::on_subscribe_failure(void* /*context*/,
                                       MQTTAsync_failureData* response) {
  std::cerr << "[TCK] Subscribe failed";
  if (response) {
    std::cerr << ": code=" << response->code;
    if (response->message) {
      std::cerr << ", message=" << response->message;
    }
  }
  std::cerr << "\n";
}

void TCKEdgeNode::handle_test_control(const std::string& message) {
  auto parts = split(message, ' ');
  if (parts.empty()) {
    return;
  }

  const std::string& command = parts[0];

  if (command == "NEW_TEST") {
    if (parts.size() < 3) {
      log("ERROR", "Invalid NEW_TEST command format");
      return;
    }

    const std::string& profile = parts[1];
    const std::string& test_type = parts[2];

    if (profile != "edge") {
      log("WARN", "Ignoring non-edge test: " + profile);
      return;
    }

    std::vector<std::string> params(parts.begin() + 3, parts.end());

    test_state_ = EdgeTestState::RUNNING;
    current_test_name_ = test_type;
    current_test_params_ = params;

    log("INFO", "Starting test: " + test_type);

    if (test_type == "SessionEstablishmentTest") {
      run_session_establishment_test(params);
    } else if (test_type == "SessionTerminationTest") {
      run_session_termination_test(params);
    } else if (test_type == "SendDataTest") {
      run_send_data_test(params);
    } else if (test_type == "SendComplexDataTest") {
      run_send_complex_data_test(params);
    } else if (test_type == "ReceiveCommandTest") {
      run_receive_command_test(params);
    } else if (test_type == "PrimaryHostTest") {
      run_primary_host_test(params);
    } else if (test_type == "MultipleBrokerTest") {
      run_multiple_broker_test(params);
    } else {
      log("ERROR", "Unknown test type: " + test_type);
      publish_result("OVERALL: NOT EXECUTED");
    }

  } else if (command == "END_TEST") {
    log("INFO", "Test end requested");

    if (test_state_ == EdgeTestState::RUNNING) {
      if (current_test_name_ == "SessionEstablishmentTest") {
        publish_result("OVERALL: PASS");
      }
    }

    if (edge_node_) {
      edge_node_->disconnect();
      edge_node_.reset();
    }

    test_state_ = EdgeTestState::IDLE;
    current_test_name_.clear();
    current_test_params_.clear();
    current_group_id_.clear();
    current_edge_node_id_.clear();
    device_ids_.clear();
  }
}

void TCKEdgeNode::handle_console_prompt(const std::string& message) {
  std::cout << "\n=== CONSOLE PROMPT ===\n";
  std::cout << message << "\n";
  std::cout << "======================\n";

  std::cout << "\nEnter response (PASS/FAIL): ";
  std::string response;
  std::getline(std::cin, response);
  response = trim(response);

  if (!response.empty()) {
    std::cout << "[TCK] Console reply: " << response << "\n";
    publish_console_reply(response);
  }
}

void TCKEdgeNode::handle_config(const std::string& message) {
  auto parts = split(message, ' ');
  if (parts.size() >= 2 && parts[0] == "UTCwindow") {
    config_.utc_window_ms = std::stoi(parts[1]);
    log("INFO", std::format("UTC window set to {} ms", config_.utc_window_ms));
  }
}

void TCKEdgeNode::handle_result_config(const std::string& message) {
  auto parts = split(message, ' ');
  if (parts.size() >= 2 && parts[0] == "NEW_RESULT-LOG") {
    log("INFO", "Result config: " + message);
  }
}

void TCKEdgeNode::run_session_establishment_test(const std::vector<std::string>& params) {
  if (params.size() < 2) {
    log("ERROR", "Missing parameters for SessionEstablishmentTest");
    publish_result("OVERALL: NOT EXECUTED");
    return;
  }

  const std::string& group_id = params[0];
  const std::string& edge_node_id = params[1];

  std::vector<std::string> device_ids;
  if (params.size() > 2 && !params[2].empty()) {
    device_ids = split(params[2], ' ');
  }

  try {
    auto result = create_edge_node(group_id, edge_node_id);
    if (!result) {
      log("ERROR", result.error());
      publish_result("OVERALL: FAIL");
      return;
    }

    device_ids_ = device_ids;
    log("INFO", "Edge Node session established successfully");

  } catch (const std::exception& e) {
    log("ERROR", std::string("Exception: ") + e.what());
    publish_result("OVERALL: FAIL");
  }
}

void TCKEdgeNode::run_session_termination_test(
    const std::vector<std::string>& /*params*/) {
  log("WARN", "SessionTerminationTest not yet implemented");
  publish_result("OVERALL: NOT EXECUTED");
}

void TCKEdgeNode::run_send_data_test(const std::vector<std::string>& /*params*/) {
  log("WARN", "SendDataTest not yet implemented");
  publish_result("OVERALL: NOT EXECUTED");
}

void TCKEdgeNode::run_send_complex_data_test(const std::vector<std::string>& /*params*/) {
  log("WARN", "SendComplexDataTest not yet implemented");
  publish_result("OVERALL: NOT EXECUTED");
}

void TCKEdgeNode::run_receive_command_test(const std::vector<std::string>& /*params*/) {
  log("WARN", "ReceiveCommandTest not yet implemented");
  publish_result("OVERALL: NOT EXECUTED");
}

void TCKEdgeNode::run_primary_host_test(const std::vector<std::string>& /*params*/) {
  log("WARN", "PrimaryHostTest not yet implemented");
  publish_result("OVERALL: NOT EXECUTED");
}

void TCKEdgeNode::run_multiple_broker_test(const std::vector<std::string>& /*params*/) {
  log("WARN", "MultipleBrokerTest not yet implemented");
  publish_result("OVERALL: NOT EXECUTED");
}

auto TCKEdgeNode::create_edge_node(const std::string& group_id,
                                   const std::string& edge_node_id)
    -> stdx::expected<void, std::string> {
  std::lock_guard<std::mutex> lock(mutex_);

  if (edge_node_) {
    return stdx::unexpected("Edge Node already exists");
  }

  log("INFO", std::format("Creating Edge Node group_id={}, edge_node_id={}", group_id,
                          edge_node_id));

  current_group_id_ = group_id;
  current_edge_node_id_ = edge_node_id;

  try {
    EdgeNode::Config config{.broker_url = config_.broker_url,
                            .client_id = edge_node_id + "_client",
                            .group_id = group_id,
                            .edge_node_id = edge_node_id};

    if (!config_.username.empty()) {
      config.username = config_.username;
      config.password = config_.password;
    }

    edge_node_ = std::make_unique<EdgeNode>(std::move(config));

    log("INFO", "Connecting Edge Node to broker");
    auto connect_result = edge_node_->connect();
    if (!connect_result) {
      return stdx::unexpected("Failed to connect: " + connect_result.error());
    }

    log("INFO", "Publishing NBIRTH");
    PayloadBuilder birth;
    birth.add_metric_with_alias("TestMetric", 1, 42.0);

    auto birth_result = edge_node_->publish_birth(birth);
    if (!birth_result) {
      return stdx::unexpected("Failed to publish NBIRTH: " + birth_result.error());
    }

    log("INFO", "Edge Node created and NBIRTH published");
    return {};

  } catch (const std::exception& e) {
    return stdx::unexpected(std::string("Exception: ") + e.what());
  }
}

void TCKEdgeNode::log(const std::string& level, const std::string& message) {
  std::string log_msg = "[" + level + "] " + message;
  std::cout << log_msg << "\n";
  (void)publish_tck("SPARKPLUG_TCK/LOG", log_msg, 0);
}

void TCKEdgeNode::publish_result(const std::string& result) {
  std::cout << "[TCK] Result: " << result << "\n";
  (void)publish_tck("SPARKPLUG_TCK/RESULT", result, 1);
  test_state_ = EdgeTestState::COMPLETED;
}

void TCKEdgeNode::publish_console_reply(const std::string& reply) {
  std::cout << "[TCK] Console reply: " << reply << "\n";
  (void)publish_tck("SPARKPLUG_TCK/CONSOLE_REPLY", reply, 1);
}

auto TCKEdgeNode::publish_tck(const std::string& topic,
                              const std::string& payload,
                              int qos) -> stdx::expected<void, std::string> {
  if (!connected_) {
    return stdx::unexpected("Not connected");
  }

  MQTTAsync_message msg = MQTTAsync_message_initializer;
  msg.payload = const_cast<char*>(payload.c_str());
  msg.payloadlen = static_cast<int>(payload.size());
  msg.qos = qos;
  msg.retained = 0;

  MQTTAsync_responseOptions opts = MQTTAsync_responseOptions_initializer;
  opts.context = this;

  int rc = MQTTAsync_sendMessage(tck_client_, topic.c_str(), &msg, &opts);
  if (rc != MQTTASYNC_SUCCESS) {
    return stdx::unexpected(std::format("Failed to publish: {}", rc));
  }

  return {};
}

auto TCKEdgeNode::get_timestamp() -> uint64_t {
  return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
                                   std::chrono::system_clock::now().time_since_epoch())
                                   .count());
}

} // namespace sparkplug::tck
