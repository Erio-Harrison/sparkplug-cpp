#pragma once

#include <atomic>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include <MQTTAsync.h>
#include <sparkplug/edge_node.hpp>
#include <sparkplug/payload_builder.hpp>

namespace sparkplug::tck {

struct TCKEdgeNodeConfig {
  std::string broker_url = "tcp://localhost:1883";
  std::string username;
  std::string password;
  std::string client_id_prefix = "tck_edge_node";
  std::string group_id = "tck_group";
  std::string edge_node_id = "tck_edge";
  std::string namespace_prefix = "spBv1.0";
  int utc_window_ms = 5000;
};

enum class EdgeTestState { IDLE, RUNNING, COMPLETED, FAILED };

class TCKEdgeNode {
public:
  explicit TCKEdgeNode(TCKEdgeNodeConfig config);
  ~TCKEdgeNode();

  [[nodiscard]] auto start() -> stdx::expected<void, std::string>;
  void stop();
  [[nodiscard]] auto is_running() const -> bool;

private:
  static void on_connection_lost(void* context, char* cause);
  static int on_message_arrived(void* context,
                                char* topicName,
                                int topicLen,
                                MQTTAsync_message* message);
  static void on_delivery_complete(void* context, MQTTAsync_token token);
  static void on_connect_success(void* context, MQTTAsync_successData* response);
  static void on_connect_failure(void* context, MQTTAsync_failureData* response);
  static void on_subscribe_success(void* context, MQTTAsync_successData* response);
  static void on_subscribe_failure(void* context, MQTTAsync_failureData* response);

  void handle_test_control(const std::string& message);
  void handle_console_prompt(const std::string& message);
  void handle_config(const std::string& message);
  void handle_result_config(const std::string& message);

  void run_session_establishment_test(const std::vector<std::string>& params);
  void run_session_termination_test(const std::vector<std::string>& params);
  void run_send_data_test(const std::vector<std::string>& params);
  void run_send_complex_data_test(const std::vector<std::string>& params);
  void run_receive_command_test(const std::vector<std::string>& params);
  void run_primary_host_test(const std::vector<std::string>& params);
  void run_multiple_broker_test(const std::vector<std::string>& params);

  [[nodiscard]] auto
  create_edge_node(const std::string& group_id,
                   const std::string& edge_node_id) -> stdx::expected<void, std::string>;

  void log(const std::string& level, const std::string& message);
  void publish_result(const std::string& result);
  void publish_console_reply(const std::string& reply);

  [[nodiscard]] auto publish_tck(const std::string& topic,
                                 const std::string& payload,
                                 int qos) -> stdx::expected<void, std::string>;

  [[nodiscard]] static auto get_timestamp() -> uint64_t;

  TCKEdgeNodeConfig config_;
  MQTTAsync tck_client_;
  std::atomic<bool> running_{false};
  std::atomic<bool> connected_{false};
  std::mutex mutex_;

  EdgeTestState test_state_{EdgeTestState::IDLE};
  std::string current_test_name_;
  std::vector<std::string> current_test_params_;

  std::unique_ptr<EdgeNode> edge_node_;
  std::string current_group_id_;
  std::string current_edge_node_id_;
  std::vector<std::string> device_ids_;
};

} // namespace sparkplug::tck
