#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <time.h>

#include "cJSON.h"
#include "driver/gpio.h"
#include "esp_event.h"
#include "esp_http_server.h"
#include "esp_log.h"
#include "esp_netif.h"
#include "esp_sntp.h"
#include "esp_system.h"
#include "esp_task_wdt.h"
#include "esp_timer.h"
#include "esp_wifi.h"
#include "freertos/FreeRTOS.h"
#include "freertos/semphr.h"
#include "freertos/task.h"
#include "lwip/ip4_addr.h"
#include "mdns.h"
#include "nvs.h"
#include "nvs_flash.h"
#include "rom/ets_sys.h"
#include "soc/gpio_struct.h"

#include "font5x7.h"
#include "web_page.h"

#ifndef M_PI
#define M_PI 3.14159265358979323846
#endif

#define APP_NAMESPACE "catwheel"

#define SENSOR_GPIO GPIO_NUM_18
#define SENSOR_DEBOUNCE_US 10000
#define SENSOR_QUEUE_SIZE 256

#define SESSION_GAP_US (5LL * 1000000LL)
#define CURRENT_SPEED_STALE_US (2LL * 1000000LL)

#define MATRIX_WIDTH 64
#define MATRIX_HEIGHT 64
#define MATRIX_SCAN_ROWS 32
#define MATRIX_PWM_PLANES 2
#define MATRIX_ROW_ON_US_BASE 40
#define MATRIX_GRAPH_HEIGHT 23

#define PIN_R1 GPIO_NUM_1
#define PIN_G1 GPIO_NUM_2
#define PIN_B1 GPIO_NUM_4
#define PIN_R2 GPIO_NUM_5
#define PIN_G2 GPIO_NUM_6
#define PIN_B2 GPIO_NUM_7
#define PIN_E GPIO_NUM_8
#define PIN_A GPIO_NUM_9
#define PIN_B GPIO_NUM_10
#define PIN_C GPIO_NUM_11
#define PIN_D GPIO_NUM_12
#define PIN_CLK GPIO_NUM_13
#define PIN_LAT GPIO_NUM_14
#define PIN_OE GPIO_NUM_15

#define PIN_BIT(pin) (1UL << (uint32_t)(pin))

#define MASK_R1 PIN_BIT(PIN_R1)
#define MASK_G1 PIN_BIT(PIN_G1)
#define MASK_B1 PIN_BIT(PIN_B1)
#define MASK_R2 PIN_BIT(PIN_R2)
#define MASK_G2 PIN_BIT(PIN_G2)
#define MASK_B2 PIN_BIT(PIN_B2)
#define MASK_A PIN_BIT(PIN_A)
#define MASK_B PIN_BIT(PIN_B)
#define MASK_C PIN_BIT(PIN_C)
#define MASK_D PIN_BIT(PIN_D)
#define MASK_E PIN_BIT(PIN_E)
#define MASK_CLK PIN_BIT(PIN_CLK)
#define MASK_LAT PIN_BIT(PIN_LAT)
#define MASK_OE PIN_BIT(PIN_OE)

#define MATRIX_DATA_MASK (MASK_R1 | MASK_G1 | MASK_B1 | MASK_R2 | MASK_G2 | MASK_B2)
#define MATRIX_ADDR_MASK (MASK_A | MASK_B | MASK_C | MASK_D | MASK_E)

#define DEFAULT_WHEEL_DIAMETER_M 1.0f
#define DEFAULT_PULSES_PER_REV 4.0f
#define DEFAULT_METERS_PER_PULSE \
    ((float)(M_PI * DEFAULT_WHEEL_DIAMETER_M / DEFAULT_PULSES_PER_REV))
#define MAX_VALID_SPEED_KMH 40.0f
#define MAX_VALID_SPEED_MPS (MAX_VALID_SPEED_KMH / 3.6f)
#define STARTUP_GLITCH_FILTER_SPEED_KMH 10.0f
#define STARTUP_GLITCH_FILTER_SPEED_MPS (STARTUP_GLITCH_FILTER_SPEED_KMH / 3.6f)
#define STARTUP_GLITCH_FILTER_PULSES 2U

#define MAX_SESSIONS 120
#define SESSION_PRUNE_BATCH 8
#define MAX_HTTP_BODY (64 * 1024)
#define SAVE_PERIOD_MS 5000
#define RENDER_PERIOD_MS 250

#define WIFI_AP_SSID "CatWheelSetup"
#define WIFI_AP_PASS "catwheel123"
#define CATWHEEL_HOSTNAME "catwheel"

#define CONFIG_VERSION 3U
#define STATS_VERSION 1U
#define SESSIONS_VERSION 1U
#define BRIGHTNESS_WINDOWS_MAX 2U

typedef enum {
    MATRIX_MODE_ALWAYS = 0,
    MATRIX_MODE_RUNNING_ONLY = 1,
    MATRIX_MODE_AUTO_TIMEOUT = 2,
} matrix_mode_t;

typedef enum {
    UI_LANGUAGE_EN = 0,
    UI_LANGUAGE_DE = 1,
} ui_language_t;

typedef enum {
    METRIC_TOTAL_DISTANCE = 0,
    METRIC_TODAY_DISTANCE = 1,
    METRIC_TOP_SPEED = 2,
    METRIC_TOP_SPEED_TODAY = 3,
    METRIC_CURRENT_SPEED = 4,
    METRIC_COUNT = 5,
} metric_id_t;

typedef struct {
    bool enabled;
    uint8_t row;
    uint32_t color_rgb;
} matrix_item_cfg_t;

typedef struct {
    bool enabled;
    uint16_t start_minute;
    uint16_t end_minute;
    uint8_t brightness_pct;
} brightness_window_cfg_t;

typedef struct {
    uint32_t version;
    char wifi_ssid[33];
    char wifi_pass[65];
    matrix_mode_t matrix_mode;
    uint16_t auto_off_sec;
    float meters_per_pulse;
    uint8_t global_brightness_pct;
    bool off_window_enabled;
    uint16_t off_window_start_minute;
    uint16_t off_window_end_minute;
    brightness_window_cfg_t brightness_windows[BRIGHTNESS_WINDOWS_MAX];
    matrix_item_cfg_t items[METRIC_COUNT];
    matrix_item_cfg_t graph_item;
} app_config_t;

typedef struct {
    uint32_t version;
    double total_distance_m;
    double distance_today_m;
    double top_speed_mps;
    double top_speed_today_mps;
    uint64_t total_pulses;
    int32_t day_id;
} stats_state_t;

typedef struct {
    int64_t start_epoch_s;
    int64_t end_epoch_s;
    uint32_t duration_s;
    uint32_t pulses;
    float distance_m;
    float avg_speed_mps;
    float top_speed_mps;
} session_record_t;

typedef struct {
    uint32_t version;
    uint16_t next_index;
    uint16_t count;
    session_record_t records[MAX_SESSIONS];
} session_store_t;

typedef struct {
    bool active;
    int64_t start_us;
    int64_t last_pulse_us;
    int64_t prev_pulse_us;
    uint8_t startup_speed_checks;
    uint32_t pulses;
    double distance_m;
    double top_speed_mps;
} running_session_t;

typedef struct {
    double current_speed_mps;
    int64_t last_pulse_us;
    bool wifi_connected;
    char sta_ip[16];
} runtime_state_t;

static const char *TAG = "cat_wheel";

static const char *k_metric_keys[METRIC_COUNT] = {
    "total_distance",
    "distance_today",
    "top_speed",
    "top_speed_today",
    "current_speed",
};

static const char *k_metric_labels[METRIC_COUNT] = {
    "TOT",
    "TOD",
    "MAX",
    "DAY",
    "NOW",
};

static SemaphoreHandle_t s_state_mutex;
static TaskHandle_t s_sensor_task;

static portMUX_TYPE s_sensor_queue_mux = portMUX_INITIALIZER_UNLOCKED;
static volatile int64_t s_sensor_ts_queue[SENSOR_QUEUE_SIZE];
static volatile uint16_t s_sensor_queue_head;
static volatile uint16_t s_sensor_queue_tail;

static portMUX_TYPE s_frame_mux = portMUX_INITIALIZER_UNLOCKED;
static portMUX_TYPE s_matrix_timing_mux = portMUX_INITIALIZER_UNLOCKED;
static uint8_t s_frame_a[MATRIX_HEIGHT][MATRIX_WIDTH];
static uint8_t s_frame_b[MATRIX_HEIGHT][MATRIX_WIDTH];
static uint8_t(*volatile s_scan_frame)[MATRIX_WIDTH] = s_frame_a;
static uint8_t(*s_draw_frame)[MATRIX_WIDTH] = s_frame_b;
static volatile bool s_matrix_visible = true;
static volatile uint8_t s_matrix_brightness_pct = 100;
static float s_speed_graph_kmh[MATRIX_WIDTH];
static uint8_t s_speed_graph_head;
static uint8_t s_speed_graph_count;

static app_config_t s_config;
static stats_state_t s_stats;
static session_store_t s_sessions;
static running_session_t s_running_session;
static runtime_state_t s_runtime;

static bool s_stats_dirty;
static bool s_config_dirty;
static bool s_sessions_dirty;

static int64_t s_epoch_anchor_us;
static int64_t s_mono_anchor_us;

static httpd_handle_t s_httpd = NULL;
static int s_wifi_retry_count;
static bool s_sntp_started;
static bool s_mdns_started;
static esp_netif_t *s_netif_sta = NULL;
static esp_netif_t *s_netif_ap = NULL;
static ui_language_t s_ui_language = UI_LANGUAGE_EN;
static bool s_ui_language_dirty;

static void configure_task_wdt(void) {
#if CONFIG_ESP_TASK_WDT
#ifdef CONFIG_ESP_TASK_WDT_PANIC
    bool trigger_panic = true;
#else
    bool trigger_panic = false;
#endif

    esp_task_wdt_config_t twdt_cfg = {
        .timeout_ms = CONFIG_ESP_TASK_WDT_TIMEOUT_S * 1000U,
        .idle_core_mask = (1U << 0),
        .trigger_panic = trigger_panic,
    };

    esp_err_t err = esp_task_wdt_reconfigure(&twdt_cfg);
    if (err == ESP_ERR_INVALID_STATE) {
        err = esp_task_wdt_init(&twdt_cfg);
    }
    if (err != ESP_OK) {
        ESP_LOGW(TAG, "task wdt configure failed: %s", esp_err_to_name(err));
    }
#endif
}

static inline const char *matrix_mode_to_str(matrix_mode_t mode) {
    switch (mode) {
    case MATRIX_MODE_ALWAYS:
        return "always";
    case MATRIX_MODE_RUNNING_ONLY:
        return "running_only";
    case MATRIX_MODE_AUTO_TIMEOUT:
    default:
        return "auto_timeout";
    }
}

static inline matrix_mode_t matrix_mode_from_str(const char *mode) {
    if (mode == NULL) {
        return MATRIX_MODE_AUTO_TIMEOUT;
    }
    if (strcmp(mode, "always") == 0) {
        return MATRIX_MODE_ALWAYS;
    }
    if (strcmp(mode, "running_only") == 0) {
        return MATRIX_MODE_RUNNING_ONLY;
    }
    return MATRIX_MODE_AUTO_TIMEOUT;
}

static inline const char *ui_language_to_str(ui_language_t lang) {
    return (lang == UI_LANGUAGE_DE) ? "de" : "en";
}

static inline ui_language_t ui_language_from_str(const char *value) {
    if (value != NULL && strcmp(value, "de") == 0) {
        return UI_LANGUAGE_DE;
    }
    return UI_LANGUAGE_EN;
}

static inline double clamp_speed_mps(double mps) {
    if (mps < 0.0) {
        return 0.0;
    }
    if (mps > (double)MAX_VALID_SPEED_MPS) {
        return (double)MAX_VALID_SPEED_MPS;
    }
    return mps;
}

static inline float clamp_speed_mps_f(float mps) {
    if (mps < 0.0f) {
        return 0.0f;
    }
    if (mps > MAX_VALID_SPEED_MPS) {
        return MAX_VALID_SPEED_MPS;
    }
    return mps;
}

static void refresh_time_anchor(void) {
    struct timeval tv = {0};
    gettimeofday(&tv, NULL);
    s_epoch_anchor_us = (int64_t)tv.tv_sec * 1000000LL + (int64_t)tv.tv_usec;
    s_mono_anchor_us = esp_timer_get_time();
}

static int64_t epoch_us_from_mono_us(int64_t mono_us) {
    return s_epoch_anchor_us + (mono_us - s_mono_anchor_us);
}

static int64_t epoch_now_s(void) {
    return epoch_us_from_mono_us(esp_timer_get_time()) / 1000000LL;
}

static bool is_time_synced_epoch(int64_t epoch_s) {
    time_t now = (time_t)epoch_s;
    struct tm local_tm = {0};
    return (localtime_r(&now, &local_tm) != NULL) && ((local_tm.tm_year + 1900) >= 2024);
}

static int32_t compute_day_id(void) {
    time_t now = (time_t)epoch_now_s();
    struct tm local_tm = {0};
    if (localtime_r(&now, &local_tm) != NULL && (local_tm.tm_year + 1900) >= 2024) {
        return (local_tm.tm_year + 1900) * 10000 + (local_tm.tm_mon + 1) * 100 +
               local_tm.tm_mday;
    }

    int64_t days = esp_timer_get_time() / 1000000LL / 86400LL;
    return (int32_t)(90000000 + days);
}

static void format_iso_time(int64_t epoch_s, char *buf, size_t len) {
    time_t ts = (time_t)epoch_s;
    struct tm local_tm = {0};
    if (localtime_r(&ts, &local_tm) == NULL) {
        snprintf(buf, len, "n/a");
        return;
    }
    strftime(buf, len, "%Y-%m-%d %H:%M:%S", &local_tm);
}

#define CAT_WHEEL_INTERNAL_MODULE_BUILD 1
#include "data_logging_module.c"
#include "matrix_driver_module.c"

static void save_task_fn(void *arg) {
    while (true) {
        vTaskDelay(pdMS_TO_TICKS(SAVE_PERIOD_MS));
        persist_dirty_state();
    }
}

#include "web_server_module.c"
#undef CAT_WHEEL_INTERNAL_MODULE_BUILD

void app_main(void) {
    esp_err_t err = nvs_flash_init();
    if (err == ESP_ERR_NVS_NO_FREE_PAGES || err == ESP_ERR_NVS_NEW_VERSION_FOUND) {
        ESP_ERROR_CHECK(nvs_flash_erase());
        ESP_ERROR_CHECK(nvs_flash_init());
    }

    s_state_mutex = xSemaphoreCreateMutex();
    if (s_state_mutex == NULL) {
        ESP_LOGE(TAG, "mutex init failed");
        return;
    }

    refresh_time_anchor();
    init_default_config(&s_config);
    init_default_stats(&s_stats);
    init_default_sessions(&s_sessions);
    memset(&s_runtime, 0, sizeof(s_runtime));
    memset(&s_running_session, 0, sizeof(s_running_session));

    err = load_persistent_state();
    if (err != ESP_OK) {
        ESP_LOGW(TAG, "load state failed, defaults in use: %s", esp_err_to_name(err));
    }

    matrix_gpio_init();
    frame_clear(s_frame_a);
    frame_clear(s_frame_b);
    sensor_gpio_init();

    wifi_init_all();
    start_http_server();

    configure_task_wdt();

    xTaskCreatePinnedToCore(sensor_task_fn, "sensor_task", 4096, NULL, 10, &s_sensor_task,
                            0);
    xTaskCreatePinnedToCore(render_task_fn, "render_task", 4096, NULL, 5, NULL, 0);
    xTaskCreatePinnedToCore(matrix_scan_task_fn, "matrix_task", 4096, NULL, 8, NULL, 1);
    xTaskCreatePinnedToCore(save_task_fn, "save_task", 4096, NULL, 4, NULL, 0);

    ESP_LOGI(TAG, "Cat Wheel ready");
}
