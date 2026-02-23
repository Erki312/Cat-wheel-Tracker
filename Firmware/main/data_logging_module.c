// Split from main.c: data logging and sensor processing

static void init_default_config(app_config_t *cfg) {
    memset(cfg, 0, sizeof(*cfg));
    cfg->version = CONFIG_VERSION;
    cfg->matrix_mode = MATRIX_MODE_AUTO_TIMEOUT;
    cfg->auto_off_sec = 45;
    cfg->meters_per_pulse = DEFAULT_METERS_PER_PULSE;
    cfg->global_brightness_pct = 80;
    cfg->off_window_enabled = false;
    cfg->off_window_start_minute = 23U * 60U;
    cfg->off_window_end_minute = 7U * 60U;
    cfg->brightness_windows[0] = (brightness_window_cfg_t){
        .enabled = false,
        .start_minute = 7U * 60U,
        .end_minute = 20U * 60U,
        .brightness_pct = 100,
    };
    cfg->brightness_windows[1] = (brightness_window_cfg_t){
        .enabled = false,
        .start_minute = 20U * 60U,
        .end_minute = 7U * 60U,
        .brightness_pct = 30,
    };

    cfg->items[METRIC_TOTAL_DISTANCE] =
        (matrix_item_cfg_t){.enabled = true, .row = 0, .color_rgb = 0x00FF55};
    cfg->items[METRIC_TODAY_DISTANCE] =
        (matrix_item_cfg_t){.enabled = true, .row = 1, .color_rgb = 0x00AAFF};
    cfg->items[METRIC_TOP_SPEED] =
        (matrix_item_cfg_t){.enabled = true, .row = 2, .color_rgb = 0xFFAA00};
    cfg->items[METRIC_TOP_SPEED_TODAY] =
        (matrix_item_cfg_t){.enabled = true, .row = 3, .color_rgb = 0xFF5500};
    cfg->items[METRIC_CURRENT_SPEED] =
        (matrix_item_cfg_t){.enabled = true, .row = 4, .color_rgb = 0xFFFFFF};
    cfg->graph_item =
        (matrix_item_cfg_t){.enabled = true, .row = 6, .color_rgb = 0x00A0FF};
}

static void init_default_stats(stats_state_t *stats) {
    memset(stats, 0, sizeof(*stats));
    stats->version = STATS_VERSION;
    stats->day_id = compute_day_id();
}

static void init_default_sessions(session_store_t *sessions) {
    memset(sessions, 0, sizeof(*sessions));
    sessions->version = SESSIONS_VERSION;
}

static inline uint16_t clamp_minute_of_day_i(int value) {
    if (value < 0) {
        return 0;
    }
    if (value > 1439) {
        return 1439;
    }
    return (uint16_t)value;
}

static inline uint8_t clamp_brightness_pct_i(int value) {
    if (value < 1) {
        return 1;
    }
    if (value > 100) {
        return 100;
    }
    return (uint8_t)value;
}

static bool minute_in_window(uint16_t start_minute, uint16_t end_minute, int minute_now) {
    if (minute_now < 0 || minute_now > 1439) {
        return false;
    }

    if (start_minute == end_minute) {
        return true;
    }
    if (start_minute < end_minute) {
        return (minute_now >= (int)start_minute) && (minute_now < (int)end_minute);
    }
    return (minute_now >= (int)start_minute) || (minute_now < (int)end_minute);
}

static int minute_of_day_from_epoch_s(int64_t epoch_s, bool time_synced) {
    if (!time_synced) {
        return -1;
    }

    time_t ts = (time_t)epoch_s;
    struct tm local_tm = {0};
    if (localtime_r(&ts, &local_tm) == NULL) {
        return -1;
    }
    return local_tm.tm_hour * 60 + local_tm.tm_min;
}

static void sanitize_loaded_state(void) {
    bool stats_changed = false;
    bool sessions_changed = false;
    bool config_changed = false;

    double capped = clamp_speed_mps(s_stats.top_speed_mps);
    if (capped != s_stats.top_speed_mps) {
        s_stats.top_speed_mps = capped;
        stats_changed = true;
    }

    capped = clamp_speed_mps(s_stats.top_speed_today_mps);
    if (capped != s_stats.top_speed_today_mps) {
        s_stats.top_speed_today_mps = capped;
        stats_changed = true;
    }

    for (uint16_t i = 0; i < s_sessions.count; i++) {
        uint16_t idx =
            (uint16_t)((s_sessions.next_index + MAX_SESSIONS - s_sessions.count + i) %
                       MAX_SESSIONS);
        session_record_t *rec = &s_sessions.records[idx];

        float top = clamp_speed_mps_f(rec->top_speed_mps);
        if (top != rec->top_speed_mps) {
            rec->top_speed_mps = top;
            sessions_changed = true;
        }

        float avg = clamp_speed_mps_f(rec->avg_speed_mps);
        if (avg != rec->avg_speed_mps) {
            rec->avg_speed_mps = avg;
            sessions_changed = true;
        }
    }

    if (stats_changed) {
        s_stats_dirty = true;
    }
    if (s_config.global_brightness_pct < 1 || s_config.global_brightness_pct > 100) {
        s_config.global_brightness_pct =
            clamp_brightness_pct_i((int)s_config.global_brightness_pct);
        config_changed = true;
    }

    s_config.off_window_start_minute =
        clamp_minute_of_day_i((int)s_config.off_window_start_minute);
    s_config.off_window_end_minute =
        clamp_minute_of_day_i((int)s_config.off_window_end_minute);

    for (uint8_t i = 0; i < BRIGHTNESS_WINDOWS_MAX; i++) {
        brightness_window_cfg_t *w = &s_config.brightness_windows[i];
        uint16_t clamped_start = clamp_minute_of_day_i((int)w->start_minute);
        uint16_t clamped_end = clamp_minute_of_day_i((int)w->end_minute);
        uint8_t clamped_brightness = clamp_brightness_pct_i((int)w->brightness_pct);
        if (w->start_minute != clamped_start || w->end_minute != clamped_end ||
            w->brightness_pct != clamped_brightness) {
            w->start_minute = clamped_start;
            w->end_minute = clamped_end;
            w->brightness_pct = clamped_brightness;
            config_changed = true;
        }
    }

    if (config_changed) {
        s_config_dirty = true;
    }
    if (sessions_changed) {
        s_sessions_dirty = true;
    }
}

static void session_store_push(session_store_t *store, const session_record_t *rec) {
    store->records[store->next_index] = *rec;
    store->next_index = (store->next_index + 1) % MAX_SESSIONS;
    if (store->count < MAX_SESSIONS) {
        store->count++;
    }
}

static uint16_t session_store_drop_oldest(session_store_t *store, uint16_t drop_count) {
    if (drop_count == 0 || store->count == 0) {
        return 0;
    }

    if (drop_count >= store->count) {
        uint16_t removed = store->count;
        store->count = 0;
        store->next_index = 0;
        return removed;
    }

    store->count -= drop_count;
    return drop_count;
}

static esp_err_t load_persistent_state(void) {
    nvs_handle_t nvs = 0;
    esp_err_t err = nvs_open(APP_NAMESPACE, NVS_READWRITE, &nvs);
    if (err != ESP_OK) {
        return err;
    }

    bool cleanup_commit_needed = false;

    size_t len = sizeof(s_stats);
    err = nvs_get_blob(nvs, "stats", &s_stats, &len);
    if (err != ESP_OK || len != sizeof(s_stats) || s_stats.version != STATS_VERSION) {
        if (err == ESP_OK || err == ESP_ERR_NVS_INVALID_LENGTH) {
            if (nvs_erase_key(nvs, "stats") == ESP_OK) {
                cleanup_commit_needed = true;
            }
        }
        init_default_stats(&s_stats);
    }

    len = sizeof(s_config);
    err = nvs_get_blob(nvs, "config", &s_config, &len);
    if (err != ESP_OK || len != sizeof(s_config) || s_config.version != CONFIG_VERSION) {
        if (err == ESP_OK || err == ESP_ERR_NVS_INVALID_LENGTH) {
            if (nvs_erase_key(nvs, "config") == ESP_OK) {
                cleanup_commit_needed = true;
            }
        }
        init_default_config(&s_config);
    }

    len = sizeof(s_sessions);
    err = nvs_get_blob(nvs, "sessions", &s_sessions, &len);
    if (err != ESP_OK || len != sizeof(s_sessions) ||
        s_sessions.version != SESSIONS_VERSION || s_sessions.count > MAX_SESSIONS ||
        s_sessions.next_index >= MAX_SESSIONS) {
        if (err == ESP_OK || err == ESP_ERR_NVS_INVALID_LENGTH) {
            if (nvs_erase_key(nvs, "sessions") == ESP_OK) {
                cleanup_commit_needed = true;
            }
        }
        init_default_sessions(&s_sessions);
    }

    if (cleanup_commit_needed) {
        err = nvs_commit(nvs);
        if (err != ESP_OK) {
            ESP_LOGW(TAG, "NVS cleanup commit failed: %s", esp_err_to_name(err));
        }
    }

    nvs_close(nvs);
    sanitize_loaded_state();
    return ESP_OK;
}

static void persist_dirty_state(void) {
    nvs_handle_t nvs = 0;
    esp_err_t err = nvs_open(APP_NAMESPACE, NVS_READWRITE, &nvs);
    if (err != ESP_OK) {
        ESP_LOGE(TAG, "nvs_open failed: %s", esp_err_to_name(err));
        return;
    }

    static int64_t s_last_save_error_log_us = 0;

    for (int attempt = 0; attempt < 12; attempt++) {
        bool do_stats = false;
        bool do_config = false;
        bool do_sessions = false;
        esp_err_t stats_err = ESP_OK;
        esp_err_t config_err = ESP_OK;
        esp_err_t sessions_err = ESP_OK;
        uint16_t pruned = 0;
        uint16_t remaining = 0;

        xSemaphoreTake(s_state_mutex, portMAX_DELAY);
        do_stats = s_stats_dirty;
        do_config = s_config_dirty;
        do_sessions = s_sessions_dirty;

        if (!do_stats && !do_config && !do_sessions) {
            xSemaphoreGive(s_state_mutex);
            break;
        }

        if (do_stats) {
            stats_err = nvs_set_blob(nvs, "stats", &s_stats, sizeof(s_stats));
        }
        if (do_config && stats_err == ESP_OK) {
            config_err = nvs_set_blob(nvs, "config", &s_config, sizeof(s_config));
        }
        if (do_sessions && stats_err == ESP_OK && config_err == ESP_OK) {
            sessions_err = nvs_set_blob(nvs, "sessions", &s_sessions, sizeof(s_sessions));
            if (sessions_err == ESP_ERR_NVS_NOT_ENOUGH_SPACE) {
                pruned = session_store_drop_oldest(&s_sessions, SESSION_PRUNE_BATCH);
                remaining = s_sessions.count;
                if (pruned > 0) {
                    s_sessions_dirty = true;
                }
            }
        }
        xSemaphoreGive(s_state_mutex);

        if (pruned > 0) {
            ESP_LOGW(TAG, "NVS full, dropped %u old sessions (%u left)",
                     (unsigned)pruned, (unsigned)remaining);
            continue;
        }

        if (stats_err != ESP_OK || config_err != ESP_OK || sessions_err != ESP_OK) {
            int64_t now_us = esp_timer_get_time();
            if ((now_us - s_last_save_error_log_us) > 10000000LL) {
                if (stats_err != ESP_OK) {
                    ESP_LOGE(TAG, "save stats failed: %s", esp_err_to_name(stats_err));
                }
                if (config_err != ESP_OK) {
                    ESP_LOGE(TAG, "save config failed: %s", esp_err_to_name(config_err));
                }
                if (sessions_err != ESP_OK) {
                    ESP_LOGE(TAG, "save sessions failed: %s",
                             esp_err_to_name(sessions_err));
                }
                s_last_save_error_log_us = now_us;
            }
            break;
        }

        err = nvs_commit(nvs);
        if (err == ESP_OK) {
            xSemaphoreTake(s_state_mutex, portMAX_DELAY);
            if (do_stats) {
                s_stats_dirty = false;
            }
            if (do_config) {
                s_config_dirty = false;
            }
            if (do_sessions) {
                s_sessions_dirty = false;
            }
            xSemaphoreGive(s_state_mutex);
            break;
        }

        xSemaphoreTake(s_state_mutex, portMAX_DELAY);
        if (err == ESP_ERR_NVS_NOT_ENOUGH_SPACE && s_sessions_dirty) {
            pruned = session_store_drop_oldest(&s_sessions, SESSION_PRUNE_BATCH);
            remaining = s_sessions.count;
            if (pruned > 0) {
                s_sessions_dirty = true;
            }
        }
        xSemaphoreGive(s_state_mutex);

        if (pruned > 0) {
            ESP_LOGW(TAG, "NVS commit full, dropped %u old sessions (%u left)",
                     (unsigned)pruned, (unsigned)remaining);
            continue;
        }

        int64_t now_us = esp_timer_get_time();
        if ((now_us - s_last_save_error_log_us) > 10000000LL) {
            ESP_LOGE(TAG, "nvs_commit failed: %s", esp_err_to_name(err));
            s_last_save_error_log_us = now_us;
        }
        break;
    }

    nvs_close(nvs);
}

static uint32_t parse_color_hex(const char *str, uint32_t fallback) {
    if (str == NULL) {
        return fallback;
    }

    if (str[0] == '#') {
        str++;
    }

    char *end = NULL;
    unsigned long value = strtoul(str, &end, 16);
    if (end == str) {
        return fallback;
    }
    return (uint32_t)(value & 0xFFFFFFUL);
}

static void color_to_hex(uint32_t color, char *out, size_t out_len) {
    snprintf(out, out_len, "#%06lX", (unsigned long)(color & 0xFFFFFFU));
}

static inline uint8_t apply_channel_gain(uint8_t value, uint16_t gain) {
    uint16_t scaled = (uint16_t)(((uint32_t)value * gain + 127U) / 255U);
    if (scaled > 255U) {
        scaled = 255U;
    }
    return (uint8_t)scaled;
}

static inline uint8_t quantize_2bit(uint8_t value) {
    // 4 levels per channel (0..3), rounding to nearest.
    return (uint8_t)(((uint16_t)value * 3U + 127U) / 255U);
}

static inline uint8_t rgb24_to_rgb6(uint32_t color) {
    // Simple per-channel calibration so warm tones look less washed out on this panel.
    uint8_t r = apply_channel_gain((uint8_t)((color >> 16) & 0xFFU), 255U);
    uint8_t g = apply_channel_gain((uint8_t)((color >> 8) & 0xFFU), 230U);
    uint8_t b = apply_channel_gain((uint8_t)(color & 0xFFU), 180U);

    uint8_t rq = quantize_2bit(r);
    uint8_t gq = quantize_2bit(g);
    uint8_t bq = quantize_2bit(b);
    return (uint8_t)(rq | (uint8_t)(gq << 2) | (uint8_t)(bq << 4));
}

static inline uint8_t rgb6_plane_to_rgb3(uint8_t color6, uint8_t plane) {
    uint8_t r = (uint8_t)((color6 >> plane) & 0x1U);
    uint8_t g = (uint8_t)((color6 >> (2U + plane)) & 0x1U);
    uint8_t b = (uint8_t)((color6 >> (4U + plane)) & 0x1U);
    return (uint8_t)(r | (uint8_t)(g << 1) | (uint8_t)(b << 2));
}

static void mark_day_rollover_locked(void) {
    int32_t today = compute_day_id();
    if (s_stats.day_id != today) {
        s_stats.day_id = today;
        s_stats.distance_today_m = 0;
        s_stats.top_speed_today_mps = 0;
        s_stats_dirty = true;
    }
}

static void finalize_running_session_locked(int64_t now_us) {
    if (!s_running_session.active) {
        return;
    }

    int64_t end_us = s_running_session.last_pulse_us > 0 ? s_running_session.last_pulse_us
                                                         : now_us;
    if (end_us < s_running_session.start_us) {
        end_us = now_us;
    }

    uint32_t duration_s =
        (uint32_t)((end_us - s_running_session.start_us) / 1000000LL);
    if (duration_s == 0) {
        duration_s = 1;
    }

    session_record_t rec = {
        .start_epoch_s = epoch_us_from_mono_us(s_running_session.start_us) / 1000000LL,
        .end_epoch_s = epoch_us_from_mono_us(end_us) / 1000000LL,
        .duration_s = duration_s,
        .pulses = s_running_session.pulses,
        .distance_m = (float)s_running_session.distance_m,
        .avg_speed_mps =
            clamp_speed_mps_f((float)(s_running_session.distance_m / (double)duration_s)),
        .top_speed_mps = clamp_speed_mps_f((float)s_running_session.top_speed_mps),
    };

    session_store_push(&s_sessions, &rec);
    s_sessions_dirty = true;

    memset(&s_running_session, 0, sizeof(s_running_session));
    s_runtime.current_speed_mps = 0;
}

static void process_pulse_locked(int64_t pulse_us) {
    mark_day_rollover_locked();

    if (s_running_session.active &&
        (pulse_us - s_running_session.last_pulse_us) > SESSION_GAP_US) {
        finalize_running_session_locked(pulse_us);
    }

    if (!s_running_session.active) {
        s_running_session.active = true;
        s_running_session.start_us = pulse_us;
        s_running_session.last_pulse_us = pulse_us;
        s_running_session.prev_pulse_us = 0;
        s_running_session.startup_speed_checks = 0;
    }

    double pulse_distance =
        (s_config.meters_per_pulse > 0.001f) ? s_config.meters_per_pulse
                                             : DEFAULT_METERS_PER_PULSE;
    double inst_speed_mps = 0;
    bool speed_valid = false;

    if (s_running_session.prev_pulse_us > 0) {
        int64_t dt = pulse_us - s_running_session.prev_pulse_us;
        if (dt <= 0) {
            return;
        }
        inst_speed_mps = pulse_distance / ((double)dt / 1000000.0);
        if (inst_speed_mps > (double)MAX_VALID_SPEED_MPS) {
            return;
        }
        if (s_running_session.startup_speed_checks < STARTUP_GLITCH_FILTER_PULSES) {
            s_running_session.startup_speed_checks++;
            if (inst_speed_mps > (double)STARTUP_GLITCH_FILTER_SPEED_MPS) {
                return;
            }
        }
        speed_valid = true;
    }

    s_running_session.prev_pulse_us = pulse_us;
    s_running_session.last_pulse_us = pulse_us;
    s_running_session.pulses++;
    s_running_session.distance_m += pulse_distance;
    if (speed_valid && inst_speed_mps > s_running_session.top_speed_mps) {
        s_running_session.top_speed_mps = inst_speed_mps;
    }

    s_stats.total_pulses++;
    s_stats.total_distance_m += pulse_distance;
    s_stats.distance_today_m += pulse_distance;
    if (speed_valid && inst_speed_mps > s_stats.top_speed_mps) {
        s_stats.top_speed_mps = inst_speed_mps;
    }
    if (speed_valid && inst_speed_mps > s_stats.top_speed_today_mps) {
        s_stats.top_speed_today_mps = inst_speed_mps;
    }
    s_stats_dirty = true;

    if (speed_valid) {
        s_runtime.current_speed_mps = inst_speed_mps;
    }
    s_runtime.last_pulse_us = pulse_us;
}

static void poll_session_timeout_locked(int64_t now_us) {
    mark_day_rollover_locked();

    if (s_running_session.active &&
        (now_us - s_running_session.last_pulse_us) > SESSION_GAP_US) {
        finalize_running_session_locked(now_us);
    }
    if ((now_us - s_runtime.last_pulse_us) > CURRENT_SPEED_STALE_US) {
        s_runtime.current_speed_mps = 0;
    }
}

static bool sensor_queue_pop(int64_t *out_ts) {
    bool has_item = false;
    portENTER_CRITICAL(&s_sensor_queue_mux);
    if (s_sensor_queue_tail != s_sensor_queue_head) {
        *out_ts = s_sensor_ts_queue[s_sensor_queue_tail];
        s_sensor_queue_tail = (s_sensor_queue_tail + 1) % SENSOR_QUEUE_SIZE;
        has_item = true;
    }
    portEXIT_CRITICAL(&s_sensor_queue_mux);
    return has_item;
}

static void IRAM_ATTR sensor_isr(void *arg) {
    static int64_t last_irq_us = 0;
    int64_t now_us = esp_timer_get_time();

    if ((now_us - last_irq_us) < SENSOR_DEBOUNCE_US) {
        return;
    }
    last_irq_us = now_us;

    portENTER_CRITICAL_ISR(&s_sensor_queue_mux);
    uint16_t next = (s_sensor_queue_head + 1) % SENSOR_QUEUE_SIZE;
    if (next != s_sensor_queue_tail) {
        s_sensor_ts_queue[s_sensor_queue_head] = now_us;
        s_sensor_queue_head = next;
    }
    portEXIT_CRITICAL_ISR(&s_sensor_queue_mux);

    BaseType_t high_prio = pdFALSE;
    if (s_sensor_task != NULL) {
        vTaskNotifyGiveFromISR(s_sensor_task, &high_prio);
    }
    if (high_prio == pdTRUE) {
        portYIELD_FROM_ISR();
    }
}

static inline void matrix_write_mask(uint32_t set_mask, uint32_t clear_mask) {
    GPIO.out_w1tc = clear_mask;
    GPIO.out_w1ts = set_mask;
}

static inline void matrix_set_addr(uint8_t row) {
    uint32_t set_mask = 0;
    if ((row >> 0) & 0x1) {
        set_mask |= MASK_A;
    }
    if ((row >> 1) & 0x1) {
        set_mask |= MASK_B;
    }
    if ((row >> 2) & 0x1) {
        set_mask |= MASK_C;
    }
    if ((row >> 3) & 0x1) {
        set_mask |= MASK_D;
    }
    if ((row >> 4) & 0x1) {
        set_mask |= MASK_E;
    }
    matrix_write_mask(set_mask, MATRIX_ADDR_MASK);
}

static inline void matrix_set_data(uint8_t top, uint8_t bottom) {
    uint32_t set_mask = 0;
    if ((top >> 0) & 0x1) {
        set_mask |= MASK_R1;
    }
    if ((top >> 1) & 0x1) {
        set_mask |= MASK_G1;
    }
    if ((top >> 2) & 0x1) {
        set_mask |= MASK_B1;
    }
    if ((bottom >> 0) & 0x1) {
        set_mask |= MASK_R2;
    }
    if ((bottom >> 1) & 0x1) {
        set_mask |= MASK_G2;
    }
    if ((bottom >> 2) & 0x1) {
        set_mask |= MASK_B2;
    }
    matrix_write_mask(set_mask, MATRIX_DATA_MASK);
}

static inline void matrix_clock_pulse(void) {
    GPIO.out_w1ts = MASK_CLK;
    GPIO.out_w1tc = MASK_CLK;
}

static void matrix_gpio_init(void) {
    static const gpio_num_t pins[] = {
        PIN_R1, PIN_G1, PIN_B1, PIN_R2, PIN_G2, PIN_B2, PIN_E,
        PIN_A,  PIN_B,  PIN_C,  PIN_D,  PIN_CLK, PIN_LAT, PIN_OE,
    };

    uint64_t mask = 0;
    for (size_t i = 0; i < sizeof(pins) / sizeof(pins[0]); i++) {
        mask |= (1ULL << (uint32_t)pins[i]);
    }

    gpio_config_t cfg = {
        .pin_bit_mask = mask,
        .mode = GPIO_MODE_OUTPUT,
        .pull_up_en = GPIO_PULLUP_DISABLE,
        .pull_down_en = GPIO_PULLDOWN_DISABLE,
        .intr_type = GPIO_INTR_DISABLE,
    };
    ESP_ERROR_CHECK(gpio_config(&cfg));

    gpio_set_level(PIN_CLK, 0);
    gpio_set_level(PIN_LAT, 0);
    gpio_set_level(PIN_OE, 1);

    // Clear RGB/ADDR/LAT/CLK and keep OE high (panel off) at start.
    GPIO.out_w1tc = MATRIX_DATA_MASK | MATRIX_ADDR_MASK | MASK_LAT | MASK_CLK;
    GPIO.out_w1ts = MASK_OE;
}

static inline void frame_clear(uint8_t frame[MATRIX_HEIGHT][MATRIX_WIDTH]) {
    memset(frame, 0, MATRIX_HEIGHT * MATRIX_WIDTH);
}

static inline void frame_set_pixel(uint8_t frame[MATRIX_HEIGHT][MATRIX_WIDTH], int x,
                                   int y, uint32_t color_rgb) {
    if (x < 0 || y < 0 || x >= MATRIX_WIDTH || y >= MATRIX_HEIGHT) {
        return;
    }
    frame[y][x] = rgb24_to_rgb6(color_rgb);
}

static void frame_draw_char(uint8_t frame[MATRIX_HEIGHT][MATRIX_WIDTH], int x, int y,
                            char c, uint32_t color_rgb) {
    if (c < 32 || c > 126) {
        c = '?';
    }

    const uint8_t *glyph = &font5x7[(c - 32) * 5];
    for (int col = 0; col < FONT5X7_WIDTH; col++) {
        uint8_t bits = glyph[col];
        for (int row = 0; row < FONT5X7_HEIGHT; row++) {
            if ((bits >> row) & 0x1) {
                frame_set_pixel(frame, x + col, y + row, color_rgb);
            }
        }
    }
}

static void frame_draw_text(uint8_t frame[MATRIX_HEIGHT][MATRIX_WIDTH], int x, int y,
                            const char *text, uint32_t color_rgb) {
    int cursor_x = x;
    for (const char *p = text; *p != '\0'; p++) {
        frame_draw_char(frame, cursor_x, y, *p, color_rgb);
        cursor_x += FONT5X7_WIDTH + 1;
        if (cursor_x >= MATRIX_WIDTH) {
            break;
        }
    }
}

static void format_distance_short(double meters, char *out, size_t len) {
    if (meters >= 1000.0) {
        double km = meters / 1000.0;
        if (km < 100.0) {
            snprintf(out, len, " %.1fkm", km);
        } else {
            snprintf(out, len, " %.0fkm", km);
        }
    } else {
        snprintf(out, len, " %.0fm", meters);
    }
}

static void format_speed_short(double mps, char *out, size_t len) {
    double kmh = clamp_speed_mps(mps) * 3.6;
    if (kmh < 10.0) {
        snprintf(out, len, "%.1fkm/h", kmh);
    } else {
        snprintf(out, len, "%.0fkm/h", kmh);
    }
}

static void matrix_graph_push_speed(float speed_kmh) {
    if (speed_kmh < 0.0f) {
        speed_kmh = 0.0f;
    }
    if (speed_kmh > MAX_VALID_SPEED_KMH) {
        speed_kmh = MAX_VALID_SPEED_KMH;
    }

    s_speed_graph_kmh[s_speed_graph_head] = speed_kmh;
    s_speed_graph_head = (uint8_t)((s_speed_graph_head + 1) % MATRIX_WIDTH);
    if (s_speed_graph_count < MATRIX_WIDTH) {
        s_speed_graph_count++;
    }
}

static void frame_draw_speed_graph(uint8_t frame[MATRIX_HEIGHT][MATRIX_WIDTH],
                                   uint32_t color_rgb, uint8_t start_row) {
    if (s_speed_graph_count < 2) {
        return;
    }

    int graph_top = (int)start_row * 8;
    int graph_bottom = graph_top + MATRIX_GRAPH_HEIGHT - 1;

    // Keep requested height by shifting up when the graph would overflow at bottom.
    if (graph_bottom >= MATRIX_HEIGHT) {
        int overflow = graph_bottom - (MATRIX_HEIGHT - 1);
        graph_top -= overflow;
        graph_bottom = MATRIX_HEIGHT - 1;
    }
    if (graph_top < 0) {
        graph_top = 0;
        graph_bottom = graph_top + MATRIX_GRAPH_HEIGHT - 1;
        if (graph_bottom >= MATRIX_HEIGHT) {
            graph_bottom = MATRIX_HEIGHT - 1;
        }
    }
    int graph_height = graph_bottom - graph_top + 1;
    if (graph_height < 2) {
        return;
    }

    float peak = 1.0f;
    for (uint8_t i = 0; i < s_speed_graph_count; i++) {
        uint16_t idx =
            (uint16_t)((s_speed_graph_head + MATRIX_WIDTH - s_speed_graph_count + i) %
                       MATRIX_WIDTH);
        float v = s_speed_graph_kmh[idx];
        if (v > peak) {
            peak = v;
        }
    }

    int base_y = graph_bottom;

    for (int x = 0; x < MATRIX_WIDTH; x++) {
        int data_x = x - (MATRIX_WIDTH - s_speed_graph_count);
        if (data_x < 0) {
            continue;
        }

        uint16_t idx =
            (uint16_t)((s_speed_graph_head + MATRIX_WIDTH - s_speed_graph_count + data_x) %
                       MATRIX_WIDTH);
        float value = s_speed_graph_kmh[idx];
        int height = (int)((value / peak) * (float)(graph_height - 1) + 0.5f);
        if (height < 0) {
            height = 0;
        }
        if (height > (graph_height - 1)) {
            height = graph_height - 1;
        }

        for (int y = 0; y <= height; y++) {
            frame_set_pixel(frame, x, base_y - y, color_rgb);
        }
    }

    for (int x = 0; x < MATRIX_WIDTH; x++) {
        frame_set_pixel(frame, x, graph_top, color_rgb);
    }
}

static uint8_t matrix_effective_brightness_pct(const app_config_t *cfg, int64_t now_epoch_s,
                                               bool time_synced) {
    uint8_t brightness = clamp_brightness_pct_i((int)cfg->global_brightness_pct);
    int minute_now = minute_of_day_from_epoch_s(now_epoch_s, time_synced);
    if (minute_now < 0) {
        return brightness;
    }

    for (uint8_t i = 0; i < BRIGHTNESS_WINDOWS_MAX; i++) {
        const brightness_window_cfg_t *w = &cfg->brightness_windows[i];
        if (!w->enabled) {
            continue;
        }
        if (minute_in_window(w->start_minute, w->end_minute, minute_now)) {
            return clamp_brightness_pct_i((int)w->brightness_pct);
        }
    }
    return brightness;
}

static bool matrix_should_be_on(const app_config_t *cfg, bool running_active,
                                int64_t last_pulse_us, int64_t now_us, int64_t now_epoch_s,
                                bool time_synced) {
    int minute_now = minute_of_day_from_epoch_s(now_epoch_s, time_synced);
    if (time_synced && cfg->off_window_enabled && minute_now >= 0 &&
        minute_in_window(cfg->off_window_start_minute, cfg->off_window_end_minute,
                         minute_now)) {
        return false;
    }

    switch (cfg->matrix_mode) {
    case MATRIX_MODE_ALWAYS:
        return true;
    case MATRIX_MODE_RUNNING_ONLY:
        return running_active;
    case MATRIX_MODE_AUTO_TIMEOUT:
    default:
        if (running_active) {
            return true;
        }
        if (cfg->auto_off_sec == 0 || last_pulse_us == 0) {
            return false;
        }
        return (now_us - last_pulse_us) <= ((int64_t)cfg->auto_off_sec * 1000000LL);
    }
}

static void swap_frames(void) {
    portENTER_CRITICAL(&s_frame_mux);
    uint8_t(*tmp)[MATRIX_WIDTH] = s_draw_frame;
    s_draw_frame = (uint8_t(*)[MATRIX_WIDTH])s_scan_frame;
    s_scan_frame = tmp;
    portEXIT_CRITICAL(&s_frame_mux);
}

static void sensor_task_fn(void *arg) {
    while (true) {
        ulTaskNotifyTake(pdTRUE, pdMS_TO_TICKS(500));

        int64_t pulse_ts = 0;
        while (sensor_queue_pop(&pulse_ts)) {
            xSemaphoreTake(s_state_mutex, portMAX_DELAY);
            process_pulse_locked(pulse_ts);
            xSemaphoreGive(s_state_mutex);
        }

        xSemaphoreTake(s_state_mutex, portMAX_DELAY);
        poll_session_timeout_locked(esp_timer_get_time());
        xSemaphoreGive(s_state_mutex);
    }
}
