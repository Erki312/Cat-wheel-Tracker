// Split from main.c: WiFi and HTTP API

#ifndef CAT_WHEEL_INTERNAL_MODULE_BUILD
#error "web_server_module.c must be included from main.c (do not add to SRCS)."
#endif

static void sntp_time_sync_cb(struct timeval *tv) {
    (void)tv;
    refresh_time_anchor();
    ESP_LOGI(TAG, "SNTP time synchronized");
}

static void start_sntp_if_needed(void) {
    if (s_sntp_started) {
        return;
    }

    setenv("TZ", "CET-1CEST,M3.5.0/2,M10.5.0/3", 1);
    tzset();

    esp_sntp_setoperatingmode(SNTP_OPMODE_POLL);
    esp_sntp_setservername(0, "pool.ntp.org");
    esp_sntp_set_time_sync_notification_cb(sntp_time_sync_cb);
    esp_sntp_init();
    s_sntp_started = true;
}

static void start_mdns_if_needed(void) {
    if (s_mdns_started) {
        return;
    }

    esp_err_t err = mdns_init();
    if (err != ESP_OK && err != ESP_ERR_INVALID_STATE) {
        ESP_LOGW(TAG, "mdns_init failed: %s", esp_err_to_name(err));
        return;
    }

    err = mdns_hostname_set(CATWHEEL_HOSTNAME);
    if (err != ESP_OK) {
        ESP_LOGW(TAG, "mdns_hostname_set failed: %s", esp_err_to_name(err));
        return;
    }

    err = mdns_instance_name_set("Cat Wheel");
    if (err != ESP_OK) {
        ESP_LOGW(TAG, "mdns_instance_name_set failed: %s", esp_err_to_name(err));
        return;
    }

    if (!mdns_service_exists("_http", "_tcp", NULL)) {
        mdns_txt_item_t txt[] = {
            {.key = "path", .value = "/"},
        };
        err = mdns_service_add("Cat Wheel", "_http", "_tcp", 80, txt,
                               sizeof(txt) / sizeof(txt[0]));
        if (err != ESP_OK) {
            ESP_LOGW(TAG, "mdns_service_add failed: %s", esp_err_to_name(err));
            return;
        }
    } else {
        err = mdns_service_port_set("_http", "_tcp", 80);
        if (err != ESP_OK) {
            ESP_LOGW(TAG, "mdns_service_port_set failed: %s", esp_err_to_name(err));
            return;
        }
        err = mdns_service_txt_item_set("_http", "_tcp", "path", "/");
        if (err != ESP_OK) {
            ESP_LOGW(TAG, "mdns_service_txt_item_set failed: %s", esp_err_to_name(err));
            return;
        }
    }

    s_mdns_started = true;
    ESP_LOGI(TAG, "mDNS ready: http://%s.local", CATWHEEL_HOSTNAME);
}

static void wifi_apply_sta_config(void) {
    app_config_t cfg_snapshot;
    bool sta_connected = false;
    xSemaphoreTake(s_state_mutex, portMAX_DELAY);
    cfg_snapshot = s_config;
    sta_connected = s_runtime.wifi_connected;
    xSemaphoreGive(s_state_mutex);

    wifi_mode_t mode = WIFI_MODE_AP;
    if (cfg_snapshot.wifi_ssid[0] != '\0') {
        mode = sta_connected ? WIFI_MODE_STA : WIFI_MODE_APSTA;
    }

    esp_err_t err = esp_wifi_set_mode(mode);
    if (err != ESP_OK) {
        ESP_LOGE(TAG, "esp_wifi_set_mode failed: %s", esp_err_to_name(err));
        return;
    }

    if (mode == WIFI_MODE_APSTA || mode == WIFI_MODE_STA) {
        wifi_config_t sta_cfg = {0};
        strlcpy((char *)sta_cfg.sta.ssid, cfg_snapshot.wifi_ssid,
                sizeof(sta_cfg.sta.ssid));
        strlcpy((char *)sta_cfg.sta.password, cfg_snapshot.wifi_pass,
                sizeof(sta_cfg.sta.password));
        sta_cfg.sta.threshold.authmode =
            (cfg_snapshot.wifi_pass[0] == '\0') ? WIFI_AUTH_OPEN : WIFI_AUTH_WPA2_PSK;
        sta_cfg.sta.pmf_cfg.capable = true;
        sta_cfg.sta.pmf_cfg.required = false;
        err = esp_wifi_set_config(WIFI_IF_STA, &sta_cfg);
        if (err != ESP_OK) {
            ESP_LOGE(TAG, "esp_wifi_set_config failed: %s", esp_err_to_name(err));
            return;
        }
        err = esp_wifi_disconnect();
        if (err != ESP_OK && err != ESP_ERR_WIFI_NOT_STARTED &&
            err != ESP_ERR_WIFI_NOT_CONNECT) {
            ESP_LOGW(TAG, "esp_wifi_disconnect: %s", esp_err_to_name(err));
        }
        err = esp_wifi_connect();
        if (err != ESP_OK && err != ESP_ERR_WIFI_NOT_STARTED) {
            ESP_LOGW(TAG, "esp_wifi_connect: %s", esp_err_to_name(err));
        }
    } else {
        err = esp_wifi_disconnect();
        if (err != ESP_OK && err != ESP_ERR_WIFI_NOT_STARTED &&
            err != ESP_ERR_WIFI_NOT_CONNECT) {
            ESP_LOGW(TAG, "esp_wifi_disconnect: %s", esp_err_to_name(err));
        }
    }
}

static void wifi_event_handler(void *arg, esp_event_base_t event_base, int32_t event_id,
                               void *event_data) {
    if (event_base == WIFI_EVENT && event_id == WIFI_EVENT_STA_START) {
        xSemaphoreTake(s_state_mutex, portMAX_DELAY);
        bool do_connect = s_config.wifi_ssid[0] != '\0';
        xSemaphoreGive(s_state_mutex);
        if (do_connect) {
            esp_wifi_connect();
        }
        return;
    }

    if (event_base == WIFI_EVENT && event_id == WIFI_EVENT_STA_DISCONNECTED) {
        wifi_event_sta_disconnected_t *disc = (wifi_event_sta_disconnected_t *)event_data;
        xSemaphoreTake(s_state_mutex, portMAX_DELAY);
        s_runtime.wifi_connected = false;
        s_runtime.sta_ip[0] = '\0';
        bool retry = s_config.wifi_ssid[0] != '\0';
        xSemaphoreGive(s_state_mutex);
        if (disc != NULL) {
            ESP_LOGW(TAG, "STA disconnected, reason=%u", (unsigned)disc->reason);
        }
        if (retry && s_wifi_retry_count < 10) {
            esp_err_t mode_err = esp_wifi_set_mode(WIFI_MODE_APSTA);
            if (mode_err != ESP_OK) {
                ESP_LOGW(TAG, "switch to APSTA failed: %s", esp_err_to_name(mode_err));
            } else {
                ESP_LOGI(TAG, "AP fallback active: http://192.168.4.1, mDNS: http://%s.local",
                         CATWHEEL_HOSTNAME);
            }
            s_wifi_retry_count++;
            esp_wifi_connect();
        }
        return;
    }

    if (event_base == IP_EVENT && event_id == IP_EVENT_STA_GOT_IP) {
        ip_event_got_ip_t *event = (ip_event_got_ip_t *)event_data;
        xSemaphoreTake(s_state_mutex, portMAX_DELAY);
        s_wifi_retry_count = 0;
        s_runtime.wifi_connected = true;
        snprintf(s_runtime.sta_ip, sizeof(s_runtime.sta_ip), IPSTR,
                 IP2STR(&event->ip_info.ip));
        xSemaphoreGive(s_state_mutex);
        esp_err_t mode_err = esp_wifi_set_mode(WIFI_MODE_STA);
        if (mode_err != ESP_OK) {
            ESP_LOGW(TAG, "disable AP failed: %s", esp_err_to_name(mode_err));
        } else {
            ESP_LOGI(TAG, "STA connected: http://" IPSTR ", mDNS: http://%s.local",
                     IP2STR(&event->ip_info.ip), CATWHEEL_HOSTNAME);
        }
        start_sntp_if_needed();
        refresh_time_anchor();
    }
}

static void wifi_init_all(void) {
    ESP_ERROR_CHECK(esp_netif_init());
    ESP_ERROR_CHECK(esp_event_loop_create_default());
    s_netif_sta = esp_netif_create_default_wifi_sta();
    s_netif_ap = esp_netif_create_default_wifi_ap();
    if (s_netif_sta != NULL) {
        esp_netif_set_hostname(s_netif_sta, CATWHEEL_HOSTNAME);
    }
    if (s_netif_ap != NULL) {
        esp_netif_set_hostname(s_netif_ap, CATWHEEL_HOSTNAME);
    }

    wifi_init_config_t cfg = WIFI_INIT_CONFIG_DEFAULT();
    ESP_ERROR_CHECK(esp_wifi_init(&cfg));

    ESP_ERROR_CHECK(
        esp_event_handler_register(WIFI_EVENT, ESP_EVENT_ANY_ID, &wifi_event_handler, NULL));
    ESP_ERROR_CHECK(
        esp_event_handler_register(IP_EVENT, IP_EVENT_STA_GOT_IP, &wifi_event_handler, NULL));

    wifi_config_t ap_cfg = {0};
    strlcpy((char *)ap_cfg.ap.ssid, WIFI_AP_SSID, sizeof(ap_cfg.ap.ssid));
    strlcpy((char *)ap_cfg.ap.password, WIFI_AP_PASS, sizeof(ap_cfg.ap.password));
    ap_cfg.ap.ssid_len = strlen(WIFI_AP_SSID);
    ap_cfg.ap.channel = 1;
    ap_cfg.ap.max_connection = 4;
    ap_cfg.ap.authmode = WIFI_AUTH_WPA2_PSK;
    if (strlen(WIFI_AP_PASS) == 0) {
        ap_cfg.ap.authmode = WIFI_AUTH_OPEN;
    }

    xSemaphoreTake(s_state_mutex, portMAX_DELAY);
    wifi_mode_t mode = (s_config.wifi_ssid[0] != '\0') ? WIFI_MODE_APSTA : WIFI_MODE_AP;
    xSemaphoreGive(s_state_mutex);

    ESP_ERROR_CHECK(esp_wifi_set_mode(mode));
    ESP_ERROR_CHECK(esp_wifi_set_config(WIFI_IF_AP, &ap_cfg));

    if (mode == WIFI_MODE_APSTA) {
        wifi_config_t sta_cfg = {0};
        xSemaphoreTake(s_state_mutex, portMAX_DELAY);
        strlcpy((char *)sta_cfg.sta.ssid, s_config.wifi_ssid, sizeof(sta_cfg.sta.ssid));
        strlcpy((char *)sta_cfg.sta.password, s_config.wifi_pass,
                sizeof(sta_cfg.sta.password));
        bool open = s_config.wifi_pass[0] == '\0';
        xSemaphoreGive(s_state_mutex);
        sta_cfg.sta.threshold.authmode = open ? WIFI_AUTH_OPEN : WIFI_AUTH_WPA2_PSK;
        sta_cfg.sta.pmf_cfg.capable = true;
        sta_cfg.sta.pmf_cfg.required = false;
        ESP_ERROR_CHECK(esp_wifi_set_config(WIFI_IF_STA, &sta_cfg));
    }

    ESP_ERROR_CHECK(esp_wifi_start());
    start_mdns_if_needed();
    ESP_LOGI(TAG, "AP available: http://192.168.4.1, mDNS: http://%s.local",
             CATWHEEL_HOSTNAME);
}

static esp_err_t read_request_body(httpd_req_t *req, char **body) {
    if (req->content_len <= 0 || req->content_len > MAX_HTTP_BODY) {
        return ESP_ERR_INVALID_SIZE;
    }

    char *buf = calloc(1, req->content_len + 1);
    if (buf == NULL) {
        return ESP_ERR_NO_MEM;
    }

    int received = 0;
    while (received < req->content_len) {
        int r = httpd_req_recv(req, buf + received, req->content_len - received);
        if (r <= 0) {
            free(buf);
            return ESP_FAIL;
        }
        received += r;
    }

    buf[req->content_len] = '\0';
    *body = buf;
    return ESP_OK;
}

static esp_err_t send_json_response(httpd_req_t *req, cJSON *root) {
    char *payload = cJSON_PrintUnformatted(root);
    cJSON_Delete(root);
    if (payload == NULL) {
        httpd_resp_send_err(req, HTTPD_500_INTERNAL_SERVER_ERROR, "json encode failed");
        return ESP_FAIL;
    }

    httpd_resp_set_type(req, "application/json");
    httpd_resp_set_hdr(req, "Cache-Control", "no-store");
    esp_err_t err = httpd_resp_send(req, payload, HTTPD_RESP_USE_STRLEN);
    free(payload);
    return err;
}

static void json_add_item_cfg(cJSON *items_obj, const char *name,
                              const matrix_item_cfg_t *cfg) {
    cJSON *obj = cJSON_CreateObject();
    if (obj == NULL) {
        return;
    }
    cJSON_AddBoolToObject(obj, "enabled", cfg->enabled);
    cJSON_AddNumberToObject(obj, "row", cfg->row);
    char color[8];
    color_to_hex(cfg->color_rgb, color, sizeof(color));
    cJSON_AddStringToObject(obj, "color", color);
    cJSON_AddItemToObject(items_obj, name, obj);
}

static void format_minutes_hhmm(uint16_t minute_of_day, char *out, size_t out_len) {
    uint16_t clamped = (minute_of_day <= 1439U) ? minute_of_day : 1439U;
    unsigned hour = (unsigned)(clamped / 60U);
    unsigned minute = (unsigned)(clamped % 60U);
    snprintf(out, out_len, "%02u:%02u", hour, minute);
}

static uint16_t parse_hhmm_to_minutes(const char *text, uint16_t fallback) {
    if (text == NULL) {
        return fallback;
    }

    unsigned hour = 0;
    unsigned minute = 0;
    if (sscanf(text, "%u:%u", &hour, &minute) != 2) {
        return fallback;
    }
    if (hour > 23U || minute > 59U) {
        return fallback;
    }
    return (uint16_t)(hour * 60U + minute);
}

static uint16_t parse_minutes_field(const cJSON *obj, const char *key, uint16_t fallback) {
    const cJSON *v = cJSON_GetObjectItemCaseSensitive(obj, key);
    if (cJSON_IsString(v) && v->valuestring != NULL) {
        return parse_hhmm_to_minutes(v->valuestring, fallback);
    }
    if (cJSON_IsNumber(v)) {
        int value = v->valueint;
        if (value < 0) {
            value = 0;
        }
        if (value > 1439) {
            value = 1439;
        }
        return (uint16_t)value;
    }
    return fallback;
}

static esp_err_t root_get_handler(httpd_req_t *req) {
    httpd_resp_set_type(req, "text/html");
    httpd_resp_set_hdr(req, "Cache-Control", "no-store");
    return httpd_resp_send(req, catwheel_get_dashboard_page(), HTTPD_RESP_USE_STRLEN);
}

static esp_err_t settings_get_handler(httpd_req_t *req) {
    httpd_resp_set_type(req, "text/html");
    httpd_resp_set_hdr(req, "Cache-Control", "no-store");
    return httpd_resp_send(req, catwheel_get_settings_page(), HTTPD_RESP_USE_STRLEN);
}

static esp_err_t status_get_handler(httpd_req_t *req) {
    app_config_t cfg;
    stats_state_t stats;
    running_session_t running;
    runtime_state_t runtime;

    xSemaphoreTake(s_state_mutex, portMAX_DELAY);
    cfg = s_config;
    stats = s_stats;
    running = s_running_session;
    runtime = s_runtime;
    xSemaphoreGive(s_state_mutex);

    int64_t now_us = esp_timer_get_time();
    uint32_t session_duration_s =
        running.active ? (uint32_t)((now_us - running.start_us) / 1000000LL) : 0;

    cJSON *root = cJSON_CreateObject();
    cJSON_AddNumberToObject(root, "total_distance_m", stats.total_distance_m);
    cJSON_AddNumberToObject(root, "distance_today_m", stats.distance_today_m);
    cJSON_AddNumberToObject(root, "top_speed_kmh", clamp_speed_mps(stats.top_speed_mps) * 3.6);
    cJSON_AddNumberToObject(root, "top_speed_today_kmh",
                            clamp_speed_mps(stats.top_speed_today_mps) * 3.6);
    cJSON_AddNumberToObject(root, "current_speed_kmh",
                            clamp_speed_mps(runtime.current_speed_mps) * 3.6);
    cJSON_AddNumberToObject(root, "total_pulses", (double)stats.total_pulses);
    cJSON_AddBoolToObject(root, "session_active", running.active);
    cJSON_AddNumberToObject(root, "session_distance_m", running.distance_m);
    cJSON_AddNumberToObject(root, "session_duration_s", session_duration_s);
    cJSON_AddNumberToObject(root, "day_id", stats.day_id);
    cJSON_AddBoolToObject(root, "wifi_connected", runtime.wifi_connected);
    cJSON_AddStringToObject(root, "sta_ip", runtime.sta_ip);
    cJSON_AddStringToObject(root, "matrix_mode", matrix_mode_to_str(cfg.matrix_mode));
    cJSON_AddNumberToObject(root, "meters_per_pulse", cfg.meters_per_pulse);

    int64_t now_epoch = epoch_now_s();
    bool time_synced = is_time_synced_epoch(now_epoch);
    char now_iso[32];
    if (time_synced) {
        format_iso_time(now_epoch, now_iso, sizeof(now_iso));
    } else {
        snprintf(now_iso, sizeof(now_iso), "Time sync pending");
    }
    cJSON_AddNumberToObject(root, "now_epoch_s", time_synced ? (double)now_epoch : 0.0);
    cJSON_AddStringToObject(root, "now_iso", now_iso);
    cJSON_AddBoolToObject(root, "time_synced", time_synced);

    return send_json_response(req, root);
}

static cJSON *build_config_json_locked(void) {
    cJSON *root = cJSON_CreateObject();
    cJSON_AddStringToObject(root, "wifi_ssid", s_config.wifi_ssid);
    cJSON_AddStringToObject(root, "wifi_pass", "");
    cJSON_AddStringToObject(root, "language", ui_language_to_str(s_ui_language));
    cJSON_AddStringToObject(root, "matrix_mode", matrix_mode_to_str(s_config.matrix_mode));
    cJSON_AddNumberToObject(root, "auto_off_sec", s_config.auto_off_sec);
    cJSON_AddNumberToObject(root, "meters_per_pulse", s_config.meters_per_pulse);
    cJSON_AddNumberToObject(root, "global_brightness_pct", s_config.global_brightness_pct);

    cJSON *off_window = cJSON_CreateObject();
    if (off_window != NULL) {
        char start_buf[6];
        char end_buf[6];
        format_minutes_hhmm(s_config.off_window_start_minute, start_buf, sizeof(start_buf));
        format_minutes_hhmm(s_config.off_window_end_minute, end_buf, sizeof(end_buf));
        cJSON_AddBoolToObject(off_window, "enabled", s_config.off_window_enabled);
        cJSON_AddStringToObject(off_window, "start", start_buf);
        cJSON_AddStringToObject(off_window, "end", end_buf);
        cJSON_AddItemToObject(root, "off_window", off_window);
    }

    cJSON *brightness_windows = cJSON_CreateArray();
    if (brightness_windows != NULL) {
        for (uint8_t i = 0; i < BRIGHTNESS_WINDOWS_MAX; i++) {
            const brightness_window_cfg_t *w = &s_config.brightness_windows[i];
            cJSON *obj = cJSON_CreateObject();
            if (obj == NULL) {
                continue;
            }
            char start_buf[6];
            char end_buf[6];
            format_minutes_hhmm(w->start_minute, start_buf, sizeof(start_buf));
            format_minutes_hhmm(w->end_minute, end_buf, sizeof(end_buf));
            cJSON_AddBoolToObject(obj, "enabled", w->enabled);
            cJSON_AddStringToObject(obj, "start", start_buf);
            cJSON_AddStringToObject(obj, "end", end_buf);
            cJSON_AddNumberToObject(obj, "brightness_pct", w->brightness_pct);
            cJSON_AddItemToArray(brightness_windows, obj);
        }
        cJSON_AddItemToObject(root, "brightness_windows", brightness_windows);
    }

    cJSON *items = cJSON_CreateObject();
    for (int i = 0; i < METRIC_COUNT; i++) {
        json_add_item_cfg(items, k_metric_keys[i], &s_config.items[i]);
    }
    cJSON_AddItemToObject(root, "items", items);

    cJSON *graph = cJSON_CreateObject();
    if (graph != NULL) {
        cJSON_AddBoolToObject(graph, "enabled", s_config.graph_item.enabled);
        cJSON_AddNumberToObject(graph, "row", s_config.graph_item.row);
        char color[8];
        color_to_hex(s_config.graph_item.color_rgb, color, sizeof(color));
        cJSON_AddStringToObject(graph, "color", color);
        cJSON_AddItemToObject(root, "graph", graph);
    }
    return root;
}

static esp_err_t config_get_handler(httpd_req_t *req) {
    xSemaphoreTake(s_state_mutex, portMAX_DELAY);
    cJSON *root = build_config_json_locked();
    xSemaphoreGive(s_state_mutex);
    return send_json_response(req, root);
}

static void parse_item_cfg_from_json(matrix_item_cfg_t *cfg, const cJSON *json) {
    if (json == NULL || !cJSON_IsObject(json)) {
        return;
    }

    const cJSON *enabled = cJSON_GetObjectItemCaseSensitive(json, "enabled");
    if (cJSON_IsBool(enabled)) {
        cfg->enabled = cJSON_IsTrue(enabled);
    }

    const cJSON *row = cJSON_GetObjectItemCaseSensitive(json, "row");
    if (cJSON_IsNumber(row)) {
        int value = row->valueint;
        if (value < 0) {
            value = 0;
        }
        if (value > 7) {
            value = 7;
        }
        cfg->row = (uint8_t)value;
    }

    const cJSON *color = cJSON_GetObjectItemCaseSensitive(json, "color");
    if (cJSON_IsString(color) && color->valuestring != NULL) {
        cfg->color_rgb = parse_color_hex(color->valuestring, cfg->color_rgb);
    }
}

static esp_err_t config_post_handler(httpd_req_t *req) {
    char *body = NULL;
    if (read_request_body(req, &body) != ESP_OK) {
        httpd_resp_send_err(req, HTTPD_400_BAD_REQUEST, "invalid body");
        return ESP_FAIL;
    }

    cJSON *json = cJSON_Parse(body);
    free(body);
    if (json == NULL) {
        httpd_resp_send_err(req, HTTPD_400_BAD_REQUEST, "json parse failed");
        return ESP_FAIL;
    }

    bool wifi_changed = false;
    app_config_t updated;

    xSemaphoreTake(s_state_mutex, portMAX_DELAY);
    updated = s_config;

    const cJSON *wifi_ssid = cJSON_GetObjectItemCaseSensitive(json, "wifi_ssid");
    if (cJSON_IsString(wifi_ssid) && wifi_ssid->valuestring != NULL) {
        if (strncmp(updated.wifi_ssid, wifi_ssid->valuestring, sizeof(updated.wifi_ssid)) !=
            0) {
            wifi_changed = true;
        }
        strlcpy(updated.wifi_ssid, wifi_ssid->valuestring, sizeof(updated.wifi_ssid));
    }

    const cJSON *wifi_pass = cJSON_GetObjectItemCaseSensitive(json, "wifi_pass");
    if (cJSON_IsString(wifi_pass) && wifi_pass->valuestring != NULL) {
        // Empty password from web UI means "keep existing password".
        if (wifi_pass->valuestring[0] != '\0') {
            if (strncmp(updated.wifi_pass, wifi_pass->valuestring,
                        sizeof(updated.wifi_pass)) != 0) {
                wifi_changed = true;
            }
            strlcpy(updated.wifi_pass, wifi_pass->valuestring, sizeof(updated.wifi_pass));
        }
    }

    const cJSON *language = cJSON_GetObjectItemCaseSensitive(json, "language");
    if (cJSON_IsString(language) && language->valuestring != NULL) {
        ui_language_t parsed = ui_language_from_str(language->valuestring);
        if (parsed != s_ui_language) {
            s_ui_language = parsed;
            s_ui_language_dirty = true;
        }
    }

    const cJSON *mode = cJSON_GetObjectItemCaseSensitive(json, "matrix_mode");
    if (cJSON_IsString(mode) && mode->valuestring != NULL) {
        updated.matrix_mode = matrix_mode_from_str(mode->valuestring);
    }

    const cJSON *auto_off = cJSON_GetObjectItemCaseSensitive(json, "auto_off_sec");
    if (cJSON_IsNumber(auto_off)) {
        int value = auto_off->valueint;
        if (value < 1) {
            value = 1;
        }
        if (value > 86400) {
            value = 86400;
        }
        updated.auto_off_sec = (uint16_t)value;
    }

    const cJSON *meters = cJSON_GetObjectItemCaseSensitive(json, "meters_per_pulse");
    if (cJSON_IsNumber(meters)) {
        double value = meters->valuedouble;
        if (value < 0.001) {
            value = 0.001;
        }
        if (value > 5.0) {
            value = 5.0;
        }
        updated.meters_per_pulse = (float)value;
    }

    const cJSON *global_brightness =
        cJSON_GetObjectItemCaseSensitive(json, "global_brightness_pct");
    if (cJSON_IsNumber(global_brightness)) {
        int value = global_brightness->valueint;
        if (value < 1) {
            value = 1;
        }
        if (value > 100) {
            value = 100;
        }
        updated.global_brightness_pct = (uint8_t)value;
    }

    const cJSON *off_window = cJSON_GetObjectItemCaseSensitive(json, "off_window");
    if (cJSON_IsObject(off_window)) {
        const cJSON *enabled = cJSON_GetObjectItemCaseSensitive(off_window, "enabled");
        if (cJSON_IsBool(enabled)) {
            updated.off_window_enabled = cJSON_IsTrue(enabled);
        }
        updated.off_window_start_minute = parse_minutes_field(
            off_window, "start", updated.off_window_start_minute);
        updated.off_window_end_minute =
            parse_minutes_field(off_window, "end", updated.off_window_end_minute);
    }

    const cJSON *brightness_windows =
        cJSON_GetObjectItemCaseSensitive(json, "brightness_windows");
    if (cJSON_IsArray(brightness_windows)) {
        for (uint8_t i = 0; i < BRIGHTNESS_WINDOWS_MAX; i++) {
            const cJSON *w = cJSON_GetArrayItem(brightness_windows, i);
            if (!cJSON_IsObject(w)) {
                continue;
            }
            brightness_window_cfg_t *dst = &updated.brightness_windows[i];
            const cJSON *enabled = cJSON_GetObjectItemCaseSensitive(w, "enabled");
            if (cJSON_IsBool(enabled)) {
                dst->enabled = cJSON_IsTrue(enabled);
            }
            dst->start_minute = parse_minutes_field(w, "start", dst->start_minute);
            dst->end_minute = parse_minutes_field(w, "end", dst->end_minute);
            const cJSON *brightness = cJSON_GetObjectItemCaseSensitive(w, "brightness_pct");
            if (cJSON_IsNumber(brightness)) {
                int value = brightness->valueint;
                if (value < 1) {
                    value = 1;
                }
                if (value > 100) {
                    value = 100;
                }
                dst->brightness_pct = (uint8_t)value;
            }
        }
    }

    const cJSON *items = cJSON_GetObjectItemCaseSensitive(json, "items");
    if (cJSON_IsObject(items)) {
        for (int i = 0; i < METRIC_COUNT; i++) {
            const cJSON *obj = cJSON_GetObjectItemCaseSensitive(items, k_metric_keys[i]);
            parse_item_cfg_from_json(&updated.items[i], obj);
        }
    }

    const cJSON *graph = cJSON_GetObjectItemCaseSensitive(json, "graph");
    if (cJSON_IsObject(graph)) {
        parse_item_cfg_from_json(&updated.graph_item, graph);
    }

    s_config = updated;
    s_config_dirty = true;
    xSemaphoreGive(s_state_mutex);
    cJSON_Delete(json);

    if (wifi_changed) {
        wifi_apply_sta_config();
    }
    persist_dirty_state();

    cJSON *resp = cJSON_CreateObject();
    cJSON_AddBoolToObject(resp, "ok", true);
    cJSON_AddBoolToObject(resp, "wifi_updated", wifi_changed);
    return send_json_response(req, resp);
}

static size_t snapshot_sessions(session_record_t *out, size_t max_items) {
    size_t copied = 0;
    xSemaphoreTake(s_state_mutex, portMAX_DELAY);
    size_t count = s_sessions.count;
    if (count > max_items) {
        count = max_items;
    }

    size_t oldest =
        (s_sessions.next_index + MAX_SESSIONS - s_sessions.count) % MAX_SESSIONS;
    size_t skip = s_sessions.count - count;
    oldest = (oldest + skip) % MAX_SESSIONS;

    for (size_t i = 0; i < count; i++) {
        size_t idx = (oldest + i) % MAX_SESSIONS;
        out[i] = s_sessions.records[idx];
    }
    copied = count;
    xSemaphoreGive(s_state_mutex);
    return copied;
}

static cJSON *session_to_json(const session_record_t *rec) {
    cJSON *obj = cJSON_CreateObject();
    cJSON_AddNumberToObject(obj, "start_epoch_s", (double)rec->start_epoch_s);
    cJSON_AddNumberToObject(obj, "end_epoch_s", (double)rec->end_epoch_s);
    cJSON_AddNumberToObject(obj, "duration_s", rec->duration_s);
    cJSON_AddNumberToObject(obj, "pulses", rec->pulses);
    cJSON_AddNumberToObject(obj, "distance_m", rec->distance_m);
    cJSON_AddNumberToObject(obj, "avg_speed_kmh", clamp_speed_mps(rec->avg_speed_mps) * 3.6);
    cJSON_AddNumberToObject(obj, "top_speed_kmh", clamp_speed_mps(rec->top_speed_mps) * 3.6);
    char start_iso[32];
    format_iso_time(rec->start_epoch_s, start_iso, sizeof(start_iso));
    cJSON_AddStringToObject(obj, "start_iso", start_iso);
    return obj;
}

static esp_err_t sessions_get_handler(httpd_req_t *req) {
    session_record_t *buffer = calloc(MAX_SESSIONS, sizeof(session_record_t));
    if (buffer == NULL) {
        httpd_resp_send_err(req, HTTPD_500_INTERNAL_SERVER_ERROR, "oom");
        return ESP_ERR_NO_MEM;
    }

    size_t count = snapshot_sessions(buffer, MAX_SESSIONS);

    cJSON *root = cJSON_CreateObject();
    cJSON_AddNumberToObject(root, "count", count);
    cJSON *arr = cJSON_CreateArray();
    for (size_t i = 0; i < count; i++) {
        cJSON_AddItemToArray(arr, session_to_json(&buffer[i]));
    }
    cJSON_AddItemToObject(root, "sessions", arr);

    free(buffer);
    return send_json_response(req, root);
}

static esp_err_t backup_get_handler(httpd_req_t *req) {
    session_record_t *buffer = calloc(MAX_SESSIONS, sizeof(session_record_t));
    if (buffer == NULL) {
        httpd_resp_send_err(req, HTTPD_500_INTERNAL_SERVER_ERROR, "oom");
        return ESP_ERR_NO_MEM;
    }

    app_config_t cfg;
    stats_state_t stats;

    xSemaphoreTake(s_state_mutex, portMAX_DELAY);
    cfg = s_config;
    stats = s_stats;
    xSemaphoreGive(s_state_mutex);

    size_t count = snapshot_sessions(buffer, MAX_SESSIONS);

    cJSON *root = cJSON_CreateObject();
    cJSON_AddNumberToObject(root, "version", 1);
    cJSON_AddNumberToObject(root, "exported_at_epoch_s", (double)epoch_now_s());

    cJSON *summary = cJSON_CreateObject();
    cJSON_AddNumberToObject(summary, "total_distance_m", stats.total_distance_m);
    cJSON_AddNumberToObject(summary, "distance_today_m", stats.distance_today_m);
    cJSON_AddNumberToObject(summary, "top_speed_mps", clamp_speed_mps(stats.top_speed_mps));
    cJSON_AddNumberToObject(summary, "top_speed_today_mps",
                            clamp_speed_mps(stats.top_speed_today_mps));
    cJSON_AddNumberToObject(summary, "total_pulses", (double)stats.total_pulses);
    cJSON_AddNumberToObject(summary, "day_id", stats.day_id);
    cJSON_AddItemToObject(root, "summary", summary);

    cJSON *config = cJSON_CreateObject();
    cJSON_AddStringToObject(config, "wifi_ssid", cfg.wifi_ssid);
    cJSON_AddBoolToObject(config, "wifi_pass_set", cfg.wifi_pass[0] != '\0');
    cJSON_AddStringToObject(config, "language", ui_language_to_str(s_ui_language));
    cJSON_AddStringToObject(config, "matrix_mode", matrix_mode_to_str(cfg.matrix_mode));
    cJSON_AddNumberToObject(config, "auto_off_sec", cfg.auto_off_sec);
    cJSON_AddNumberToObject(config, "meters_per_pulse", cfg.meters_per_pulse);
    cJSON_AddNumberToObject(config, "global_brightness_pct", cfg.global_brightness_pct);

    cJSON *off_window = cJSON_CreateObject();
    if (off_window != NULL) {
        char start_buf[6];
        char end_buf[6];
        format_minutes_hhmm(cfg.off_window_start_minute, start_buf, sizeof(start_buf));
        format_minutes_hhmm(cfg.off_window_end_minute, end_buf, sizeof(end_buf));
        cJSON_AddBoolToObject(off_window, "enabled", cfg.off_window_enabled);
        cJSON_AddStringToObject(off_window, "start", start_buf);
        cJSON_AddStringToObject(off_window, "end", end_buf);
        cJSON_AddItemToObject(config, "off_window", off_window);
    }

    cJSON *brightness_windows = cJSON_CreateArray();
    if (brightness_windows != NULL) {
        for (uint8_t i = 0; i < BRIGHTNESS_WINDOWS_MAX; i++) {
            const brightness_window_cfg_t *w = &cfg.brightness_windows[i];
            cJSON *obj = cJSON_CreateObject();
            if (obj == NULL) {
                continue;
            }
            char start_buf[6];
            char end_buf[6];
            format_minutes_hhmm(w->start_minute, start_buf, sizeof(start_buf));
            format_minutes_hhmm(w->end_minute, end_buf, sizeof(end_buf));
            cJSON_AddBoolToObject(obj, "enabled", w->enabled);
            cJSON_AddStringToObject(obj, "start", start_buf);
            cJSON_AddStringToObject(obj, "end", end_buf);
            cJSON_AddNumberToObject(obj, "brightness_pct", w->brightness_pct);
            cJSON_AddItemToArray(brightness_windows, obj);
        }
        cJSON_AddItemToObject(config, "brightness_windows", brightness_windows);
    }

    cJSON *items = cJSON_CreateObject();
    for (int i = 0; i < METRIC_COUNT; i++) {
        json_add_item_cfg(items, k_metric_keys[i], &cfg.items[i]);
    }
    cJSON_AddItemToObject(config, "items", items);

    cJSON *graph = cJSON_CreateObject();
    if (graph != NULL) {
        cJSON_AddBoolToObject(graph, "enabled", cfg.graph_item.enabled);
        cJSON_AddNumberToObject(graph, "row", cfg.graph_item.row);
        char color[8];
        color_to_hex(cfg.graph_item.color_rgb, color, sizeof(color));
        cJSON_AddStringToObject(graph, "color", color);
        cJSON_AddItemToObject(config, "graph", graph);
    }
    cJSON_AddItemToObject(root, "config", config);

    cJSON *sessions = cJSON_CreateArray();
    for (size_t i = 0; i < count; i++) {
        cJSON_AddItemToArray(sessions, session_to_json(&buffer[i]));
    }
    cJSON_AddItemToObject(root, "sessions", sessions);

    free(buffer);
    return send_json_response(req, root);
}

static void parse_backup_summary(stats_state_t *stats, const cJSON *summary) {
    if (summary == NULL || !cJSON_IsObject(summary)) {
        return;
    }

    const cJSON *v = cJSON_GetObjectItemCaseSensitive(summary, "total_distance_m");
    if (cJSON_IsNumber(v)) {
        stats->total_distance_m = v->valuedouble;
    }
    v = cJSON_GetObjectItemCaseSensitive(summary, "distance_today_m");
    if (cJSON_IsNumber(v)) {
        stats->distance_today_m = v->valuedouble;
    }
    v = cJSON_GetObjectItemCaseSensitive(summary, "top_speed_mps");
    if (cJSON_IsNumber(v)) {
        stats->top_speed_mps = clamp_speed_mps(v->valuedouble);
    }
    v = cJSON_GetObjectItemCaseSensitive(summary, "top_speed_today_mps");
    if (cJSON_IsNumber(v)) {
        stats->top_speed_today_mps = clamp_speed_mps(v->valuedouble);
    }
    v = cJSON_GetObjectItemCaseSensitive(summary, "total_pulses");
    if (cJSON_IsNumber(v) && v->valuedouble >= 0) {
        stats->total_pulses = (uint64_t)v->valuedouble;
    }
    v = cJSON_GetObjectItemCaseSensitive(summary, "day_id");
    if (cJSON_IsNumber(v)) {
        stats->day_id = v->valueint;
    }
}

static void parse_backup_config(app_config_t *cfg, const cJSON *config, bool *wifi_changed) {
    if (config == NULL || !cJSON_IsObject(config)) {
        return;
    }

    const cJSON *v = cJSON_GetObjectItemCaseSensitive(config, "wifi_ssid");
    if (cJSON_IsString(v) && v->valuestring != NULL) {
        if (strncmp(cfg->wifi_ssid, v->valuestring, sizeof(cfg->wifi_ssid)) != 0) {
            *wifi_changed = true;
        }
        strlcpy(cfg->wifi_ssid, v->valuestring, sizeof(cfg->wifi_ssid));
    }

    v = cJSON_GetObjectItemCaseSensitive(config, "wifi_pass");
    if (cJSON_IsString(v) && v->valuestring != NULL) {
        if (strncmp(cfg->wifi_pass, v->valuestring, sizeof(cfg->wifi_pass)) != 0) {
            *wifi_changed = true;
        }
        strlcpy(cfg->wifi_pass, v->valuestring, sizeof(cfg->wifi_pass));
    } else {
        v = cJSON_GetObjectItemCaseSensitive(config, "wifi_pass_set");
        if (cJSON_IsBool(v) && !cJSON_IsTrue(v) && cfg->wifi_pass[0] != '\0') {
            cfg->wifi_pass[0] = '\0';
            *wifi_changed = true;
        }
    }

    v = cJSON_GetObjectItemCaseSensitive(config, "language");
    if (cJSON_IsString(v) && v->valuestring != NULL) {
        ui_language_t parsed = ui_language_from_str(v->valuestring);
        if (parsed != s_ui_language) {
            s_ui_language = parsed;
            s_ui_language_dirty = true;
        }
    }

    v = cJSON_GetObjectItemCaseSensitive(config, "matrix_mode");
    if (cJSON_IsString(v) && v->valuestring != NULL) {
        cfg->matrix_mode = matrix_mode_from_str(v->valuestring);
    }

    v = cJSON_GetObjectItemCaseSensitive(config, "auto_off_sec");
    if (cJSON_IsNumber(v)) {
        int value = v->valueint;
        if (value < 1) {
            value = 1;
        }
        if (value > 86400) {
            value = 86400;
        }
        cfg->auto_off_sec = (uint16_t)value;
    }

    v = cJSON_GetObjectItemCaseSensitive(config, "meters_per_pulse");
    if (cJSON_IsNumber(v)) {
        double value = v->valuedouble;
        if (value < 0.001) {
            value = 0.001;
        }
        if (value > 5.0) {
            value = 5.0;
        }
        cfg->meters_per_pulse = (float)value;
    }

    v = cJSON_GetObjectItemCaseSensitive(config, "global_brightness_pct");
    if (cJSON_IsNumber(v)) {
        int value = v->valueint;
        if (value < 1) {
            value = 1;
        }
        if (value > 100) {
            value = 100;
        }
        cfg->global_brightness_pct = (uint8_t)value;
    }

    const cJSON *off_window = cJSON_GetObjectItemCaseSensitive(config, "off_window");
    if (cJSON_IsObject(off_window)) {
        v = cJSON_GetObjectItemCaseSensitive(off_window, "enabled");
        if (cJSON_IsBool(v)) {
            cfg->off_window_enabled = cJSON_IsTrue(v);
        }
        cfg->off_window_start_minute =
            parse_minutes_field(off_window, "start", cfg->off_window_start_minute);
        cfg->off_window_end_minute =
            parse_minutes_field(off_window, "end", cfg->off_window_end_minute);
    }

    const cJSON *brightness_windows =
        cJSON_GetObjectItemCaseSensitive(config, "brightness_windows");
    if (cJSON_IsArray(brightness_windows)) {
        for (uint8_t i = 0; i < BRIGHTNESS_WINDOWS_MAX; i++) {
            const cJSON *w = cJSON_GetArrayItem(brightness_windows, i);
            if (!cJSON_IsObject(w)) {
                continue;
            }
            brightness_window_cfg_t *dst = &cfg->brightness_windows[i];
            v = cJSON_GetObjectItemCaseSensitive(w, "enabled");
            if (cJSON_IsBool(v)) {
                dst->enabled = cJSON_IsTrue(v);
            }
            dst->start_minute = parse_minutes_field(w, "start", dst->start_minute);
            dst->end_minute = parse_minutes_field(w, "end", dst->end_minute);
            v = cJSON_GetObjectItemCaseSensitive(w, "brightness_pct");
            if (cJSON_IsNumber(v)) {
                int value = v->valueint;
                if (value < 1) {
                    value = 1;
                }
                if (value > 100) {
                    value = 100;
                }
                dst->brightness_pct = (uint8_t)value;
            }
        }
    }

    const cJSON *items = cJSON_GetObjectItemCaseSensitive(config, "items");
    if (cJSON_IsObject(items)) {
        for (int i = 0; i < METRIC_COUNT; i++) {
            const cJSON *item = cJSON_GetObjectItemCaseSensitive(items, k_metric_keys[i]);
            parse_item_cfg_from_json(&cfg->items[i], item);
        }
    }

    const cJSON *graph = cJSON_GetObjectItemCaseSensitive(config, "graph");
    if (cJSON_IsObject(graph)) {
        parse_item_cfg_from_json(&cfg->graph_item, graph);
    }
}

static void parse_backup_sessions(session_store_t *store, const cJSON *sessions) {
    if (sessions == NULL || !cJSON_IsArray(sessions)) {
        return;
    }

    init_default_sessions(store);
    const cJSON *item = NULL;
    cJSON_ArrayForEach(item, sessions) {
        if (!cJSON_IsObject(item)) {
            continue;
        }

        session_record_t rec = {0};
        const cJSON *v = cJSON_GetObjectItemCaseSensitive(item, "start_epoch_s");
        if (cJSON_IsNumber(v)) {
            rec.start_epoch_s = (int64_t)v->valuedouble;
        }
        v = cJSON_GetObjectItemCaseSensitive(item, "end_epoch_s");
        if (cJSON_IsNumber(v)) {
            rec.end_epoch_s = (int64_t)v->valuedouble;
        }
        v = cJSON_GetObjectItemCaseSensitive(item, "duration_s");
        if (cJSON_IsNumber(v) && v->valueint > 0) {
            rec.duration_s = (uint32_t)v->valueint;
        } else {
            rec.duration_s = 1;
        }
        v = cJSON_GetObjectItemCaseSensitive(item, "pulses");
        if (cJSON_IsNumber(v) && v->valueint >= 0) {
            rec.pulses = (uint32_t)v->valueint;
        }
        v = cJSON_GetObjectItemCaseSensitive(item, "distance_m");
        if (cJSON_IsNumber(v)) {
            rec.distance_m = (float)v->valuedouble;
        }
        v = cJSON_GetObjectItemCaseSensitive(item, "avg_speed_kmh");
        if (cJSON_IsNumber(v)) {
            rec.avg_speed_mps = clamp_speed_mps_f((float)(v->valuedouble / 3.6));
        }
        v = cJSON_GetObjectItemCaseSensitive(item, "top_speed_kmh");
        if (cJSON_IsNumber(v)) {
            rec.top_speed_mps = clamp_speed_mps_f((float)(v->valuedouble / 3.6));
        }

        session_store_push(store, &rec);
    }
}

static esp_err_t backup_upload_handler(httpd_req_t *req) {
    char *body = NULL;
    if (read_request_body(req, &body) != ESP_OK) {
        httpd_resp_send_err(req, HTTPD_400_BAD_REQUEST, "invalid body");
        return ESP_FAIL;
    }

    cJSON *json = cJSON_Parse(body);
    free(body);
    if (json == NULL) {
        httpd_resp_send_err(req, HTTPD_400_BAD_REQUEST, "json parse failed");
        return ESP_FAIL;
    }

    bool wifi_changed = false;

    xSemaphoreTake(s_state_mutex, portMAX_DELAY);
    parse_backup_summary(&s_stats, cJSON_GetObjectItemCaseSensitive(json, "summary"));
    parse_backup_config(&s_config, cJSON_GetObjectItemCaseSensitive(json, "config"),
                        &wifi_changed);
    parse_backup_sessions(&s_sessions, cJSON_GetObjectItemCaseSensitive(json, "sessions"));

    s_stats.version = STATS_VERSION;
    s_config.version = CONFIG_VERSION;
    s_sessions.version = SESSIONS_VERSION;
    s_stats_dirty = true;
    s_config_dirty = true;
    s_sessions_dirty = true;
    memset(&s_running_session, 0, sizeof(s_running_session));
    s_runtime.current_speed_mps = 0;
    xSemaphoreGive(s_state_mutex);

    cJSON_Delete(json);
    persist_dirty_state();

    if (wifi_changed) {
        wifi_apply_sta_config();
    }

    cJSON *resp = cJSON_CreateObject();
    cJSON_AddBoolToObject(resp, "ok", true);
    cJSON_AddBoolToObject(resp, "wifi_updated", wifi_changed);
    return send_json_response(req, resp);
}

static void start_http_server(void) {
    httpd_config_t config = HTTPD_DEFAULT_CONFIG();
    config.stack_size = 8192;
    config.max_uri_handlers = 14;

    if (httpd_start(&s_httpd, &config) != ESP_OK) {
        ESP_LOGE(TAG, "http server start failed");
        return;
    }

    const httpd_uri_t root = {
        .uri = "/",
        .method = HTTP_GET,
        .handler = root_get_handler,
        .user_ctx = NULL,
    };
    const httpd_uri_t settings = {
        .uri = "/settings",
        .method = HTTP_GET,
        .handler = settings_get_handler,
        .user_ctx = NULL,
    };
    const httpd_uri_t status = {
        .uri = "/api/status",
        .method = HTTP_GET,
        .handler = status_get_handler,
        .user_ctx = NULL,
    };
    const httpd_uri_t cfg_get = {
        .uri = "/api/config",
        .method = HTTP_GET,
        .handler = config_get_handler,
        .user_ctx = NULL,
    };
    const httpd_uri_t cfg_post = {
        .uri = "/api/config",
        .method = HTTP_POST,
        .handler = config_post_handler,
        .user_ctx = NULL,
    };
    const httpd_uri_t sessions = {
        .uri = "/api/sessions",
        .method = HTTP_GET,
        .handler = sessions_get_handler,
        .user_ctx = NULL,
    };
    const httpd_uri_t backup = {
        .uri = "/api/backup",
        .method = HTTP_GET,
        .handler = backup_get_handler,
        .user_ctx = NULL,
    };
    const httpd_uri_t backup_upload = {
        .uri = "/api/backup/upload",
        .method = HTTP_POST,
        .handler = backup_upload_handler,
        .user_ctx = NULL,
    };

    httpd_register_uri_handler(s_httpd, &root);
    httpd_register_uri_handler(s_httpd, &settings);
    httpd_register_uri_handler(s_httpd, &status);
    httpd_register_uri_handler(s_httpd, &cfg_get);
    httpd_register_uri_handler(s_httpd, &cfg_post);
    httpd_register_uri_handler(s_httpd, &sessions);
    httpd_register_uri_handler(s_httpd, &backup);
    httpd_register_uri_handler(s_httpd, &backup_upload);
}

static void sensor_gpio_init(void) {
    gpio_config_t cfg = {
        .pin_bit_mask = (1ULL << SENSOR_GPIO),
        .mode = GPIO_MODE_INPUT,
        // Sensor liefert kurzen HIGH-Puls (3.3V) je Ereignis:
        // Ruhezustand LOW halten und nur steigende Flanke zählen.
        .pull_up_en = GPIO_PULLUP_DISABLE,
        .pull_down_en = GPIO_PULLDOWN_ENABLE,
        .intr_type = GPIO_INTR_POSEDGE,
    };
    ESP_ERROR_CHECK(gpio_config(&cfg));
    ESP_ERROR_CHECK(gpio_install_isr_service(0));
    ESP_ERROR_CHECK(gpio_isr_handler_add(SENSOR_GPIO, sensor_isr, NULL));
}

