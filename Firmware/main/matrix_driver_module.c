// Split from main.c: LED matrix rendering and scan task

#ifndef CAT_WHEEL_INTERNAL_MODULE_BUILD
#error "matrix_driver_module.c must be included from main.c (do not add to SRCS)."
#endif

static void render_task_fn(void *arg) {
    char value[16];
    char line[24];

    while (true) {
        app_config_t cfg;
        stats_state_t stats;
        running_session_t running;
        runtime_state_t runtime;
        int64_t now_us = esp_timer_get_time();
        int64_t now_epoch_s = epoch_now_s();
        bool time_synced = is_time_synced_epoch(now_epoch_s);

        xSemaphoreTake(s_state_mutex, portMAX_DELAY);
        cfg = s_config;
        stats = s_stats;
        running = s_running_session;
        runtime = s_runtime;
        xSemaphoreGive(s_state_mutex);

        matrix_graph_push_speed((float)(runtime.current_speed_mps * 3.6));

        bool visible = matrix_should_be_on(&cfg, running.active, runtime.last_pulse_us, now_us,
                                           now_epoch_s, time_synced);
        s_matrix_visible = visible;
        s_matrix_brightness_pct =
            matrix_effective_brightness_pct(&cfg, now_epoch_s, time_synced);

        frame_clear(s_draw_frame);
        if (visible) {
            for (int metric = 0; metric < METRIC_COUNT; metric++) {
                const matrix_item_cfg_t *item = &cfg.items[metric];
                if (!item->enabled) {
                    continue;
                }

                int y = (item->row % 8) * 8;
                uint32_t color_rgb = item->color_rgb;

                switch (metric) {
                case METRIC_TOTAL_DISTANCE:
                    format_distance_short(stats.total_distance_m, value, sizeof(value));
                    snprintf(line, sizeof(line), "%s%s", k_metric_labels[metric], value);
                    break;
                case METRIC_TODAY_DISTANCE:
                    format_distance_short(stats.distance_today_m, value, sizeof(value));
                    snprintf(line, sizeof(line), "%s%s", k_metric_labels[metric], value);
                    break;
                case METRIC_TOP_SPEED:
                    format_speed_short(stats.top_speed_mps, value, sizeof(value));
                    snprintf(line, sizeof(line), "%s%s", k_metric_labels[metric], value);
                    break;
                case METRIC_TOP_SPEED_TODAY:
                    format_speed_short(stats.top_speed_today_mps, value, sizeof(value));
                    snprintf(line, sizeof(line), "%s%s", k_metric_labels[metric], value);
                    break;
                case METRIC_CURRENT_SPEED:
                default:
                    format_speed_short(runtime.current_speed_mps, value, sizeof(value));
                    snprintf(line, sizeof(line), "%s%s", k_metric_labels[metric], value);
                    break;
                }

                frame_draw_text(s_draw_frame, 0, y, line, color_rgb);
            }

            if (cfg.graph_item.enabled) {
                frame_draw_speed_graph(s_draw_frame, cfg.graph_item.color_rgb,
                                       cfg.graph_item.row);
            }
        }

        swap_frames();
        vTaskDelay(pdMS_TO_TICKS(RENDER_PERIOD_MS));
    }
}

static void matrix_scan_task_fn(void *arg) {
    while (true) {
        if (!s_matrix_visible) {
            GPIO.out_w1ts = MASK_OE;
            vTaskDelay(pdMS_TO_TICKS(10));
            continue;
        }

        uint8_t(*frame)[MATRIX_WIDTH] = (uint8_t(*)[MATRIX_WIDTH])s_scan_frame;

        for (uint8_t row = 0; row < MATRIX_SCAN_ROWS; row++) {
            for (uint8_t plane = 0; plane < MATRIX_PWM_PLANES; plane++) {
                GPIO.out_w1ts = MASK_OE;
                matrix_set_addr(row);

                for (int col = 0; col < MATRIX_WIDTH; col++) {
                    uint8_t top = rgb6_plane_to_rgb3(frame[row][col], plane);
                    uint8_t bottom =
                        rgb6_plane_to_rgb3(frame[row + MATRIX_SCAN_ROWS][col], plane);

                    matrix_set_data(top, bottom);
                    matrix_clock_pulse();
                }

                GPIO.out_w1ts = MASK_LAT;
                GPIO.out_w1tc = MASK_LAT;
                // Binary coded modulation: plane 0 = 1x on-time, plane 1 = 2x on-time.
                portENTER_CRITICAL(&s_matrix_timing_mux);
                GPIO.out_w1tc = MASK_OE;
                uint32_t base_on_us = (uint32_t)(MATRIX_ROW_ON_US_BASE << plane);
                uint32_t on_us =
                    (base_on_us * (uint32_t)s_matrix_brightness_pct + 50U) / 100U;
                if (on_us < 1U) {
                    on_us = 1U;
                }
                ets_delay_us(on_us);
                GPIO.out_w1ts = MASK_OE;
                portEXIT_CRITICAL(&s_matrix_timing_mux);
            }
        }
    }
}

