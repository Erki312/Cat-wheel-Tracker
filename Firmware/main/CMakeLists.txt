idf_component_register(
    # NOTE:
    # `data_logging_module.c`, `matrix_driver_module.c`, and `web_server_module.c`
    # are intentionally included from `main.c` and must NOT be added to `SRCS`.
    # Adding them here will compile them as standalone translation units and cause
    # many missing-type errors.
    SRCS
        "web_page.c"
        "font5x7.c"
        "main.c"
    PRIV_REQUIRES
        spi_flash
        esp_driver_gpio
        esp_event
        esp_http_server
        esp_netif
        esp_timer
        esp_wifi
        espressif__mdns
        json
        lwip
        nvs_flash
    INCLUDE_DIRS ""
)
