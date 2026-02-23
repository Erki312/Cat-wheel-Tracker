idf_component_register(
    SRCS "web_page.c" "font5x7.c" "main.c"
    PRIV_REQUIRES
        spi_flash
        esp_driver_gpio
        esp_event
        esp_http_server
        esp_netif
        esp_timer
        esp_wifi
        json
        lwip
        nvs_flash
    INCLUDE_DIRS ""
)
