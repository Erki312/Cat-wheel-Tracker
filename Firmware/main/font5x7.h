#pragma once
#include <stdint.h>

// 5x7 Pixel, ASCII 32..126 (95 Zeichen), spaltenweise, Bit0 = oberste Zeile
extern const uint8_t font5x7[95 * 5];

#define FONT5X7_WIDTH   5
#define FONT5X7_HEIGHT  7
