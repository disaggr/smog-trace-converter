/*
 * Copyright (c) 2022 - 2023 OSM Group @ HPI, University of Potsdam
 */

#ifndef UTIL_H_
#define UTIL_H_

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

size_t parse_size_string(const char *s);

const char *format_size_string(size_t s);

#ifdef __cplusplus
}
#endif

#endif  // UTIL_H_
