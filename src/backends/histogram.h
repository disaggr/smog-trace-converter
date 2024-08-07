/*
 * Copyright (c) 2022 - 2023 OSM Group @ HPI, University of Potsdam
 */

#ifndef BACKENDS_HISTOGRAM_H_
#define BACKENDS_HISTOGRAM_H_

#include "tracefile.h"

#ifdef __cplusplus
extern "C" {
#endif  // __cplusplus

int backend_histogram(struct smog_tracefile *tracefile, const char *path);

#ifdef __cplusplus
}
#endif  // __cplusplus

#endif  // BACKENDS_HISTOGRAM_H_