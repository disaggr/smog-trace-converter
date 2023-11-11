/*
 * Copyright (c) 2022 - 2023 OSM Group @ HPI, University of Potsdam
 */

#ifndef BACKENDS_PARQUET_H_
#define BACKENDS_PARQUET_H_

#include "./tracefile.h"

#ifdef __cplusplus
extern "C" {
#endif  // __cplusplus

int backend_parquet(struct smog_tracefile *tracefile, const char *path);

#ifdef __cplusplus
}
#endif  // __cplusplus

#endif  // BACKENDS_PARQUET_H_
