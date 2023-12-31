/*
 * Copyright (c) 2022 - 2023 OSM Group @ HPI, University of Potsdam
 */

#ifndef BACKENDS_PNG_FRAMES_H_
#define BACKENDS_PNG_FRAMES_H_

#include "./tracefile.h"

#ifdef __cplusplus
extern "C" {
#endif

int backend_png_frames(struct smog_tracefile *tracefile, const char *path);

#ifdef __cplusplus
}
#endif

#endif  // BACKENDS_PNG_FRAMES_H_
