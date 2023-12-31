/*
 * Copyright (c) 2022 - 2023 OSM Group @ HPI, University of Potsdam
 */

#ifndef BACKENDS_PNG_H_
#define BACKENDS_PNG_H_

#include "./tracefile.h"

int backend_png(struct smog_tracefile *tracefile, const char *path);

#endif  // BACKENDS_PNG_H_
