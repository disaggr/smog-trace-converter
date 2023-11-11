/*
 * Copyright (c) 2022 - 2023 OSM Group @ HPI, University of Potsdam
 */

#ifndef TRACEFILE_H_
#define TRACEFILE_H_

#include <stddef.h>
#include <sys/types.h>

struct smog_tracefile {
    char *buffer;
    size_t length;

    off_t *frame_offsets;
    size_t num_frames;
};

int tracefile_open(struct smog_tracefile *tracefile, const char *path);

void tracefile_close(struct smog_tracefile *tracefile);

int tracefile_index_frames(struct smog_tracefile *tracefile);

#endif  // TRACEFILE_H_

