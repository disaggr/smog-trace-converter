/*
 * Copyright (c) 2022 - 2023 OSM Group @ HPI, University of Potsdam
 */

#include "backends/png-frames.h"

#include <string.h>
#include <stdio.h>

#include "./util.h"
#include "./smog-trace-converter.h"

// TODO: Squarified Treemaps
// https://www.win.tue.nl/~vanwijk/stm.pdf

int backend_png_frames(struct smog_tracefile *tracefile, const char *path) {
    // check the outfile pattern
    if (!strstr(path, "%s")) {
        fprintf(stderr, "error: OUTFILE must contain '%%s'\n");
        return 1;
    }

    return 0;
}
