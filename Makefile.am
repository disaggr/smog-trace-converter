
AUTOMAKE_OPTIONS = subdir-objects

bin_PROGRAMS = smog-trace-converter

smog_trace_converter_CPPFLAGS = -Isrc/ -Wall -Wextra
smog_trace_converter_CXXFLAGS = @ARROW_CFLAGS@ @OPENMP_CFLAGS@
smog_trace_converter_LDADD = @ARROW_LIBS@

smog_trace_converter_SOURCES = src/smog-trace-converter.cpp
