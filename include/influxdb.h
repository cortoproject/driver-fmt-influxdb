/* influxdb.h
 * This is the main package file. Include this file in other projects.
 * Only modify inside the header-end and body-end sections.
 */

#ifndef DRIVER_FMT_INFLUXDB_H
#define DRIVER_FMT_INFLUXDB_H

#include <driver/fmt/influxdb/_project.h>
#include <tags/c/c.h>
#include <corto/c/c.h>
#include <tags/tags.h>

/* $header() */
#ifdef __cplusplus
extern "C" {
#endif

/* Implements the line protocol used by InfluxDb */
corto_string influxdb_fromValue(
    corto_fmt_opt* opt,
    corto_value *v);

/* Stub: line protocol is only used to store data in InfluxDb so this function
 * will never be used. */
corto_int16 influxdb_toValue(
    corto_fmt_opt* opt,
    corto_value *v,
    corto_string data);

corto_int16 influxdb_toObject(
    corto_fmt_opt* opt,
    corto_object* o,
    char *s);

corto_string influxdb_fromObject(
    corto_fmt_opt* opt,
    corto_object o);

corto_word influxdb_fromResult(
    corto_fmt_opt* opt,
    corto_record *r);

corto_int16 influxdb_toResult(
    corto_fmt_opt* opt,
    corto_record *r,
    char *influx);

void influxdb_release(
    char *data);

corto_string influxdb_copy(
    char *data);

void influxdb_safeString(
    corto_buffer *b,
    corto_string source);

#ifdef __cplusplus
}
#endif
/* $end */

/* $body() */
/* Definitions here that need your package headers go here. */
/* $end */

#endif
