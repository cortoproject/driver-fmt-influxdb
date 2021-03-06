#include <driver/fmt/influxdb/influxdb.h>
#include <corto/string.h>
#include <stdlib.h>

const uint32_t SEC_TO_NANOSEC = 1000000000;
const corto_string TIMESTAMP_SEC_MEMBER = "sec";
const corto_string TIMESTAMP_NANOSEC_MEMBER = "nanosec";

typedef struct influxdbSer_t {
    corto_buffer b;
    corto_uint32 fieldCount;
} influxdbSer_t;

void influxdb_safeString(
    corto_buffer *b,
    corto_string source) {
    /* Measurements and Tags names cannot contain non-espaced spaces */
    char *ptr, ch;
    char buff[512];
    int pos = 0;
    for (ptr = source; (ch = *ptr); ptr++) {
        if (ch == ' ') {
            buff[pos] = '\\';
            buff[pos+1] = ch;
            pos++;
        } else {
            buff[pos] = ch;
        }

        pos++;
    }

    buff[pos] = '\0';
    corto_buffer_appendstr(b, buff);
}

corto_int16 influxdb_serScalar(
    corto_walk_opt *walk,
    corto_value *info,
    void *userData)
{
    influxdbSer_t *data = userData;
    void *ptr = corto_value_ptrof(info);
    corto_type t = corto_value_typeof(info);
    corto_object o = corto_value_objectof(info);
    corto_string str = NULL;

    /* Do not add timestamp members to the update buffer. sec and nanosec will
        be redundant */
    ///TODO Verify this is still happening.

    if (info->kind == CORTO_MEMBER) {
        if ((strcmp(TIMESTAMP_SEC_MEMBER, corto_idof(info->is.member.member)) == 0) ||
            (strcmp(TIMESTAMP_NANOSEC_MEMBER, corto_idof(info->is.member.member)) == 0)) {
            corto_error("Hit [%s]", corto_idof(info->is.member.member));
            goto unsupported;
        }
    }

    /* Only serialize types supported by influxdb */
    switch(corto_primitive(t)->kind) {
    case CORTO_BOOLEAN:
    case CORTO_INTEGER:
    case CORTO_UINTEGER:
    case CORTO_FLOAT:
    case CORTO_TEXT:
    case CORTO_ENUM:
        break;
    default:
        goto unsupported;
    }

    if (data->fieldCount) {
        corto_buffer_appendstr(&data->b, ",");
    }

    if (info->kind == CORTO_MEMBER) {
        influxdb_safeString(&data->b, corto_idof(info->is.member.member));
    } else {
        influxdb_safeString(&data->b, corto_idof(o));
    }
    corto_buffer_appendstr(&data->b, "=");

    switch(corto_primitive(t)->kind) {
    case CORTO_BOOLEAN:
        if (*(corto_bool*)ptr == true) {
            corto_buffer_appendstr(&data->b, "true");
        } else {
            corto_buffer_appendstr(&data->b, "false");
        }
        break;
    case CORTO_INTEGER:
        corto_ptr_cast(t, ptr, corto_string_o, &str);
        corto_buffer_appendstr(&data->b, str);
        corto_dealloc(str);
        corto_buffer_appendstr(&data->b, "i");
        break;
    case CORTO_UINTEGER:
        corto_ptr_cast(t, ptr, corto_string_o, &str);
        corto_buffer_appendstr(&data->b, str);
        corto_dealloc(str);
        corto_buffer_appendstr(&data->b, "i");
        break;
    case CORTO_FLOAT:
        corto_ptr_cast(t, ptr, corto_string_o, &str);
        corto_buffer_appendstr(&data->b, str);
        if ((corto_primitive(t)->kind != CORTO_FLOAT) && (corto_primitive(t)->kind != CORTO_BOOLEAN)) {
            corto_buffer_appendstr(&data->b, "i");
        }
        corto_dealloc(str);
        break;
    case CORTO_TEXT:
        if (*(corto_string*)ptr) {
            corto_buffer_appendstrn(&data->b, "\"", 1);
            corto_buffer_appendstr(&data->b, *(corto_string*)ptr);
            corto_buffer_appendstrn(&data->b, "\"", 1);
        } else {
            corto_buffer_appendstrn(&data->b, "\"\"", 2);
        }
        break;
    case CORTO_ENUM:
        corto_buffer_appendstrn(&data->b, "\"", 1);
        corto_ptr_cast(t, ptr, corto_string_o, &str);
        corto_buffer_appendstr(&data->b, str);
        corto_dealloc(str);
        corto_buffer_appendstrn(&data->b, "\"", 1);
        break;
    default:
        corto_assert(0, "unreachable code");
        break;
    }

    data->fieldCount++;

unsupported:
    return 0;
}

corto_int16 influxdb_serComplex(
    corto_walk_opt *walk,
    corto_value *info,
    void *userData)
{
    corto_type t = corto_value_typeof(info);

    if (t->kind == CORTO_COMPOSITE) {
        if (corto_walk_members(walk, info, userData)) {
            goto error;
        }
    } else if (t->kind == CORTO_COLLECTION) {
        if (corto_walk_elements(walk, info, userData)) {
            goto error;
        }
    } else {
        corto_throw("Unexpected serialize request for type [%d]", t->kind);
        goto error;
    }

    return 0;
error:
    return -1;
}

int16_t influxdb_serObject(
    corto_walk_opt *walk,
    corto_value *info,
    void *userData)
{
    influxdbSer_t *data = userData;
    corto_object o = corto_value_objectof(info);

    if (corto_walk_value(walk, info, userData)) {
        goto error;
    }

    /* Specific handling for timestamp data */
    if (corto_type_instanceof(corto_interface_o, corto_typeof(o))) {
        corto_member m = corto_interface_resolve_member_by_tag(
            corto_typeof(o),
            tags_time_last_modified_o);
        if (!m) {
            m = corto_interface_resolve_member_by_tag(
                corto_typeof(o),
                tags_time_created_o);
        }
        if (!m) {
            m = corto_interface_resolve_member_by_tag(
                corto_typeof(o),
                tags_time_received_o);
        }

        if (m && (corto_type_instanceof(corto_time_o, m->type))) {
            corto_time *time_o = (corto_time*)CORTO_OFFSET(o, m->offset);
            if (time_o == NULL)
            {
                goto error;
            }

            uint64_t ts = (uint64_t) time_o->sec * SEC_TO_NANOSEC +
                (uint64_t) time_o->nanosec;
            char buffer[64];
            corto_string timeStr = corto_ulltoa(ts, buffer, 10);
            corto_buffer_appendstr(&data->b, " ");
            corto_buffer_appendstr(&data->b, timeStr);
        }
    }

    return 0;
error:
    return -1;
}

int16_t influxdb_serItem(
    corto_walk_opt *walk,
    corto_value *info,
    void *userData)
{
    corto_type t = corto_value_typeof(info);
    corto_member m = info->is.member.member;
    corto_string id = corto_idof(m);

    /* Avoid serializing timestamp data into datafields. */
    if (corto_ll_count(m->tags)) {
        if (corto_ll_hasObject(m->tags, tags_time_last_modified_o) ||
            corto_ll_hasObject(m->tags, tags_time_created_o) ||
            corto_ll_hasObject(m->tags, tags_time_received_o)) {
            goto ignore;
        }
    }

    if ((t->kind == CORTO_COMPOSITE) || (t->kind == CORTO_COLLECTION)) {
        corto_fmt fmt = corto_fmt_lookup("text/json");
        char* json = (char*)corto_fmt_from_value(fmt, NULL, info);
        influxdbSer_t *data = userData;
        if (data->fieldCount) {
            corto_buffer_appendstrn(&data->b, ",", 1);
        }
        corto_buffer_appendstr(&data->b, id);
        corto_buffer_appendstrn(&data->b, "=\"", 2);
        influxdb_safeString(&data->b, json);
        corto_buffer_appendstrn(&data->b, "\"", 1);
        corto_dealloc(json);
        data->fieldCount++;
    } else {
        if (corto_walk_value(walk, info, userData)) {
            goto error;
        }
    }

    return 0;
ignore:
    return 0;
error:
    return -1;
}

corto_string influxdb_fromValue(
    corto_fmt_opt* opt,
    corto_value *v)
{
    influxdbSer_t walkData = {CORTO_BUFFER_INIT, 0};
    corto_walk_opt walk;
    corto_walk_init(&walk);

    /* Only serialize scalars */
    walk.access = CORTO_LOCAL|CORTO_PRIVATE;
    walk.accessKind = CORTO_NOT;
    walk.program[CORTO_PRIMITIVE] = influxdb_serScalar;
    walk.program[CORTO_COMPOSITE] = influxdb_serComplex;
    walk.program[CORTO_COLLECTION] = influxdb_serComplex;

    walk.metaprogram[CORTO_OBJECT] = influxdb_serObject;
    walk.metaprogram[CORTO_MEMBER] = influxdb_serItem;
    walk.metaprogram[CORTO_ELEMENT] = influxdb_serItem;

    if (v->kind == CORTO_OBJECT) {
        corto_object o = corto_value_objectof(v);
        corto_walk(&walk, o, &walkData);
    }
    else {
        corto_walk_value(&walk, v, &walkData);
    }

    return corto_buffer_str(&walkData.b);
}

/* Not supported */
corto_int16 influxdb_toValue(
    corto_fmt_opt* opt,
    corto_value *v,
    corto_string data)
{
    corto_throw("conversion from influx to corto not supported");
    return -1;
}

corto_int16 influxdb_toObject(
    corto_fmt_opt* opt,
    corto_object* o,
    corto_string s)
{
    corto_throw("conversion from influx to corto not supported");
    return -1;
}

corto_string influxdb_fromObject(
    corto_fmt_opt* opt,
    corto_object o)
{
    influxdbSer_t walkData = {CORTO_BUFFER_INIT, 0};
    corto_walk_opt walk;
    corto_walk_init(&walk);

    /* Only serialize scalars */
    walk.access = CORTO_LOCAL|CORTO_PRIVATE;
    walk.accessKind = CORTO_NOT;
    walk.metaprogram[CORTO_OBJECT] = influxdb_serObject;
    walk.program[CORTO_PRIMITIVE] = influxdb_serScalar;

    corto_walk(&walk, o, &walkData);

    return corto_buffer_str(&walkData.b);
}

corto_word influxdb_fromResult(
    corto_fmt_opt* opt,
    corto_record *r)
{
    CORTO_UNUSED(r);
    return 0;
}

corto_int16 influxdb_toResult(
    corto_fmt_opt* opt,
    corto_record *r,
    char *influx)
{
    corto_throw("conversion from influx to corto not supported");
    return -1;
}

void influxdb_release(
    char *data) {
    corto_dealloc(data);
}

corto_string influxdb_copy(
    char *data) {
    return corto_strdup(data);
}

int cortomain(int argc, char *argv[]) {
    CORTO_UNUSED(argc);
    CORTO_UNUSED(argv);
    return 0;
}
