#include "shim_util.h"

#include "log/log.h"
#include "zarr/store.h"
#include "zarr/zarr_group.h"

#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

char*
shim_alloc_printf(const char* fmt, ...)
{
    va_list ap, ap2;
    va_start(ap, fmt);
    va_copy(ap2, ap);
    int n = vsnprintf(NULL, 0, fmt, ap);
    va_end(ap);
    if (n < 0) {
        va_end(ap2);
        return NULL;
    }
    char* buf = malloc((size_t)n + 1);
    if (!buf) {
        va_end(ap2);
        return NULL;
    }
    vsnprintf(buf, (size_t)n + 1, fmt, ap2);
    va_end(ap2);
    return buf;
}

int
shim_write_intermediate_groups(struct store* store, const char* key)
{
    if (!key) {
        return 0;
    }

    size_t len = strlen(key);
    // Prefix buffer: holds the evolving "a/b/c" path (null-terminated at
    // each '/' for mkdirs). Group-key buffer: prefix + "/zarr.json".
    // Both sized for the full key to avoid any fixed-size truncation.
    static const char SUFFIX[] = "/zarr.json";
    char* prefix = malloc(len + 1);
    char* group_key = malloc(len + sizeof(SUFFIX));
    int rc = 0;
    if (!prefix || !group_key) {
        rc = 1;
        goto done;
    }
    memcpy(prefix, key, len + 1);

    for (size_t i = 0; i < len; ++i) {
        if (prefix[i] == '/') {
            prefix[i] = '\0';
            if (store->mkdirs(store, prefix) != 0) {
                log_error("mkdirs failed for intermediate group '%s'", prefix);
                rc = 1;
                goto done;
            }
            memcpy(group_key, prefix, i);
            memcpy(group_key + i, SUFFIX, sizeof(SUFFIX));
            if (zarr_group_write_with_raw_attrs(store, group_key, "{}") != 0) {
                log_error("failed to write intermediate group metadata '%s'",
                          group_key);
                rc = 1;
                goto done;
            }
            prefix[i] = '/';
        }
    }

done:
    free(prefix);
    free(group_key);
    return rc;
}
