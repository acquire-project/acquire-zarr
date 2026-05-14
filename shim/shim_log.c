#include "shim_log.h"

#include "acquire.zarr.h"
#include "chucky_log.h"

#include <stdatomic.h>

#ifndef ACQUIRE_ZARR_API_VERSION
#define ACQUIRE_ZARR_API_VERSION "0.7.0"
#endif

static _Atomic int current_log_level = ZarrLogLevel_Info;

const char*
Zarr_get_api_version(void)
{
    return ACQUIRE_ZARR_API_VERSION;
}

// Forward current_log_level to chucky's log dispatcher. Default chucky level
// is CHUCKY_LOG_TRACE (0), so without this chucky emits everything to stderr
// regardless of the acquire-zarr log level.
void
shim_apply_log_level(void)
{
    ZarrLogLevel level =
      (ZarrLogLevel)atomic_load_explicit(&current_log_level,
                                         memory_order_relaxed);
    switch (level) {
        case ZarrLogLevel_Debug:
            chucky_log_set_quiet(0);
            chucky_log_set_level(CHUCKY_LOG_DEBUG);
            break;
        case ZarrLogLevel_Info:
            chucky_log_set_quiet(0);
            chucky_log_set_level(CHUCKY_LOG_INFO);
            break;
        case ZarrLogLevel_Warning:
            chucky_log_set_quiet(0);
            chucky_log_set_level(CHUCKY_LOG_WARN);
            break;
        case ZarrLogLevel_Error:
            chucky_log_set_quiet(0);
            chucky_log_set_level(CHUCKY_LOG_ERROR);
            break;
        case ZarrLogLevel_None:
        default:
            chucky_log_set_quiet(1);
            break;
    }
}

ZarrStatusCode
Zarr_set_log_level(ZarrLogLevel level)
{
    if (level < 0 || level >= ZarrLogLevelCount) {
        return ZarrStatusCode_InvalidArgument;
    }
    atomic_store_explicit(&current_log_level, (int)level, memory_order_relaxed);
    shim_apply_log_level();
    return ZarrStatusCode_Success;
}

ZarrLogLevel
Zarr_get_log_level(void)
{
    return (ZarrLogLevel)atomic_load_explicit(&current_log_level,
                                              memory_order_relaxed);
}

const char*
Zarr_get_status_message(ZarrStatusCode code)
{
    switch (code) {
        case ZarrStatusCode_Success:
            return "Success";
        case ZarrStatusCode_InvalidArgument:
            return "Invalid argument";
        case ZarrStatusCode_Overflow:
            return "Buffer overflow";
        case ZarrStatusCode_InvalidIndex:
            return "Invalid index";
        case ZarrStatusCode_NotYetImplemented:
            return "Not yet implemented";
        case ZarrStatusCode_InternalError:
            return "Internal error";
        case ZarrStatusCode_OutOfMemory:
            return "Out of memory";
        case ZarrStatusCode_IOError:
            return "I/O error";
        case ZarrStatusCode_CompressionError:
            return "Error compressing";
        case ZarrStatusCode_InvalidSettings:
            return "Invalid settings";
        case ZarrStatusCode_WillNotOverwrite:
            return "Refusing to overwrite existing data";
        case ZarrStatusCode_PartialWrite:
            return "Data partially written";
        case ZarrStatusCode_WriteOutOfBounds:
            return "Attempted write beyond array boundary";
        case ZarrStatusCode_KeyNotFound:
            return "Array key not found";
        default:
            return "Unknown error";
    }
}
