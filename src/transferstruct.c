#include "transferstruct.h"

#include "log.h"
#include "util.h"

#include <stdlib.h>
#include <string.h>

size_t write_memory_callback(void *recv_data, size_t size, size_t nmemb,
                             void *userp)
{
    TransferStruct *ts = (TransferStruct *) userp;
    if (ts->type == CONTENT && ts->cf) {
        lprintf(transferstruct_debug, "ts->cf: %x", ts->cf);
        lprintf(transferstruct_debug, "thread %x locking ts_lock\n",
                pthread_self());
        PTHREAD_MUTEX_LOCK(&ts->cf->ts_lock);
    }
    size_t recv_size = size * nmemb;
    ts->data = realloc(ts->data, ts->curr_size + recv_size + 1);
    if (!ts->data) {
        /*
         * out of memory!
         */
        lprintf(fatal, "realloc failure!\n");
    }

    memmove(&ts->data[ts->curr_size], recv_data, recv_size);
    ts->curr_size += recv_size;
    ts->data[ts->curr_size] = '\0';

    if (ts->type == CONTENT && ts->cf) {
        lprintf(transferstruct_debug, "thread %x unlocking ts_lock\n",
                pthread_self());
        PTHREAD_MUTEX_UNLOCK(&ts->cf->ts_lock);
    }

    return recv_size;
}

long TransferStruct_read(TransferStruct *ts, char *const output_buf,
                         const off_t len, const off_t offset)
{
    while (1) {
        lprintf(transferstruct_debug, "thread %x locking ts_lock, cf: %x\n",
                pthread_self(), ts->cf);
        PTHREAD_MUTEX_LOCK(&ts->cf->ts_lock);

        off_t ts_offset = offset - ts->offset;
        if (offset >= 0 && (off_t) ts->curr_size >= len) {
            memmove(output_buf, ts->data + ts_offset, len);

            lprintf(transferstruct_debug, "thread %x unlocking ts_lock\n",
                    pthread_self());
            PTHREAD_MUTEX_UNLOCK(&ts->cf->ts_lock);

            return len;
        }

        lprintf(transferstruct_debug, "thread %x unlocking ts_lock\n",
                pthread_self());
        PTHREAD_MUTEX_UNLOCK(&ts->cf->ts_lock);
    }
}