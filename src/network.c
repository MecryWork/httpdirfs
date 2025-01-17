#include "network.h"

#include "log.h"
#include "memcache.h"
#include "util.h"

#include <openssl/crypto.h>

#include <errno.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>
#include <stdbool.h>

/*
 * ----------------- External variables ----------------------
 */
CURLSH *CURL_SHARE;

/*
 * ----------------- Static variable -----------------------
 */
/** \brief curl multi interface handle */
static CURLM *curl_multi;
/** \brief  mutex for transfer functions */
static pthread_mutex_t transfer_lock;
/** \brief the lock array for cryptographic functions */
static pthread_mutex_t *crypto_lockarray;
/** \brief mutex for curl share interface itself */
static pthread_mutex_t curl_lock;

/*
 * -------------------- Functions --------------------------
 */
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-function"
/**
 * \brief OpenSSL 1.02 cryptography callback function
 * \details Required for OpenSSL 1.02, but not OpenSSL 1.1
 */
static void crypto_lock_callback(int mode, int type, char *file, int line)
{
    (void) file;
    (void) line;
    if (mode & CRYPTO_LOCK) {
        PTHREAD_MUTEX_LOCK(&(crypto_lockarray[type]));
    } else {
        PTHREAD_MUTEX_UNLOCK(&(crypto_lockarray[type]));
    }
}

/**
 * \brief OpenSSL 1.02 thread ID function
 * \details Required for OpenSSL 1.02, but not OpenSSL 1.1
 */
static unsigned long thread_id(void)
{
    unsigned long ret;

    ret = (unsigned long) pthread_self();
    return ret;
}

#pragma GCC diagnostic pop

static void crypto_lock_init(void)
{
    int i;

    crypto_lockarray =
        (pthread_mutex_t *) OPENSSL_malloc(CRYPTO_num_locks() *
                                           sizeof(pthread_mutex_t));
    for (i = 0; i < CRYPTO_num_locks(); i++) {
        if (pthread_mutex_init(&(crypto_lockarray[i]), NULL)) {
            lprintf(fatal, "crypto_lockarray[%d] initialisation \
failed!\n", i);
        };
    }

    CRYPTO_set_id_callback((unsigned long (*)()) thread_id);
    CRYPTO_set_locking_callback((void (*)()) crypto_lock_callback);
}

/**
 * \brief Curl share handle callback function
 * \details Adapted from:
 * https://curl.haxx.se/libcurl/c/threaded-shared-conn.html
 */
static void
curl_callback_lock(CURL *handle, curl_lock_data data,
                   curl_lock_access access, void *userptr)
{
    (void) access;              /* unused */
    (void) userptr;             /* unused */
    (void) handle;              /* unused */
    (void) data;                /* unused */
    PTHREAD_MUTEX_LOCK(&curl_lock);
}

static void
curl_callback_unlock(CURL *handle, curl_lock_data data, void *userptr)
{
    (void) userptr;             /* unused */
    (void) handle;              /* unused */
    (void) data;                /* unused */
    PTHREAD_MUTEX_UNLOCK(&curl_lock);
}

/**
 * \brief Process a curl message
 * \details Adapted from:
 * https://curl.haxx.se/libcurl/c/10-at-a-time.html
 */
static int
curl_process_msgs(CURLMsg *curl_msg, int n_running_curl, int n_mesgs)
{
    int result = 0;
    (void) n_running_curl;
    (void) n_mesgs;
    static volatile int slept = 0;
    if (curl_msg->msg == CURLMSG_DONE) {
        TransferStruct *ts;
        CURL *curl = curl_msg->easy_handle;
        CURLcode ret =
            curl_easy_getinfo(curl_msg->easy_handle, CURLINFO_PRIVATE,
                              &ts);
        if (ret) {
            lprintf(error, "%s", curl_easy_strerror(ret));
        }
        ts->transferring = 0;
        char *url = NULL;
        ret = curl_easy_getinfo(curl, CURLINFO_EFFECTIVE_URL, &url);
        if (ret) {
            lprintf(error, "%s", curl_easy_strerror(ret));
        }

        /*
         * Wait for 5 seconds if we get HTTP 429
         */
        long http_resp = 0;
        ret = curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_resp);
        if (ret) {
            lprintf(error, "%s", curl_easy_strerror(ret));
        }
        if (HTTP_temp_failure(http_resp)) {
            if (!slept) {
                lprintf(warning,
                        "HTTP %ld, sleeping for %d sec\n",
                        http_resp, CONFIG.http_wait_sec);
                sleep(CONFIG.http_wait_sec);
                slept = 1;
            }
        } else {
            slept = 0;
        }

        if (!curl_msg->data.result) {
            /*
             * Transfer successful, set the file size
             */
            if (ts->type == FILESTAT) {
                Link_set_file_stat(ts->link, curl);
            }
        } else {
            lprintf(error, "%d - %s <%s>\n",
                    curl_msg->data.result,
                    curl_easy_strerror(curl_msg->data.result), url);
                    result = curl_msg->data.result;
        }
        curl_multi_remove_handle(curl_multi, curl);
        /*
         * clean up the handle, if we are querying the file size
         */
        if (ts->type == FILESTAT) {
            curl_easy_cleanup(curl);
            FREE(ts);
        }
    } else {
        lprintf(warning, "curl_msg->msg: %d\n", curl_msg->msg);
    }
    return result;
}

static int http_error_result(int http_response)
{
    switch(http_response)
    {
        case 0:   //eg connection down  from kick-off ~suggest retrying till some max limit
        case 200: //yay we at least got to our url
        case 206: //Partial Content
        break;

        case 416:
        //cannot d/l range ~ either cos no server support
        //or cos we're asking for an invalid range ~ie: we already d/ld the file
        printf("HTTP416: either the d/l is already complete or the http server cannot d/l a range\n");
        default:
            return 0;//suggest quitting on an unhandled error
    }

    return 1;
}

static int curl_error_result(int curl_result)
{
    switch (curl_result)
    {
        case CURLE_OK:
        case CURLE_COULDNT_CONNECT:      //no network connectivity ?
        case CURLE_OPERATION_TIMEDOUT:   //cos of CURLOPT_LOW_SPEED_TIME
        case CURLE_COULDNT_RESOLVE_HOST: //host/DNS down ?
            break; //we'll keep trying
        default://see: http://curl.haxx.se/libcurl/c/libcurl-errors.html
            return 0;
    }
    return 1;
}


/**
 * \details  effectively based on
 * https://curl.haxx.se/libcurl/c/multi-double.html
 */
int curl_multi_perform_once(int *result)
{
    lprintf(network_lock_debug,
            "thread %x: locking transfer_lock;\n", pthread_self());
    PTHREAD_MUTEX_LOCK(&transfer_lock);

    /*
     * Get curl multi interface to perform pending tasks
     */
    int n_running_curl;
    CURLMcode mc = curl_multi_perform(curl_multi, &n_running_curl);
    if (mc) {
        lprintf(error, "%s\n", curl_multi_strerror(mc));
    }

    mc = curl_multi_poll(curl_multi, NULL, 0, 100, NULL);
    if (mc) {
        lprintf(error, "%s\n", curl_multi_strerror(mc));
    }

    /*
     * Process the message queue
     */
    int n_mesgs;
    CURLMsg *curl_msg;
    while ((curl_msg = curl_multi_info_read(curl_multi, &n_mesgs))) {
        int nResult = curl_process_msgs(curl_msg, n_running_curl, n_mesgs);
        if (!http_error_result(n_mesgs) || !curl_error_result(nResult)) {
            *result = 1;
        }else{
            *result = 0;
        }
    }

    lprintf(network_lock_debug,
            "thread %x: unlocking transfer_lock;\n", pthread_self());
    PTHREAD_MUTEX_UNLOCK(&transfer_lock);

    return n_running_curl;
}

void NetworkSystem_init(void)
{
    /*
     * ------- Global related ----------
     */
    if (curl_global_init(CURL_GLOBAL_ALL)) {
        lprintf(fatal, "curl_global_init() failed!\n");
    }

    /*
     * -------- Share related ----------
     */
    CURL_SHARE = curl_share_init();
    if (!(CURL_SHARE)) {
        lprintf(fatal, "curl_share_init() failed!\n");
    }

    curl_share_setopt(CURL_SHARE, CURLSHOPT_SHARE, CURL_LOCK_DATA_COOKIE);
    curl_share_setopt(CURL_SHARE, CURLSHOPT_SHARE, CURL_LOCK_DATA_DNS);
    curl_share_setopt(CURL_SHARE, CURLSHOPT_SHARE,
                      CURL_LOCK_DATA_SSL_SESSION);

    if (pthread_mutex_init(&curl_lock, NULL)) {
        lprintf(fatal, "curl_lock initialisation failed!\n");
    }
    curl_share_setopt(CURL_SHARE, CURLSHOPT_LOCKFUNC, curl_callback_lock);
    curl_share_setopt(CURL_SHARE, CURLSHOPT_UNLOCKFUNC,
                      curl_callback_unlock);

    /*
     * ------------- Multi related -----------
     */
    curl_multi = curl_multi_init();
    if (!curl_multi) {
        lprintf(fatal, "curl_multi_init() failed!\n");
    }
    curl_multi_setopt(curl_multi, CURLMOPT_MAX_TOTAL_CONNECTIONS,
                      CONFIG.max_conns);
    curl_multi_setopt(curl_multi, CURLMOPT_MAX_HOST_CONNECTIONS,
                      CONFIG.max_conns);

    /*
     * ------------ Initialise locks ---------
     */
    if (pthread_mutex_init(&transfer_lock, NULL)) {
        lprintf(fatal, "transfer_lock initialisation failed!\n");
    }

    /*
     * cryptographic lock functions were shamelessly copied from
     * https://curl.haxx.se/libcurl/c/threaded-ssl.html
     */
    crypto_lock_init();
}

void transfer_blocking(CURL *curl, size_t start)
{
    TransferStruct *ts;
    CURLcode ret = curl_easy_getinfo(curl, CURLINFO_PRIVATE, &ts);
    if (ret) {
        lprintf(error, "%s", curl_easy_strerror(ret));
    }

    lprintf(network_lock_debug,
            "thread %x: locking transfer_lock;\n", pthread_self());
    PTHREAD_MUTEX_LOCK(&transfer_lock);

    CURLMcode res = curl_multi_add_handle(curl_multi, curl);
    if (res > 0) {
        lprintf(error, "%d, %s\n", res, curl_multi_strerror(res));
    }

    lprintf(network_lock_debug,
            "thread %x: unlocking transfer_lock;\n", pthread_self());
    PTHREAD_MUTEX_UNLOCK(&transfer_lock);

    int result = 0;
    bool restartDown = false;

    while (ts->transferring && !restartDown) {
        /*
        * When the network is abnormal during the file download, start to resume the transfer
        */
        if (0 != result) {
            curl_multi_remove_handle(curl_multi,curl);
            curl_easy_setopt(curl, CURLOPT_RESUME_FROM_LARGE, start);
            res = curl_multi_add_handle(curl_multi, curl);
            if (res > 0) {
                lprintf(error, "%d, %s\n", res, curl_multi_strerror(res));
            }
        }
        curl_multi_perform_once(&result);
    }
}

void transfer_nonblocking(CURL *curl)
{
    lprintf(network_lock_debug,
            "thread %x: locking transfer_lock;\n", pthread_self());
    PTHREAD_MUTEX_LOCK(&transfer_lock);

    CURLMcode res = curl_multi_add_handle(curl_multi, curl);
    if (res > 0) {
        lprintf(error, "%s\n", curl_multi_strerror(res));
    }

    lprintf(network_lock_debug,
            "thread %x: unlocking transfer_lock;\n", pthread_self());
    PTHREAD_MUTEX_UNLOCK(&transfer_lock);
}

int HTTP_temp_failure(HTTPResponseCode http_resp)
{
    switch (http_resp) {
    case HTTP_TOO_MANY_REQUESTS:
    case HTTP_CLOUDFLARE_UNKNOWN_ERROR:
    case HTTP_CLOUDFLARE_TIMEOUT:
        return 1;
    default:
        return 0;
    }
}
