#include <stdio.h>
#include <stdlib.h>
#include <signal.h>
#include <string.h>
#include <strings.h>
#include <sys/time.h>
#include <unistd.h>
#include <librdkafka/rdkafka.h>
#include "ini.h"        /* https://github.com/benhoyt/inih */

/* Kafka producer based on rdfkafka_simple_producer.c from librdkafka */

#define SUCCESS 0
#define ERROR   -1
#define TRUE    1
#define FALSE   0
#define MATCH(s, n) strcasecmp(section, s) == 0 && strcasecmp(name, n) == 0

#define THROUGHPUT_CHECK_INTERVAL_SEC   1
#define MSG_BUFLEN             512
#define THROUGHPUT_BUFLEN      512



typedef enum {
    ThroughputUnit_BYTES = 0,
    ThroughputUnit_KBYTES,
    ThroughputUnit_MBYTES,
    ThroughputUnit_GBYTES
} ThroughputUnit;
static char *throughput_unit_str[4] = {"bytes", "kbytes", "mbytes", "gbytes"};
static int throughput_unit_conversion[4] = {1, 1e3, 1e6, 1e9};

typedef struct {
    char *brokers;              /* CSV list of brokers (host:port) */
    char *topic;
    ThroughputUnit throughput_unit;
    int throughput_interval_sec;
    char *throughput_file;
    char *message_file;
    int message_loop;
} ProducerConfig;
static char *message_loop_str[2] = {"false", "true"};

/* global variables */
static ProducerConfig prod_conf;
static int run = 1;
static long time_start = 0, time_lastcheck = 0;
static long bytes_sent = 0;
static FILE *throughput_fp = NULL;
static int throughput_lineno = 0;
static float throughput_target = 0;
static FILE *message_fp = NULL;


static
void release_config(ProducerConfig *conf) {
    if (conf->brokers)          free(conf->brokers);
    if (conf->topic)            free(conf->topic);
    if (conf->throughput_file)  free(conf->throughput_file);
    if (conf->message_file)     free(conf->message_file);
}

static
void print_config(ProducerConfig *conf) {
    printf("Producer configurations:\n");
    printf("\tbrokers=%s, topic=%s\n", conf->brokers, conf->topic);
    printf("\tthroughput_unit=%s, throughput_file=%s\n",
           throughput_unit_str[conf->throughput_unit], conf->throughput_file);
    printf("\tthroughput_interval_sec=%d\n", conf->throughput_interval_sec);
    printf("\tmessage_file=%s, message_loop=%s\n", conf->message_file, message_loop_str[conf->message_loop]);
}

static
int parse_config(void* user, const char* section, const char* name, const char* value) {
    ProducerConfig *conf = (ProducerConfig *)user;

    if (MATCH("kafka", "brokers"))              
        conf->brokers = strdup(value);
    else if (MATCH("kafka", "topic"))           
        conf->topic = strdup(value);
    else if (MATCH("throughput", "unit")) {
        if (strcasecmp(value, "bytes") == 0 || strcasecmp(value, "b") == 0)
            conf->throughput_unit = ThroughputUnit_BYTES;
        else if (strcasecmp(value, "kbytes") == 0 || strcasecmp(value, "kb") == 0)
            conf->throughput_unit = ThroughputUnit_KBYTES;
        else if (strcasecmp(value, "mbytes") == 0 || strcasecmp(value, "mb") == 0)
            conf->throughput_unit = ThroughputUnit_MBYTES;
        else if (strcasecmp(value, "gbytes") == 0 || strcasecmp(value, "gb") == 0)
            conf->throughput_unit = ThroughputUnit_GBYTES;
        else {
            fprintf(stderr, "%% Illegal unit option: %s\n", optarg);
            goto illegal_config;
        }
    } 
    else if (MATCH("throughput", "file"))
        conf->throughput_file = strdup(value);
    else if (MATCH("throughput", "interval_sec"))
        conf->throughput_interval_sec = atoi(value);
    else if (MATCH("message", "file"))
        conf->message_file = strdup(value);
    else if (MATCH("message", "loop")) {
        if (strcasecmp(value, "true") == 0)
            conf->message_loop = 1;
        else if (strcasecmp(value, "false") == 0)
            conf->message_loop = 0;
        else {
            fprintf(stderr, "%% Illegal message loop option: %s\n", optarg);
            goto illegal_config;
        }
    }
    else 
        goto illegal_config;

    return 1;   /* inih requires to return 1 if no error */

illegal_config:
    return 0;
}

static void stop (int signal) {
    run = 0;
    fprintf(stderr, "%% Stopping producer...");
}

static long get_current_time_msec() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return tv.tv_sec * 1e3 + tv.tv_usec * 1e-3;
}

static float get_throughput_target(long time_curr) {
    /* returns throughput target in bytes/sec */

    char buf[THROUGHPUT_BUFLEN];
    int lineno = (time_curr - time_start) / (1000 * prod_conf.throughput_interval_sec);

    if (throughput_lineno >= lineno)
        return throughput_target;

    while (throughput_lineno < lineno) {
        if (fgets(buf, THROUGHPUT_BUFLEN, throughput_fp) == NULL) {
            fprintf(stderr, "%% Failed to read throughput file\n");
            return -1.0;
        }
        throughput_lineno++;
    }

    /* buf should be NULL-teriminated */
    throughput_target = (float)atof(buf) * throughput_unit_conversion[prod_conf.throughput_unit];

    return throughput_target;
}

static int read_message(char *buf) {
    while (fgets(buf, MSG_BUFLEN, message_fp) == NULL) {
        if (feof(message_fp) && prod_conf.message_loop) {
            fprintf(stderr, "%% Message file reached eof, rewinded to the beginning of the file\n");
            rewind(message_fp);
        }
        else if (ferror(message_fp)) {
            fprintf(stderr, "%% Failed to read throughput file\n");
            return 0;
        }
    }
    return strlen(buf);
}

static 
void msg_callback (rd_kafka_t *rk, const rd_kafka_message_t *rkmessage, void *opaque) {
    long time_curr, time_delta, time_wait;
    float throughput_target, throughput_curr;

    if (rkmessage->err)
        fprintf(stderr, "%% Message delivery failed: %s\n", rd_kafka_err2str(rkmessage->err));
    else {
        bytes_sent += rkmessage->len;
        time_curr = get_current_time_msec();

        if (time_curr - time_lastcheck >= THROUGHPUT_CHECK_INTERVAL_SEC * 1000) {
            time_delta = time_curr - time_lastcheck;
            if ((throughput_target = get_throughput_target(time_curr)) < 0) {
                stop(0);
                return;
            }
            throughput_curr = bytes_sent / time_delta;
            if (throughput_target < throughput_curr) {
                /* Rate control: wait some time to adjust throughput */
                time_wait = bytes_sent / throughput_target - time_delta;
                printf("throughput (target = %.3f, curr = %.3f) bytes/s, time_wait = %ld ms\n", throughput_target, throughput_target, time_wait);
                usleep(time_wait * 1000);
            }
            else {
                /* No chance to control throughput */
                fprintf(stderr, "%% Cannot achieve target throughput (target: %.3f vs. current: %.3f)\n", throughput_target, throughput_curr);
                /* TODO: adjust Kafka producer parameter if possible */
            }

            bytes_sent = 0;
            time_lastcheck = time_curr;
        }
    }

    /* The rkmessage is destroyed automatically by librdkafka */
}


int main (int argc, char** argv) {
    rd_kafka_t *rk = NULL;
    rd_kafka_topic_t *rkt = NULL;
    rd_kafka_conf_t *kafka_conf = NULL;
    char errstr[512];           /* librdkafka API error reporting buffer */
    char msgbuf[MSG_BUFLEN];    /* Message value temporary buffer */
    int retval = EXIT_SUCCESS;

    if (argc < 2) {
        fprintf(stderr, "Usage: %s [init file]\n", argv[0]);
        exit(EXIT_FAILURE);
    }

    /* Signal handler for clean shutdown */
    signal(SIGINT, stop);    

    /* Initialize producer configurations */
    bzero(&prod_conf, sizeof(ProducerConfig));
    if (ini_parse(argv[1], parse_config, &prod_conf) != SUCCESS) {
        fprintf(stderr, "%% Failed parsing %s\n", argv[1]);
        exit(EXIT_FAILURE);
    }
    print_config(&prod_conf);
    time_start = get_current_time_msec();
    
    /* Open throughput and message files */
    if ((throughput_fp = fopen(prod_conf.throughput_file, "r")) == NULL) {
        fprintf(stderr, "%% Failed opening %s\n", prod_conf.throughput_file);
        retval = EXIT_FAILURE;
        goto exit;
    }
    if ((message_fp = fopen(prod_conf.message_file, "r")) == NULL) {
        fprintf(stderr, "%% Failed opening %s\n", prod_conf.message_file);
        retval = EXIT_FAILURE;
        goto exit;
    }

    /* Create Kafka config and setup callback */
    kafka_conf = rd_kafka_conf_new();
    if (rd_kafka_conf_set(kafka_conf, "bootstrap.servers", prod_conf.brokers,
                          errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
        fprintf(stderr, "%% %s\n", errstr);
        retval = EXIT_FAILURE;
        goto exit;
    }
    rd_kafka_conf_set_dr_msg_cb(kafka_conf, msg_callback);
    
    /* Create main Kafka object */
    rk = rd_kafka_new(RD_KAFKA_PRODUCER, kafka_conf, errstr, sizeof(errstr));
    if (!rk) {
        fprintf(stderr, "%% Failed to create new producer: %s\n", errstr);
        retval = EXIT_FAILURE;
        goto exit;
    } 

    /* Create Kafka topic */
    rkt = rd_kafka_topic_new(rk, prod_conf.topic, NULL);
    if (!rkt) {
        fprintf(stderr, "%% Failed to create topic object: %s\n",
                rd_kafka_err2str(rd_kafka_last_error()));
        retval = EXIT_FAILURE;
        goto exit;
    }

    while (run) {
        size_t len = read_message(msgbuf);
        if (len == 0) {
            fprintf(stderr, "%% Finished reading all messages");
            break;
        }

    retry:
        if (rd_kafka_produce(rkt, RD_KAFKA_PARTITION_UA, RD_KAFKA_MSG_F_COPY,
                             msgbuf, len, NULL, 0, NULL) == -1) {
            /* Failed to *enqueue* message for producing */
            fprintf(stderr, "%% Failed to produce to topic %s: %s\n",
                    rd_kafka_topic_name(rkt),
                    rd_kafka_err2str(rd_kafka_last_error()));

            /* Poll to handle delivery reports */
            if (rd_kafka_last_error() == RD_KAFKA_RESP_ERR__QUEUE_FULL) {
                rd_kafka_poll(rk, 1000 /*block for max 1000ms*/);
                goto retry;
            }
        } else {
            rd_kafka_poll(rk, 0 /*non-blocking*/);
        }
    }    

    fprintf(stderr, "%% Flushing final messages..\n");
    rd_kafka_flush(rk, 10*1000 /* wait for max 10 seconds */);

exit:
    if (rkt)
        rd_kafka_topic_destroy(rkt);
    if (rk)
        rd_kafka_destroy(rk);

    release_config(&prod_conf);
    if (throughput_fp)
        fclose(throughput_fp);
    if (message_fp)
        fclose(message_fp);    

    return retval;
}
    
