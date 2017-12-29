#include <getopt.h>
#include <stdio.h>
#include <stdlib.h>
#include <signal.h>
#include <string.h>
#include <strings.h>
#include <unistd.h>
#include <librdkafka/rdkafka.h>

/* Kafka producer based on rdfkafka_simple_producer.c from librdkafka */

#define SUCCESS 0
#define ERROR   -1
#define TRUE    1
#define FALSE   0

#define THROUGHPUT_CHECK_INTERVAL_SEC   1

typedef enum {
    ThroughputUnit_BYTE = 0,
    ThroughputUnit_KBYTE,
    ThroughputUnit_MBYTE,
    ThroughputUnit_GBYTE
} ThroughputUnit;
static char *throughput_unit_str[4] = {"byte", "kbyte", "mbyte", "gbyte"};

typedef enum {
    ThroughputPattern_TWEETS = 0,
    ThroughputPattern_WCUP,
    ThroughputPattern_ADSB
} ThroughputPattern;
static char *throughput_pattern_str[3] = {"tweets", "wcup", "adsb"};

typedef struct {
    char *brokers;
    char *topic;

    ThroughputUnit throughput_unit;
    ThroughputPattern throughput_pattern;
    int throughput_interval; /* [sec] */
    char *throughput_file;
    char *data_file;
    int loop;
} ProducerConfig;

static char *loop_str[2] = {"false", "true"};


/* global variables */
static int run = 1;
static long time_lastcheck = 0;
static long bytes_sent = 0;
static float *target_throughput = NULL;
static int target_throughput_size = 0;

static
void init_conf(ProducerConfig *conf) {
    conf->throughput_unit = ThroughputUnit_KBYTE;
    conf->throughput_pattern = ThroughputPattern_TWEETS;
    conf->throughput_file = "./throughput/tweets01";
    conf->throughput_interval = 5; 
    conf->data_file = "./data/tweets-500m";
    conf->loop = TRUE;
    conf->throughput_interval = 5;
}

static
void print_conf(ProducerConfig *conf) {
    printf("Producer configurations:\n");
    printf("\tthroughput_unit=%s, throughput_pattern=%s\n",
           throughput_unit_str[conf->throughput_unit], throughput_pattern_str[conf->throughput_pattern]);
    printf("\tthroughput_file=%s, throughput_interval=%ds\n",
           conf->throughput_file, conf->throughput_interval);
    printf("\tdata_file=%s, loop=%s\n", conf->data_file, loop_str[conf->loop]);
}

static
int parse_conf(int argc, char** argv, ProducerConfig *conf) {
    int opt;
    char *optarg;

    while ((opt = getopt(argc, argv, "ulidpf:")) != -1) {
        optarg = argv[optind];
        switch (opt) {
        case 'u':
            if (strcmp(optarg, "byte") == 0 || strcmp(optarg, "b") == 0)
                conf->throughput_unit = ThroughputUnit_BYTE;
            else if (strcmp(optarg, "kbyte") == 0 || strcmp(optarg, "kb") == 0)
                conf->throughput_unit = ThroughputUnit_KBYTE;
            else if (strcmp(optarg, "mbyte") == 0 || strcmp(optarg, "mb") == 0)
                conf->throughput_unit = ThroughputUnit_MBYTE;
            else if (strcmp(optarg, "gbyte") == 0 || strcmp(optarg, "gb") == 0)
                conf->throughput_unit = ThroughputUnit_GBYTE;
            else {
                fprintf(stderr, "Illegal unit option: %s\n", optarg);
                goto illegal_option;
            }
            break;

        case 'l':
            if (strcasecmp(optarg, "true") == 0)
                conf->loop = 0;
            else if (strcasecmp(optarg, "false") == 0)
                conf->loop = 1;
            else {
                fprintf(stderr, "Illegal loop option: %s\n", optarg);
                goto illegal_option;
            }
            break;
            
        case 'i':
            conf->throughput_interval = atoi(optarg);
            if (conf->throughput_interval <= 2) {
                fprintf(stderr, "Interval too small: %ds\n", conf->throughput_interval);
                goto illegal_option;
            }
            break;

        case 'd':
            conf->data_file = optarg;
            break;
            
        case 'p':
            if (strcmp(optarg, "tweets") == 0)
                conf->throughput_pattern = ThroughputPattern_TWEETS;
            else if (strcmp(optarg, "wcup") == 0)
                conf->throughput_pattern = ThroughputPattern_WCUP;
            else if (strcmp(optarg, "adsb") == 0)
                conf->throughput_pattern = ThroughputPattern_ADSB;
            else {
                fprintf(stderr, "Illegal throughput pattern option: %s\n", optarg);
                goto illegal_option;
            }
            break;

        case 'f':
            conf->throughput_file = optarg;
            break;

        case '?':
        default:
            goto illegal_option;
        }
    }

    return SUCCESS;

illegal_option:
    fprintf(stderr, "Usage: %s [-u (b)yte|(kb)yte|(mb)yte|(gb)yte] [-l true|false] \n", argv[0]);
    fprintf(stderr, "\t\t [-i throughput_interval (seconds)]  [-d data_file]\n");
    fprintf(stderr, "\t\t [-t tweets/wcup/adsb]  [-f file]\n");

    return ERROR;
}

static void stop (int signal) {
    run = 0;
}

static long get_current_time_msec() {
}

static float get_target_throughput(long time_now) {
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
            throughput_target = get_throuput_target(time_curr);
            throughput_curr = bytes_sent / time_delta;
            if (throughput_target < throughput_curr) {
                /* Rate control: wait some time to adjust throughput */
                time_wait = bytes_sent / throughput_target - time_delta;
                usleep(time_wait * 1000);
            }
            else {
                /* No chance to control throughput */
                fprintf(stderr, "%% WARNING: Cannot achieve target throughput (target: %.3f vs. current: %.3f)\n", throughput_target, throughput_curr);
                /* TODO: adjust Kafka producer parameter if possible */
            }

            bytes_sent = 0;
            time_lastcheck = time_curr;
        }
    }

    /* The rkmessage is destroyed automatically by librdkafka */
}


int main (int argc, char** argv) {
    rd_kafka_t *rk;             /* Producer instance handle */
    rd_kafka_topic_t *rkt;      /* Topic object */
    rd_kafka_conf_t *kafka_conf;  /* Temporary configuration object */
    char errstr[512];           /* librdkafka API error reporting buffer */
    char buf[512];              /* Message value temporary buffer */
    char *brokers;              /* broker list */
    char *topic;                /* topic to produce to */

    ProducerConfig prod_conf;

    init_conf(&prod_conf);
    if (parse_conf(argc, argv, &prod_conf) != SUCCESS)
        exit(EXIT_FAILURE);
    print_conf(&prod_conf);

    brokers = prod_conf.brokers;
    topic = prod_conf.topic;

    kafka_conf = rd_kafka_conf_new();
    if (rd_kafka_conf_set(kafka_conf, "bootstrap.servers", brokers,
                          errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
        fprintf(stderr, "%s\n", errstr);
        return EXIT_FAILURE;
    }
    rd_kafka_conf_set_dr_msg_cb(kafka_conf, msg_callback);
    
    rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
    if (!rk) {
        fprintf(stderr, "%% Failed to create new producer: %s\n", errstr);
        return EXIT_FAILURE;
    } 

    rkt = rd_kafka_topic_new(rk, topic, NULL);
    if (!rkt) {
        fprintf(stderr, "%% Failed to create topic object: %s\n",
                rd_kafka_err2str(rd_kafka_last_error()));
        rd_kafka_destroy(rk);
        return EXIT_FAILURE;
    }

    /* Signal handler for clean shutdown */
    signal(SIGINT, stop);    

    while (run) {
        size_t len = read_data(buf);
    retry:
        if (rd_kafka_produce(rkt, RD_KAFKA_PARTITION_UA, RD_KAFKA_MSG_F_COPY,
                             buf, len, NULL, 0, NULL) == -1) {
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

    rd_kafka_topic_destroy(rkt);
    rd_kafka_destroy(rk);

    return EXIT_SUCCESS;
}
    
