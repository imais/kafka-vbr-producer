#include <getopt.h>
#include <stdio.h>
#include <stdlib.h>
#include <signal.h>
#include <string.h>
#include <strings.h>
#include <unistd.h>
#include <librdkafka/rdkafka.h>

#define SUCCESS 0
#define ERROR   -1
#define TRUE    1
#define FALSE   0

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
    ThroughputUnit throughput_unit;
    ThroughputPattern throughput_pattern;
    int throughput_interval; /* [sec] */
    char *throughput_file;
    char *data_file;
    int loop;
} Config;

static char *loop_str[2] = {"false", "true"};


void init_conf(Config *conf) {
    conf->throughput_unit = ThroughputUnit_BYTE;
    conf->throughput_pattern = ThroughputPattern_TWEETS;
    conf->throughput_file = "./throughput/tweets";
    conf->throughput_interval = 5; 
    conf->data_file = "./data/tweets";
    conf->loop = TRUE;
    conf->throughput_interval = 5;
}


void print_conf(Config *conf) {
    printf("Configurations:\n");
    printf("\tthroughput_unit=%s, throughput_pattern=%s\n",
           throughput_unit_str[conf->throughput_unit], throughput_pattern_str[conf->throughput_pattern]);
    printf("\tthroughput_file=%s, throughput_interval=%ds\n",
           conf->throughput_file, conf->throughput_interval);
    printf("\tdata_file=%s, loop=%s\n", conf->data_file, loop_str[conf->loop]);
}


int parse_conf(int argc, char** argv, Config *conf) {
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




int main (int argc, char** argv) {
    Config conf;

    init_conf(&conf);
    if (parse_conf(argc, argv, &conf) != SUCCESS)
        exit(EXIT_FAILURE);
    print_conf(&conf);

    return EXIT_SUCCESS;
}
    
