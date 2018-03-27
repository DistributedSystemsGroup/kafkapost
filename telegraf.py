import datetime
import json
import logging
from timeit import default_timer as timer

import psycopg2
import psycopg2.sql

log = logging.getLogger(__name__)


measurement_metadata = {
    'net': {
        'known': {
            'host',
            'interface',
            'err_out',
            'err_in',
            'bytes_recv',
            'bytes_sent',
            'drop_out',
            'drop_in',
            'packets_sent',
            'packets_recv',
            'ip_forwarding',
            'ip_reasmtimeout',
            'ip_defaultttl',
            'ip_forwdatagrams',
            'ip_fragfails',
            'ip_outrequests',
            'ip_reasmoks',
            'ip_inhdrerrors',
            'ip_inaddrerrors',
            'ip_inreceives',
            'ip_reasmfails',
            'ip_indelivers',
            'ip_outnoroutes',
            'ip_indiscards',
            'ip_inunknownprotos',
            'ip_reasmreqds',
            'ip_fragoks',
            'ip_fragcreates',
            'ip_outdiscards',
            'icmp_outechos',
            'icmp_outdestunreachs',
            'icmp_outtimestampreps',
            'icmp_outsrcquenchs',
            'icmp_outechoreps',
            'icmp_outerrors',
            'icmp_outredirects',
            'icmp_outparmprobs',
            'icmp_outtimestamps',
            'icmp_outaddrmasks',
            'icmp_outtimeexcds',
            'icmp_outaddrmaskreps',
            'icmp_outmsgs',
            'icmp_intimestampreps',
            'icmp_indestunreachs',
            'icmp_inredirects',
            'icmp_inechos',
            'icmp_inerrors',
            'icmp_intimestamps',
            'icmp_inparmprobs',
            'icmp_intimeexcds',
            'icmp_inechoreps',
            'icmp_insrcquenchs',
            'icmp_incsumerrors',
            'icmp_inmsgs',
            'icmp_inaddrmaskreps',
            'icmp_inaddrmasks',
            'icmp_indestunreachs',
            'icmpmsg_intype0',
            'icmpmsg_intype3',
            'icmpmsg_intype8',
            'icmpmsg_outtype0',
            'icmpmsg_outtype3',
            'icmpmsg_outtype5',
            'icmpmsg_outtype8',
            'icmpmsg_outtype11',
            'icmpmsg_intype11',
            'tcp_outrsts',
            'tcp_rtomin',
            'tcp_currestab',
            'tcp_retranssegs',
            'tcp_outsegs',
            'tcp_insegs',
            'tcp_attemptfails',
            'tcp_rtoalgorithm',
            'tcp_activeopens',
            'tcp_rtomax',
            'tcp_incsumerrors',
            'tcp_estabresets',
            'tcp_passiveopens',
            'tcp_maxconn',
            'tcp_inerrs',
            'udplite_sndbuferrors',
            'udplite_incsumerrors',
            'udplite_inerrors',
            'udplite_noports',
            'udplite_ignoredmulti',
            'udplite_indatagrams',
            'udplite_rcvbuferrors',
            'udplite_outdatagrams',
            'udp_noports',
            'udp_ignoredmulti',
            'udp_incsumerrors',
            'udp_indatagrams',
            'udp_rcvbuferrors',
            'udp_sndbuferrors',
            'udp_inerrors',
            'udp_outdatagrams'
        },
        'discard': {
            'ip_forwarding',
            'ip_reasmtimeout',
            'ip_defaultttl',
            'ip_forwdatagrams',
            'ip_fragfails',
            'ip_outrequests',
            'ip_reasmoks',
            'ip_inhdrerrors',
            'ip_inaddrerrors',
            'ip_inreceives',
            'ip_reasmfails',
            'ip_indelivers',
            'ip_outnoroutes',
            'ip_indiscards',
            'ip_inunknownprotos',
            'ip_reasmreqds',
            'ip_fragoks',
            'ip_fragcreates',
            'ip_outdiscards',
            'icmp_intimestampreps',
            'icmp_outechos',
            'icmp_outdestunreachs',
            'icmp_outtimestampreps',
            'icmp_outsrcquenchs',
            'icmp_inredirects',
            'icmp_inechos',
            'icmp_inerrors',
            'icmp_intimestamps',
            'icmp_outechoreps',
            'icmp_inparmprobs',
            'icmp_outerrors',
            'icmp_outredirects',
            'icmp_intimeexcds',
            'icmp_outparmprobs',
            'icmp_inechoreps',
            'icmp_outtimestamps',
            'icmp_outaddrmasks',
            'icmp_insrcquenchs',
            'icmp_incsumerrors',
            'icmp_outtimeexcds',
            'icmp_inmsgs',
            'icmp_outaddrmaskreps',
            'icmp_inaddrmaskreps',
            'icmp_inaddrmasks',
            'icmp_outmsgs',
            'icmp_indestunreachs',
            'icmpmsg_outtype0',
            'icmpmsg_intype3',
            'icmpmsg_intype8',
            'icmpmsg_outtype8',
            'icmpmsg_intype0',
            'icmpmsg_outtype3',
            'icmpmsg_outtype5',
            'icmpmsg_outtype11',
            'icmpmsg_intype11',
            'tcp_outrsts',
            'tcp_rtomin',
            'tcp_currestab',
            'tcp_retranssegs',
            'tcp_outsegs',
            'tcp_insegs',
            'tcp_attemptfails',
            'tcp_rtoalgorithm',
            'tcp_activeopens',
            'tcp_rtomax',
            'tcp_incsumerrors',
            'tcp_estabresets',
            'tcp_passiveopens',
            'tcp_maxconn',
            'tcp_inerrs',
            'udplite_sndbuferrors',
            'udplite_incsumerrors',
            'udplite_inerrors',
            'udplite_noports',
            'udplite_ignoredmulti',
            'udplite_indatagrams',
            'udplite_rcvbuferrors',
            'udplite_outdatagrams',
            'udp_noports',
            'udp_ignoredmulti',
            'udp_incsumerrors',
            'udp_indatagrams',
            'udp_rcvbuferrors',
            'udp_sndbuferrors',
            'udp_inerrors',
            'udp_outdatagrams'
        }
    },
    'cpu': {
        'known': {
            'cpu',
            'host',
            'usage_user',
            'usage_steal',
            'usage_iowait',
            'usage_softirq',
            'usage_system',
            'usage_guest_nice',
            'usage_guest',
            'usage_idle',
            'usage_irq',
            'usage_nice'
        },
        'discard': set()
    },
    'diskio': {
        'known': {
            'host',
            'name',
            'reads',
            'write_time',
            'write_bytes',
            'writes',
            'read_time',
            'read_bytes',
            'io_time',
            'weighted_io_time',
            'iops_in_progress'
        },
        'discard': set()
    },
    'netstat': {
        'known': {
            'host',
            'tcp_close',
            'udp_socket',
            'tcp_syn_sent',
            'tcp_closing',
            'tcp_time_wait',
            'tcp_fin_wait1',
            'tcp_fin_wait2',
            'tcp_close_wait',
            'tcp_established',
            'tcp_listen',
            'tcp_syn_recv',
            'tcp_last_ack',
            'tcp_none'
        },
        'discard': set()
    },
    'mem': {
        'known': {
            'host',
            'total',
            'inactive',
            'active',
            'free',
            'available',
            'available_percent',
            'used_percent',
            'buffered',
            'cached',
            'used',
            'slab'
        },
        'discard': {'slab'}
    },
    'swap': {
        'known': {
            'host',
            'used',
            'used_percent',
            'free',
            'total',
            'out',
            'in'
        },
        'discard': {
            'used_percent',
            'out',
            'in'
        }
    },
    'docker': {
        'known': {
            'host',
            'engine_host',
            'n_goroutines',
            'n_containers',
            'n_cpus',
            'n_containers_running',
            'n_containers_paused',
            'n_containers_stopped',
            'n_used_file_descriptors',
            'n_images',
            'n_listener_events',
            'memory_total',
            'unit'
        },
        'discard': {
            'engine_host',
            'n_cpus',
            'n_goroutines',
            'n_used_file_descriptors',
            'n_listener_events',
            'memory_total',
            'unit'
        }
    },
    'docker_container_cpu': {
        'known': {
            'container_id',
            'usage_total',
            'engine_host',
            'cpu',
            'author',
            'maintainer',
            'container_version',
            'container_image',
            'zoe_deployment_name',
            'host',
            'zoe_execution_name',
            'container_name',
            'zoe_execution_id',
            'zoe_owner',
            'zoe_service_id',
            'zoe_service_name',
            'zoe_type',
            'com.docker.swarm.id',
            'usage_in_kernelmode',
            'throttling_periods',
            'usage_in_usermode',
            'usage_percent',
            'throttling_throttled_time',
            'usage_system',
            'throttling_throttled_periods'
        },
        'discard': {
            'container_id',
            'engine_host',
            'author',
            'maintainer',
            'zoe_type',
            'com.docker.swarm.id',
            'usage_in_kernelmode',
            'throttling_periods',
            'usage_in_usermode',
            'throttling_throttled_time',
            'usage_system',
            'throttling_throttled_periods'
        }
    },
    'docker_container_net': {
        'known': {
            'container_image',
            'zoe_type',
            'engine_host',
            'zoe_execution_id',
            'zoe_owner',
            'zoe_service_name',
            'zoe_service_id',
            'zoe_deployment_name',
            'maintainer',
            'author',
            'container_name',
            'network',
            'host',
            'zoe_execution_name',
            'container_version',
            'tx_errors',
            'tx_packets',
            'rx_dropped',
            'rx_errors',
            'container_id',
            'tx_bytes',
            'rx_packets',
            'rx_bytes',
            'tx_dropped',
            'com.docker.swarm.id'
        },
        'discard': {
            'container_id',
            'engine_host',
            'author',
            'maintainer',
            'zoe_type',
            'com.docker.swarm.id'
        }
    },
    'docker_container_blkio': {
        'known': {
            'zoe_deployment_name',
            'zoe_execution_name',
            'container_image',
            'container_version',
            'device',
            'maintainer',
            'author',
            'zoe_type',
            'zoe_service_id',
            'zoe_owner',
            'host',
            'container_name',
            'engine_host',
            'zoe_execution_id',
            'zoe_service_name',
            'io_serviced_recursive_write',
            'io_serviced_recursive_total',
            'io_service_bytes_recursive_read',
            'io_service_bytes_recursive_sync',
            'io_service_bytes_recursive_write',
            'io_serviced_recursive_read',
            'io_service_bytes_recursive_async',
            'io_serviced_recursive_async',
            'container_id',
            'io_serviced_recursive_sync',
            'io_service_bytes_recursive_total',
            'com.docker.swarm.id'
        },
        'discard': {
            'author',
            'maintainer',
            'zoe_type',
            'engine_host',
            'container_id',
            'com.docker.swarm.id'
        }
    },
    'docker_container_mem': {
        'known': {
            'active_file',
            'active_anon',
            'author',
            'maintainer',
            'cache',
            'host',
            'engine_host',
            'container_version',
            'container_image',
            'container_name',
            'container_id',
            'inactive_anon',
            'limit',
            'max_usage',
            'mapped_file',
            'pgpgin',
            'pgfault',
            'total_rss',
            'total_pgpgin',
            'total_cache',
            'total_active_file',
            'total_pgmajfault',
            'total_writeback',
            'total_active_anon',
            'total_rss_huge',
            'total_inactive_anon',
            'total_pgfault',
            'total_pgpgout',
            'total_inactive_file',
            'total_unevictable',
            'total_mapped_file',
            'usage_percent',
            'zoe_type',
            'zoe_service_id',
            'zoe_execution_id',
            'zoe_service_name',
            'zoe_deployment_name',
            'zoe_owner',
            'zoe_execution_name',
            'inactive_file',
            'rss',
            'pgmajfault',
            'rss_huge',
            'pgpgout',
            'usage',
            'hierarchical_memory_limit',
            'writeback',
            'unevictable',
            'com.docker.swarm.id'
        },
        'discard': {
            'zoe_type',
            'author',
            'maintainer',
            'engine_host',
            'inactive_anon',
            'total_pgpgin',
            'pgpgin',
            'cache',
            'active_file',
            'pgfault',
            'container_id',
            'max_usage',
            'mapped_file',
            'limit',
            'total_rss_huge',
            'active_anon',
            'inactive_file',
            'rss',
            'pgmajfault',
            'rss_huge',
            'pgpgout',
            'total_unevictable',
            'total_mapped_file',
            'total_active_file',
            'total_pgmajfault',
            'total_writeback',
            'total_active_anon',
            'total_inactive_anon',
            'total_pgpgout',
            'total_inactive_file',
            'writeback',
            'unevictable',
            'com.docker.swarm.id'
        }
    },
    'ping': {
        'known': {
            'average_response_ms',
            'minimum_response_ms',
            'result_code',
            'maximum_response_ms',
            'packets_transmitted',
            'packets_received',
            'percent_packet_loss',
            'url',
            'host'
        },
        'discard': set()
    }
}
conn = psycopg2.connect(host="bf11", dbname="metrics", user="postgres", password="zoepostgres")
for m in measurement_metadata:
    measurement_metadata[m]['keep'] = measurement_metadata[m]['known'] - measurement_metadata[m]['discard']

counters = {
    'inserts': 0,
    'errors': 0,
    'total_lag': 0,
    'insert_times': [],
    'msg_in': 0
}


def unpack_telegraf_json_protocol(message):
    timestamp = datetime.datetime.fromtimestamp(message['timestamp'] / 1000, datetime.timezone.utc)
    measurement = message['name']

    values = {}
    for k, v in message['tags'].items():
        v = v.replace(':', '_')
        v = v.replace('==', '-eq-')
        v = v.replace('=', '-')
        v = v.replace(' ', '_')
        values[k] = v

    for k, v in message['fields'].items():
        values[k] = v

    return measurement, timestamp, values


def router(message, kafka_producer):
    counters['msg_in'] += 1
    try:
        measurement, timestamp, values = unpack_telegraf_json_protocol(message)
    except Exception as e:
        log.error(e)
        counters['errors'] += 1
        return True

    if measurement not in measurement_metadata:
        log.warning('Skipping unknown measurement: {}'.format(message))
        counters['errors'] += 1
        return False

    values_names = set(values.keys())
    if message_filter(measurement, values_names):
        return True

    values_to_insert = values_names & measurement_metadata[measurement]['keep']
    if values_to_insert != measurement_metadata[measurement]['keep']:
        error_message = {
            'measurement': measurement,
            'timestamp': timestamp.timestamp(),
            'tags': values,
            'unknown_tags': list(values_names - measurement_metadata[measurement]['known']),
            'missing_tags': list(measurement_metadata[measurement]['keep'] - values_names)
        }
        kafka_producer.produce('metrics_errors', json.dumps(error_message))
        counters['errors'] += 1
        if len(error_message['unknown_tags']) > 0:
            log.error('Error: {}'.format(error_message))
        return True

    values = {k: v for k, v in values.items() if k in values_to_insert}
    return insert(measurement, timestamp, values)


def insert(table, timestamp, values):
    values["timestamp"] = timestamp
    time_start = timer()
    q = psycopg2.sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
        psycopg2.sql.Identifier(table),
        psycopg2.sql.SQL(', ').join(map(psycopg2.sql.Identifier, values.keys())),
        psycopg2.sql.SQL(', ').join(map(psycopg2.sql.Placeholder, values.keys())))
    with conn:
        with conn.cursor() as cur:
            try:
                cur.execute(q, values)
                counters['inserts'] += 1
            except (psycopg2.OperationalError, psycopg2.ProgrammingError) as e:
                log.error(e)
                counters['errors'] += 1
                return False
    counters['insert_times'].append(timer() - time_start)
    return True


def flush_stats(time_interval):
    if time_interval == 0:
        return
    timestamp = datetime.datetime.now(datetime.timezone.utc)
    q = 'INSERT INTO kafkapost (timestamp, host, kafka_lag, messages_in_sec, inserts_sec, parse_errors_sec, avg_insert_time) VALUES (%s, %s, %s, %s, %s, %s, %s)'
    if len(counters['insert_times']) == 0:
        avg_insert_time = 0
    else:
        avg_insert_time = sum(counters['insert_times']) / len(counters['insert_times'])
    with conn:
        with conn.cursor() as cur:
            cur.execute(q, (timestamp, 'bf11', counters['total_lag'], counters['msg_in'] / time_interval, counters['inserts'] / time_interval, counters['errors'] / time_interval, avg_insert_time))
    counters['inserts'] = 0
    counters['errors'] = 0
    counters['total_lag'] = 0
    counters['insert_times'] = []
    counters['msg_in'] = 0


def message_filter(measurement, values_names):
    if measurement == 'swap' and 'in' in values_names and 'out' in values_names:
        return True
    else:
        return False

#######################
# CREATE TABLE diskio
# (
#   timestamp TIMESTAMP WITH TIME ZONE,
#   host TEXT,
#   name TEXT,
#   reads BIGINT,
#   write_time BIGINT,
#   write_bytes BIGINT,
#   writes BIGINT,
#   read_time BIGINT,
#   read_bytes BIGINT,
#   io_time BIGINT,
#   weighted_io_time BIGINT,
#   iops_in_progress BIGINT
#  )
#
# CREATE TABLE docker_container_cpu
# (
#   timestamp TIMESTAMP WITH TIME ZONE,
#   host TEXT,
#   usage_total BIGINT,
#   cpu TEXT,
#   container_version TEXT,
#   container_image TEXT,
#   zoe_deployment_name TEXT NULL DEFAULT NULL,
#   zoe_execution_name TEXT NULL DEFAULT NULL,
#   container_name TEXT,
#   zoe_execution_id TEXT NULL DEFAULT NULL,
#   zoe_owner TEXT NULL DEFAULT NULL,
#   zoe_service_id TEXT NULL DEFAULT NULL,
#   zoe_service_name TEXT NULL DEFAULT NULL
# )
#
# CREATE TABLE docker_container_net
# (
#   timestamp TIMESTAMP WITH TIME ZONE,
#   host TEXT,
#   container_image TEXT,
#   zoe_execution_id TEXT NULL DEFAULT NULL,
#   zoe_owner TEXT NULL DEFAULT NULL,
#   zoe_service_name TEXT NULL DEFAULT NULL,
#   zoe_service_id TEXT NULL DEFAULT NULL,
#   zoe_deployment_name TEXT NULL DEFAULT NULL,
#   container_name TEXT NULL DEFAULT NULL,
#   network TEXT,
#   zoe_execution_name TEXT NULL DEFAULT NULL,
#   container_version TEXT NULL DEFAULT NULL,
#   tx_errors BIGINT,
#   tx_packets BIGINT,
#   rx_dropped BIGINT,
#   rx_errors BIGINT,
#   tx_bytes BIGINT,
#   rx_packets BIGINT,
#   rx_bytes BIGINT,
#   tx_dropped BIGINT
# )
#
# CREATE TABLE docker_container_blkio
# (
#   timestamp TIMESTAMP WITH TIME ZONE,
#   host TEXT,
#   container_image TEXT,
#   zoe_execution_id TEXT NULL DEFAULT NULL,
#   zoe_owner TEXT NULL DEFAULT NULL,
#   zoe_service_name TEXT NULL DEFAULT NULL,
#   zoe_service_id TEXT NULL DEFAULT NULL,
#   zoe_deployment_name TEXT NULL DEFAULT NULL,
#   container_name TEXT NULL DEFAULT NULL,
#   network TEXT,
#   zoe_execution_name TEXT NULL DEFAULT NULL,
#   container_version TEXT NULL DEFAULT NULL,
#   device TEXT,
#   io_serviced_recursive_write BIGINT,
#   io_serviced_recursive_total BIGINT,
#   io_service_bytes_recursive_read BIGINT,
#   io_service_bytes_recursive_sync BIGINT,
#   io_service_bytes_recursive_write BIGINT,
#   io_serviced_recursive_read BIGINT,
#   io_service_bytes_recursive_async BIGINT,
#   io_serviced_recursive_async BIGINT,
#   io_serviced_recursive_sync BIGINT,
#   io_service_bytes_recursive_total BIGINT
# )
#
# CREATE TABLE docker_container_mem
# (
#   timestamp TIMESTAMP WITH TIME ZONE,
#   host TEXT,
#   container_image TEXT,
#   container_version TEXT NULL DEFAULT NULL,
#   container_name TEXT NULL DEFAULT NULL,
#   zoe_execution_id TEXT NULL DEFAULT NULL,
#   zoe_owner TEXT NULL DEFAULT NULL,
#   zoe_service_name TEXT NULL DEFAULT NULL,
#   zoe_service_id TEXT NULL DEFAULT NULL,
#   zoe_deployment_name TEXT NULL DEFAULT NULL,
#   zoe_execution_name TEXT NULL DEFAULT NULL,
#   total_rss BIGINT,
#   total_cache BIGINT,
#   total_pgfault BIGINT,
#   usage_percent FLOAT,
#   usage BIGINT,
#   hierarchical_memory_limit BIGINT
# )
#
# CREATE TABLE ping
# (
#   timestamp TIMESTAMP WITH TIME ZONE,
#   host TEXT,
#   average_response_ms FLOAT,
#   minimum_response_ms FLOAT,
#   result_code INT,
#   maximum_response_ms FLOAT,
#   packets_transmitted INT,
#   packets_received INT,
#   percent_packet_loss FLOAT,
#   url TEXT
# )
