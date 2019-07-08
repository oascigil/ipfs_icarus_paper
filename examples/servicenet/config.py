# -*- coding: utf-8 -*-
"""This module contains all configuration information used to run simulations
"""
from multiprocessing import cpu_count
from collections import deque
import copy
from math import pow
from icarus.util import Tree

# GENERAL SETTINGS

# Level of logging output
# Available options: DEBUG, INFO, WARNING, ERROR, CRITICAL
LOG_LEVEL = 'INFO'

# If True, executes simulations in parallel using multiple processes
# to take advantage of multicore CPUs
PARALLEL_EXECUTION = True

# Number of processes used to run simulations in parallel.
# This option is ignored if PARALLEL_EXECUTION = False
N_PROCESSES = 20 #cpu_count()

# Granularity of caching.
# Currently, only OBJECT is supported
CACHING_GRANULARITY = 'OBJECT'

# Warm-up strategy
#WARMUP_STRATEGY = 'MFU' #'HYBRID'
WARMUP_STRATEGY = 'NDN' #'OPTIMAL' #'HYBRID'

# Format in which results are saved.
# Result readers and writers are located in module ./icarus/results/readwrite.py
# Currently only PICKLE is supported 
RESULTS_FORMAT = 'PICKLE'

# Number of times each experiment is replicated
# This is necessary for extracting confidence interval of selected metrics
N_REPLICATIONS = 1

# List of metrics to be measured in the experiments
# The implementation of data collectors are located in ./icaurs/execution/collectors.py
DATA_COLLECTORS = ['VIDEO_PLAYBACK']

# Range of alpha values of the Zipf distribution using to generate content requests
# alpha values must be positive. The greater the value the more skewed is the 
# content popularity distribution
# Range of alpha values of the Zipf distribution using to generate content requests
# alpha values must be positive. The greater the value the more skewed is the 
# content popularity distribution
# Note: to generate these alpha values, numpy.arange could also be used, but it
# is not recommended because generated numbers may be not those desired. 
# E.g. arange may return 0.799999999999 instead of 0.8. 
# This would give problems while trying to plot the results because if for
# example I wanted to filter experiment with alpha=0.8, experiments with
# alpha = 0.799999999999 would not be recognized 
ALPHA = 0.75 #0.75
#ALPHA = [0.00001]

# Total size of network cache as a fraction of content population
NETWORK_CACHE = 0.05

# Number of content objects
#N_CONTENTS = 1000
N_CONTENTS = 100
N_CHUNKS = 200 #1000

VIDEO_PLAYBACK_RATE = 10
BUFFER_SIZE = VIDEO_PLAYBACK_RATE

N_SERVICES = 1

# Number of requests per second (over the whole network)
NETWORK_REQUEST_RATE = 50.0

# Number of cores for each node in the experiment
NUM_CORES = 1

# Number of content requests generated to prepopulate the caches
# These requests are not logged
N_WARMUP_REQUESTS = 0 #30000

# Number of content requests generated after the warmup and logged
# to generate results. 
#N_MEASURED_REQUESTS = 1000 #60*30000 #100000

SECS = 60 #do not change
MINS = 5.0 #10.0 #5.5
N_MEASURED_REQUESTS = NETWORK_REQUEST_RATE*SECS*MINS

# List of all implemented topologies
# Topology implementations are located in ./icarus/scenarios/topology.py
TOPOLOGIES =  ['TISCALI']
TREE_DEPTH = 3
BRANCH_FACTOR = 2
NUM_NODES = int(pow(BRANCH_FACTOR, TREE_DEPTH+1) -1) 

# Replacement Interval in seconds
REPLACEMENT_INTERVAL = 30.0
NUM_REPLACEMENTS = 10000

# List of caching and routing strategies
# The code is located in ./icarus/models/strategy.py
STRATEGIES = ['OPTIMAL', 'NDN', 'NDN_MEASUREMENT', 'IPFS']  # service-based routing
#STRATEGIES = ['NDN_MEASUREMENT']  # service-based routing
#STRATEGIES = ['IPFS']  # service-based routing

# Cache replacement policy used by the network caches.
# Supported policies are: 'LRU', 'LFU', 'FIFO', 'RAND' and 'NULL'
# Cache policy implmentations are located in ./icarus/models/cache.py
CACHE_POLICY = 'LRU'

# Task scheduling policy used by the cloudlets.
# Supported policies are: 'EDF' (Earliest Deadline First), 'FIFO'
SCHED_POLICY = 'EDF'

# Queue of experiments
EXPERIMENT_QUEUE = deque()
default = Tree()

default['workload'] = {'name':       'STATIONARY',
                       'n_contents': N_CONTENTS,
                       'n_chunks' : N_CHUNKS,
                       'playback_rate' : VIDEO_PLAYBACK_RATE,
                       'wndw_size' : BUFFER_SIZE,
                       'n_warmup':   N_WARMUP_REQUESTS,
                       'n_measured': N_MEASURED_REQUESTS,
                       'rate':       NETWORK_REQUEST_RATE,
                       'seed':  0,
                       'n_services': N_SERVICES,
                       'alpha' : ALPHA
                      }
#default['computation_placement']['name'] = 'CENTRALITY'
default['cache_placement']['name'] = 'EDGE'
default['cache_placement']['cache_budget'] = 2*N_CHUNKS
default['computation_placement']['name'] = 'EDGE'
default['computation_placement']['service_budget'] = NUM_CORES #NUM_CORES*NUM_NODES*3 #   N_SERVICES/2 #N_SERVICES/2 
default['computation_placement']['computation_budget'] = NUM_CORES  # NUM_CORES for each node 
default['cache_placement']['network_cache'] = 1.0
default['content_placement']['name'] = 'UNIFORM'
default['content_placement']['n_chunks'] = N_CHUNKS
default['cache_policy']['name'] = CACHE_POLICY
default['sched_policy']['name'] = SCHED_POLICY
default['strategy']['n_chunks'] = N_CHUNKS
default['strategy']['playback_rate'] = VIDEO_PLAYBACK_RATE
default['topology']['name'] = 'TISCALI'
default['topology']['nrecv_per_edge'] = 10
default['warmup_strategy']['name'] = WARMUP_STRATEGY
default['warmup_strategy']['n_chunks'] = N_CHUNKS
default['warmup_strategy']['playback_rate'] = VIDEO_PLAYBACK_RATE

# Create experiments multiplexing all desired parameters
"""
for strategy in ['LRU']: # STRATEGIES:
    for p in [0.1, 0.25, 0.50, 0.75, 1.0]:
        experiment = copy.deepcopy(default)
        experiment['strategy']['name'] = strategy
        experiment['warmup_strategy']['name'] = strategy
        experiment['strategy']['p'] = p
        experiment['desc'] = "strategy: %s, prob: %s" \
                             % (strategy, str(p))
        EXPERIMENT_QUEUE.append(experiment)
"""
# Compare SDF, LFU, Hybrid for default values
#"""

PLAYBACK_BUFFERS = [int(VIDEO_PLAYBACK_RATE), int(1.5*VIDEO_PLAYBACK_RATE), int(2*VIDEO_PLAYBACK_RATE), int(2.5*VIDEO_PLAYBACK_RATE), int(3.0*VIDEO_PLAYBACK_RATE)]
#PLAYBACK_BUFFERS = [int(VIDEO_PLAYBACK_RATE)]
for strategy in STRATEGIES:
    for buffer_size in PLAYBACK_BUFFERS:
        if N_CHUNKS < buffer_size:
            raise ValueError("Invalid parameters: playback buffer size must be smaller than the number of video segments.")
        experiment = copy.deepcopy(default)
        experiment['strategy']['name'] = strategy
        experiment['strategy']['wndw_size'] = buffer_size
        experiment['warmup_strategy']['name'] = strategy
        experiment['warmup_strategy']['wndw_size'] = buffer_size
        if strategy is not 'OPTIMAL':
            experiment['warmup_strategy']['advertisement_period'] = 5.0
            experiment['strategy']['advertisement_period'] = 5.0
            experiment['warmup_strategy']['n_contents'] = N_CONTENTS
            experiment['strategy']['n_contents'] = N_CONTENTS
            if strategy is 'NDN':
                experiment['warmup_strategy']['load_balance_distance'] = 1
                experiment['strategy']['load_balance_distance'] = 1
        experiment['desc'] = "strategy: %s buffer size %s" \
                         % (strategy, str(buffer_size))
        EXPERIMENT_QUEUE.append(experiment)
#"""
# Experiment with different budgets
"""
budgets = [N_SERVICES/8, N_SERVICES/4, N_SERVICES/2, 0.75*N_SERVICES, N_SERVICES, 2*N_SERVICES]
for strategy in STRATEGIES:
    for budget in budgets:
        experiment = copy.deepcopy(default)
        experiment['strategy']['name'] = strategy
        experiment['warmup_strategy']['name'] = strategy
        experiment['computation_placement']['service_budget'] = budget
        experiment['strategy']['replacement_interval'] = REPLACEMENT_INTERVAL
        experiment['strategy']['n_replacements'] = NUM_REPLACEMENTS
        experiment['desc'] = "strategy: %s, budget: %s" \
                             % (strategy, str(budget))
        EXPERIMENT_QUEUE.append(experiment)
"""
# Experiment comparing FIFO with EDF 
"""
for schedule_policy in ['EDF', 'FIFO']:
    for strategy in STRATEGIES:
        experiment = copy.deepcopy(default)
        experiment['strategy']['name'] = strategy
        experiment['warmup_strategy']['name'] = strategy
        experiment['sched_policy']['name'] = schedule_policy
        experiment['desc'] = "strategy: %s, schedule policy: %s" \
                             % (strategy, str(schedule_policy))
        EXPERIMENT_QUEUE.append(experiment)
"""
# Experiment with various zipf values
"""
for alpha in [0.1, 0.25, 0.50, 0.75, 1.0]:
    for strategy in STRATEGIES:
        experiment = copy.deepcopy(default)
        experiment['workload']['alpha'] = alpha
        experiment['strategy']['name'] = strategy
        experiment['desc'] = "strategy: %s, zipf: %s" \
                         % (strategy, str(alpha))
        EXPERIMENT_QUEUE.append(experiment)
"""
# Experiment with various request rates (for sanity checking)
