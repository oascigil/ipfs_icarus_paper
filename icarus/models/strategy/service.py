# -*- coding: utf-8 -*-
"""Implementations of all service-based strategies"""
from __future__ import division
from __future__ import print_function

import networkx as nx
import sys

import random
from math import ceil
from icarus.registry import register_strategy
from icarus.util import inheritdoc, path_links
from .base import Strategy
from icarus.models.service import Task, VM

__all__ = [
       'Optimal',
       'Ndn',
       'Ndn_measurement',
       'Ipfs'
           ]

# Status codes
# TODO define these as class variables of Event class
REQUEST = 0
RESPONSE = 1
TASK_COMPLETE = 2
CONSUME = 3
SESSION_COMPLETE = 4
# Admission results:
DEADLINE_MISSED = 0
CONGESTION = 1
SUCCESS = 2
CLOUD = 3
NO_INSTANCES = 4

class Measurement(object):
    """
    Emulate measurement table of NDN
    """
    
    def __init__(self, nodes):
        self.all_measurements = {x:{} for x in nodes}

    def add(self, node, receiver, record):
        """
        Add a new measurement observed at time t for rtt from node to receiver.
        """
        table = self.all_measurements[node]
        table[receiver] = record

    def lookup(self, node, receiver, time, freshness_limit):
        """
        lookup the latest measurement
        """
        
        table = self.all_measurements[node]
        if receiver in table.keys():
            record = table[receiver]
            if time - record.time > freshness_limit:
                del table[receiver]
                return None
            else:
                return record

class Measurement_Record(object):
    """
    A measurment record
    """

    def __init__(self, time, rtt):
        self.time = time
        self.rtt = rtt

class Pit(object):
    """
    Emulate Pit table for a given list of nodes.
    """

    def __init__(self, nodes):
        self.all_pits = {x:{}  for x in nodes}
        self.dest_counts = {x:{} for x in nodes}

    def insert_with_hint(self, node, content, record):
        """
        insert a record to the pit table containing a fwd hint.
        """

        pit = self.all_pits[node]
        hint = record.destination
        if hint is None:
            raise ValueError("Hint is None in insert_with_hint()")
        aggregation = False
        if content in pit.keys():
            records = pit[content]
            for aRecord in records:
                if aRecord.flow_id == record.flow_id:
                    print("Error in pit insert(): request is revisting the node: " + str(node) + " for flow_id: " + str(record.flow_id))
                    return None
            
            add_record = True
            for aRecord in records:
                if aRecord.destination == record.destination:
                    aggregation = True
                    if aRecord.prev_hop == record.prev_hop:
                        # no need to add the record to pit
                        add_record = False
                        break
            
            if add_record is True:
                records.append(record)    
        else:
            pit[content] = [record]

        # update the destination count for the node
        counts = self.dest_counts[node]
        if record.destination in counts.keys():
            counts[record.destination] += 1
        else:
            counts[record.destination] = 1

        return aggregation

    def insert(self, node, content, record):
        """
        insert a record to the pit table.
        Returns True if the insertion has led to aggregation with existing records. Otherwise, returns False.
        """
        pit = self.all_pits[node]
        aggregation = False
        if content in pit.keys():
            records = pit[content]
            for aRecord in records:
                if aRecord.flow_id == record.flow_id:
                    print("Error in pit insert(): request is revisting the node: " + str(node) + " for flow_id: " + str(record.flow_id))
                    return None
            for aRecord in records:
                if aRecord.prev_hop == record.prev_hop:
                    # no need to add the record to pit
                    break 
            else:    
                records.append(record)
            aggregation = True
        else:
            pit[content] = [record]

        # update the destination count for the node
        counts = self.dest_counts[node]
        if record.destination in counts.keys():
            counts[record.destination] += 1
        else:
            counts[record.destination] = 1

        return aggregation

    def lookup(self, node, content):
        """
        lookup content in the pit table and
        return an array of pit records.
        """
        pit = self.all_pits[node]
        if content in pit.keys():
            return pit[content]
        else:
            return None

    def remove_expired_pits(self, time):
        """Remove all pit records that has expired
        """
        for node, pit in self.all_pits.iteritems():
            for content in pit.keys():
                records = pit[content]
                for record in records[:]:
                    if time - record.time >= PitRecord.EXPIRATION:
                        #print ("Removing expired Pit record at node: " + str(node))
                        records.remove(record)

    def remove(self, node, content):
        """
        remove the pit record
        """
        pit = self.all_pits[node]
        if content not in pit.keys():
            raise ValueError("Error in Pit_Table remove(): content: " + str(content) + " does not exist")
        
        # update the destination count for the node
        counts = self.dest_counts[node]
        for record in pit[content]:
            if record.destination in counts.keys():
                counts[record.destination] -= 1
            else:
                raise ValueError("Error in Pit remove(): destination count is missing for content: " + str(content) + " @node: " + str(node))

            if counts[record.destination] < 0:
                raise ValueError("Error in Pit remove(): destination count is inaccurate")
                
        del pit[content] 

class PitRecord(object):
    """
    Record containing flow_id and receiver
    """
    EXPIRATION = 6.0
    def __init__(self, flow_id, receiver, destination, prev_hop, time):
        self.flow_id = flow_id
        self.receiver = receiver
        self.destination = destination
        self.prev_hop = prev_hop
        self.time = time

class Session(object):
    """
    A receiver's state of a VoD download
    """
    # VoD consumer session state
    BUFFERING = 0 
    PLAYING = 1

    # consumption rate of chunks per second
    RATE = 20
    def __init__(self, view, controller, receiver, content, n_chunks, buffer_size, flow_id): 
        self.view = view
        self.controller = controller
        self.content = content
        # state of the session (either buffering initially or playing)
        self.state = Session.BUFFERING
        # size of the video playback buffer
        self.buffer_size = buffer_size 
        # video playback buffer
        self.video_buffer = []
        # number of chunks in a video content
        self.n_chunks = n_chunks
        # segment to be consumed next by the video app
        self.receiver = receiver
        # next segment to request 
        self.next_segment = content+buffer_size
        # last consumed segment
        self.last_consumed = None
        # flow id associated with the session
        self.flow_id = flow_id
    
    def deliver(self, time, content, flow_id):
        """
        This method is called when a content is received by a receiver. The content is added to the 
        video playback buffer.
        """
        
        if content in self.video_buffer:
            print ("Segment: " + str(content) + " already downloaded at receiver: " + str(self.receiver))
            raise ValueError("Error in Session deliver(): segment already downloaded")

        if self.last_consumed is not None and content <= self.last_consumed:
            print ("Segment: " + str(content) + " already consumed at receiver: " + str(self.receiver))
            raise ValueError("Error in Session deliver(): segment already consumed")

        self.video_buffer.append(content)
        # there may be out of order arrivals
        self.video_buffer = sorted(self.video_buffer)
        if self.state == Session.BUFFERING:
            # set the buffer size:
            if self.last_consumed is not None and ((self.last_consumed + self.buffer_size) > (self.content + self.n_chunks - 1)):
                # end of download, so shrink the buffer
                self.buffer_size = self.content + self.n_chunks - self.last_consumed - 1
            if len(self.video_buffer) >= self.buffer_size:
                self.state = Session.PLAYING 
                # schedule next consume event
                self.controller.add_event(time + 1.0/Session.RATE, self.receiver, self.video_buffer[0], None, flow_id, None, None, CONSUME)
            
    def consume(self, time, content, flow_id):
        """
        Consume the content from the playback buffer.
        Returns True for success; False for failure of content consumption from the buffer.
        """
        if self.state == Session.BUFFERING:
            raise ValueError("Error: consume should not be called while buffering")
        if content not in self.video_buffer:
            #print ("At time: " + str(time) + " playback is stalled at receiver: " + str(self.receiver) + " flow_id: " + str(flow_id) + " content: " + str(content) + " not in buffer...")
            
            self.state = Session.BUFFERING
            self.controller.report_buffering(content, flow_id)
            return False
        else:
            self.video_buffer.remove(content)
            self.last_consumed = content
            if (self.next_segment % self.n_chunks) != 0:
                # schedule next request (sent immediately to server)
                deadline = self.get_playback_deadline(time)
                server = self.view.get_optimal_server(time, self.receiver, self.next_segment, Optimal.CONTENT_SERVICE, self.flow_id, deadline)
                if self.view.has_cache(server):
                    # Refresh content (by re-putting) at the receiver to avoid cache miss
                    self.controller.put_content(server, self.next_segment)
                path_delay = self.view.path_delay(self.receiver, server)
                self.controller.forward_request_path(self.receiver, server)
                self.controller.add_event(time+path_delay, self.receiver, self.next_segment, server, self.flow_id, deadline, path_delay, REQUEST)
                self.next_segment += 1
            if ((self.last_consumed % self.n_chunks) != (self.n_chunks-1)):
                # schedule the next consume
                self.controller.add_event(time + 1.0/Session.RATE, self.receiver, self.last_consumed + 1, None, flow_id, None, None, CONSUME)

            return True
    
    def consume_ndn(self, time, content, flow_id, content_servers):
        """
        Consume the content from the playback buffer.
        Returns True for success; False for failure of content consumption from the buffer.
        """
        if self.state == Session.BUFFERING:
            raise ValueError("Error: consume should not be called while buffering")

        if content not in self.video_buffer:
            #print ("At time: " + str(time) + " playback is stalled at receiver: " + str(self.receiver) + " flow_id: " + str(flow_id) + " content: " + str(content) + " not in buffer...")
            
            self.state = Session.BUFFERING
            self.controller.report_buffering(content, flow_id)
            return False
        else:
            self.video_buffer.remove(content)
            self.last_consumed = content
            if (self.next_segment % self.n_chunks) != 0:
                # schedule next request (sent immediately to server)
                deadline = self.get_playback_deadline(time)
                servers = self.view.get_min_distance_servers(self.receiver, self.next_segment, content_servers)
                server = servers[0]
                if self.view.has_cache(server):
                    # Refresh content (by re-putting) at the receiver to avoid cache miss
                    self.controller.put_content(server, self.next_segment)
                path = self.view.shortest_path(self.receiver, server)
                next_hop = path[1]
                delay = self.view.path_delay(self.receiver, next_hop)
                self.controller.forward_request_hop(self.receiver, next_hop)
                self.controller.add_event(time+delay, self.receiver, self.next_segment, next_hop, self.flow_id, deadline, delay, REQUEST, None, self.receiver)
                self.next_segment += 1
            if ((self.last_consumed % self.n_chunks) != (self.n_chunks-1)):
                # schedule the next consume
                self.controller.add_event(time + 1.0/Session.RATE, self.receiver, self.last_consumed + 1, None, flow_id, None, None, CONSUME)
                #self.last_consumed += 1

            return True
    
    def consume_and_request_from_server(self, time, content, flow_id, server):
        """
        Consume the content from the playback buffer.
        Returns True for success; False for failure of content consumption from the buffer.
        """
        if self.state == Session.BUFFERING:
            raise ValueError("Error: consume should not be called while buffering")

        if content not in self.video_buffer:
            #print ("At time: " + str(time) + " playback is stalled at receiver: " + str(self.receiver) + " flow_id: " + str(flow_id) + " content: " + str(content) + " not in buffer...")
            
            self.state = Session.BUFFERING
            self.controller.report_buffering(content, flow_id)
            return False
        else:
            self.video_buffer.remove(content)
            self.last_consumed = content
            if (self.next_segment % self.n_chunks) != 0:
                # schedule next request (sent immediately to server)
                deadline = self.get_playback_deadline(time)
                if self.view.has_cache(server):
                    # Refresh content (by re-putting) at the receiver to avoid cache miss
                    self.controller.put_content(server, self.next_segment)
                path = self.view.shortest_path(self.receiver, server)
                next_hop = path[1]
                delay = self.view.path_delay(self.receiver, next_hop)
                self.controller.forward_request_hop(self.receiver, next_hop)
                self.controller.add_event(time+delay, self.receiver, self.next_segment, next_hop, self.flow_id, deadline, delay, REQUEST, None, self.receiver, server)
                self.next_segment += 1
            if ((self.last_consumed % self.n_chunks) != (self.n_chunks-1)):
                # schedule the next consume
                self.controller.add_event(time + 1.0/Session.RATE, self.receiver, self.last_consumed + 1, None, flow_id, None, None, CONSUME)
                #self.last_consumed += 1

            return True

    def get_playback_deadline(self, curr_time):
        n_segments = None
        if self.last_consumed != None:
            n_segments = self.next_segment - self.last_consumed
        else:
            n_segments = self.buffer_size

        return curr_time + (1.0/Session.RATE)*n_segments

@register_strategy('OPTIMAL')
class Optimal(Strategy):
    """
    A strategy for p2p content delivery for video-on-demand using a centralised, load-aware resolution mechanism.
    """
    CONTENT_SERVICE = 0
    def __init__(self, view, controller, n_chunks, playback_rate, wndw_size = 10, debug=False, **kwargs):
        super(Optimal, self).__init__(view,controller)
        self.last_replacement = 0
        self.n_chunks = n_chunks
        self.wndw_size = wndw_size
        self.topology = view.topology()
        # last k (i.e.,of a content) requested by a receiver
        self.consumer_session = {v:None for v in self.topology.graph['receivers']}
        self.debug = debug
        Session.RATE = playback_rate
        random.seed(0)
    
    # Optimal
    @inheritdoc(Strategy)
    def process_event(self, time, receiver, content, log, node, flow_id, deadline, rtt_delay, status, task=None, prev_hop=None, destination=None):
        if self.debug:
            print ("Event: @time: " + str(time) + " @receiver: " + str(receiver) + " @node: " + str(node) + " @content: " + str(content) + " @status: " + str(status)) 
        if status == CONSUME:
            session = self.consumer_session[receiver]
            if session is None:
                raise ValueError("Error in Optimal: no session found for receiver: " + str(receiver))
            success = session.consume(time, content, flow_id)
            if success and (content % self.n_chunks) == (self.n_chunks-1):
                # last segment is consumed, end session.
                del self.consumer_session[receiver]
                self.controller.add_event(time+(1.0/Session.RATE), receiver, None, None, flow_id, 0, None, SESSION_COMPLETE)
                self.controller.end_session(True, time, flow_id)
            #else:
                # session is buffering
        # Optimal
        elif status == REQUEST:
            session = self.consumer_session.get(receiver, None)
            if receiver == node:
                if content % self.n_chunks == 0:
                    # initiate the session and fill the window
                    self.controller.start_session(time, receiver, content, log, flow_id, self.n_chunks)
                    if session != None:
                        print ("Event: @time: " + str(time) + " @receiver: " + str(receiver) + " @node: " + str(node) + " @content: " + str(content) + " @status: " + str(status)) 
                        raise ValueError("Error: There is an on-going session.")
                    self.consumer_session[receiver] = Session(self.view, self.controller, receiver, content, self.n_chunks, self.wndw_size, flow_id)
                    for incr in range(0, self.wndw_size):
                        server = self.view.get_optimal_server(time+(1.0*incr/1000), receiver, content+incr, Optimal.CONTENT_SERVICE, flow_id)
                        if self.view.has_cache(server):
                            # Refresh content (by re-putting) at the receiver to avoid cache miss
                            self.controller.put_content(server, content+incr)
                        path_delay = self.view.path_delay(receiver, server)
                        self.controller.forward_request_path(receiver, server)
                        self.controller.add_event(time+(1.0*incr/1000)+path_delay, receiver, content+incr, server, flow_id, 0.0, path_delay, REQUEST)
                else:
                    # pick the closest receiver which satisfies the consumption deadline
                    deadline = session.get_playback_deadline(time)
                    server = self.view.get_optimal_server(time, receiver, content, Optimal.CONTENT_SERVICE, flow_id, deadline)
                    if self.view.has_cache(server):
                        # Refresh content (by re-putting) at the receiver to avoid cache miss
                        self.controller.put_content(server, content)
                    path_delay = self.view.path_delay(node, server)
                    self.controller.forward_request_path(node, server)
                    self.controller.add_event(time+path_delay, receiver, content, server, flow_id, deadline, path_delay, REQUEST)
            # Optimal
            else: 
                source = self.view.content_source(content)
                if node == source:
                    delay = self.view.path_delay(node, receiver)
                    self.controller.add_event(time+delay, receiver, content, receiver, flow_id, deadline, rtt_delay, RESPONSE)
                    self.controller.forward_content_path(source, receiver)
                else:
                    # add request in the task queue of the server
                    if self.controller.get_content(node, content) is False:
                        raise ValueError("Request arrived at a server node which does not have the content requested")
                    compSpot = None
                    if self.view.has_computationalSpot(node):
                        compSpot = self.view.compSpot(node)
                    else:
                        raise ValueError("Error: node: " + str(node) + " has not a computation spot")
                    compSpot.admit_content_request_task(Optimal.CONTENT_SERVICE, content, time, flow_id, deadline, receiver, rtt_delay, self.controller, self.debug)    

        # Optimal
        elif status == RESPONSE:
            if receiver == node:
                session = self.consumer_session[receiver]
                if session is None:
                    raise ValueError("Error: No existing session for the received Data")
                session.deliver(time, content, flow_id)
                self.controller.put_content(receiver, content)
            else:
                raise ValueError("Error: Response received at node: " + str(node))
        # Optimal
        elif status == TASK_COMPLETE:
            # route current request back to the receiver
            compSpot = self.view.compSpot(node)
            self.controller.complete_task(task, time)
            delay = self.view.path_delay(node, task.receiver)
            self.controller.forward_content_path(node, task.receiver)
            self.controller.add_event(time+delay, task.receiver, task.content, task.receiver, task.flow_id, task.expiry, task.rtt_delay, RESPONSE)
            # schedule next task if any 
            newTask = compSpot.scheduler.schedule(time)
            if newTask is not None:
                self.controller.add_event(newTask.completionTime, newTask.receiver, newTask.content, node, newTask.flow_id, newTask.expiry, newTask.rtt_delay, TASK_COMPLETE, newTask)

@register_strategy('NDN')
class Ndn(Strategy):
    """
    A strategy for p2p video-on-demand content delivery using icn using load-aware name resolution.
    """
    CONTENT_SERVICE = 0
    def __init__(self, view, controller, n_chunks, playback_rate, n_contents, wndw_size = 10, load_balance_distance = 1, advertisement_period = 30.0, debug=False, **kwargs):
        super(Ndn, self).__init__(view,controller)
        self.last_replacement = 0
        self.n_chunks = n_chunks
        self.wndw_size = wndw_size
        self.topology = view.topology()
        # last k (i.e.,of a content) requested by a receiver
        self.consumer_session = {v:None for v in self.topology.graph['receivers']}
        self.debug = debug
        Session.RATE = playback_rate
        self.advertisement_period = advertisement_period
        self.last_advertisement = 0.0
        self.pit = Pit(self.topology.graph['routers'] + self.topology.graph['receivers'])
        self.n_contents = n_contents
        self.load_balance_distance = load_balance_distance
        self.content_servers = {c:set() for c in range(0, self.n_chunks*self.n_contents)}
        # flows that were terminated due to looping issue
        self.terminated_flows = set()
        self.content_servers_old = {c:set() for c in range(0, self.n_chunks*self.n_contents)}
        self.flow_start_times = {}
        self.flows = []
        random.seed(0)

    def terminate_flow(self, time, receiver, flow_id):
        """ Force a flow to terminate
        """
        self.terminated_flows.add(flow_id)
        del self.consumer_session[receiver]
        self.controller.add_event(time+(1.0/Session.RATE), receiver, None, None, flow_id, 0, None, SESSION_COMPLETE)
        self.controller.end_session(True, time, flow_id)

    def update_content_locations(self, time):
        """
        This simulates the advertisement of content locations
        """
        for content in range(0, self.n_contents*self.n_chunks):
            self.content_servers_old[content] = self.content_servers[content]

        for content in range(0, self.n_contents*self.n_chunks):
            locations = self.view.content_locations(content, True)
            self.content_servers[content] = locations

        flows_to_remove = []
        for f in self.flows:
            if time - self.flow_start_times[f] > 6*self.n_chunks/Session.RATE:
                print ("Error in NDN: Flow: " + str(f)  + " has not completed. Terminating flow...")
                #raise ValueError("Error: Incomplete flow.")
                receiver = self.controller.session[f]['receiver']
                self.terminate_flow(time, receiver, f)
                flows_to_remove.append(f)
                    
        for f in flows_to_remove:
            self.flows.remove(f)
            del self.flow_start_times[f]

        self.pit.remove_expired_pits(time)

    # Ndn
    @inheritdoc(Strategy)
    def process_event(self, time, receiver, content, log, node, flow_id, deadline, rtt_delay, status, task=None, prev_hop=None, destination=None):
        
        #print ("Event: @time: " + str(time) + " @flow_id: " + str(flow_id) + " @receiver: " + str(receiver) + " @node: " + str(node) + " @content: " + str(content) + " @status: " + str(status) + " @prev_hop: " + str(prev_hop)) 
        if flow_id in self.terminated_flows:
            return

        if (time - self.last_advertisement) > self.advertisement_period:
            # update the content locations information
            print ("Content advertisement at time: " + str(time))
            self.update_content_locations(time)
            self.last_advertisement = time
            
        source = self.view.content_source(content)
        if status == CONSUME:
            session = self.consumer_session[receiver]
            if session is None:
                raise ValueError("Error in NDN: no session found for receiver: " + str(receiver))
            success = session.consume_ndn(time, content, flow_id, self.content_servers)
            if success and (content % self.n_chunks) == (self.n_chunks-1):
                # last segment is consumed, end session.
                del self.consumer_session[receiver]
                self.controller.add_event(time+(1.0/Session.RATE), receiver, None, None, flow_id, 0, None, SESSION_COMPLETE)
                if flow_id in self.flows:
                    self.flows.remove(flow_id)
                if flow_id in self.flow_start_times.keys():
                    del self.flow_start_times[flow_id]
                self.controller.end_session(True, time, flow_id)
        
        # Ndn
        elif status == REQUEST:
            if node != source and node in self.topology.graph['routers']:
                if self.view.has_cache(node) and self.controller.get_content(node, content):
                    if prev_hop is None:
                        raise ValueError("Error in Ndn Request: prev_hop is None")
                    delay = self.view.path_delay(node, prev_hop)
                    self.controller.forward_content_hop(node, prev_hop)
                    self.controller.add_event(time+delay, receiver, content, prev_hop, flow_id, deadline, rtt_delay, RESPONSE)
                    return
                servers = []
                server = None
                if destination is None:
                    if self.flow_start_times[flow_id] < self.last_advertisement:
                        servers = self.view.get_min_distance_servers(receiver, content, self.content_servers_old)
                    else:
                        servers = self.view.get_min_distance_servers(receiver, content, self.content_servers)
                    if len(self.view.shortest_path(receiver, servers[0])) <= self.load_balance_distance:
                        # forward to least-loaded server
                        servers = self.view.load_balance_sort(servers, self.pit, node)
                        server = self.view.select_server(node, servers, prev_hop)
                    else:
                        # forward to closest server
                        server = self.view.select_server(node, servers, prev_hop)
                    pit_record = PitRecord(flow_id, receiver, server, prev_hop, time)
                    ret = self.pit.insert_with_hint(node, content, pit_record)
                    if ret is None:
                        # there is a problem (looping) with this flow, terminate session
                        self.terminate_flow(time, receiver, flow_id)
                        del self.flow_start_times[flow_id]
                        self.flows.remove(flow_id)
                    elif ret is False:
                        # no pit aggregation
                        path = self.view.shortest_path(node, server)
                        next_hop = path[1]
                        delay = self.view.path_delay(node, next_hop)
                        self.controller.forward_request_hop(node, next_hop)
                        self.controller.add_event(time+delay, receiver, content, next_hop, flow_id, deadline, rtt_delay+2*delay, REQUEST, None, node, server)
                else: # destination is already set
                    if len(self.view.shortest_path(receiver, destination)) <= self.load_balance_distance:
                        if self.flow_start_times[flow_id] < self.last_advertisement:
                            servers = self.view.get_min_distance_servers(receiver, content, self.content_servers_old)
                        else:
                            servers = self.view.get_min_distance_servers(receiver, content, self.content_servers)
                        destination = self.view.select_server(node, servers, prev_hop)
                        
                    pit_record = PitRecord(flow_id, receiver, destination, prev_hop, time)
                    ret = self.pit.insert_with_hint(node, content, pit_record)
                    if ret is None:
                        # there is a problem (looping) with this flow, terminate session
                        self.terminate_flow(time, receiver, flow_id)
                        del self.flow_start_times[flow_id]
                        self.flows.remove(flow_id)
                    elif ret is False:
                        # no pit aggregation
                        path = self.view.shortest_path(node, destination)
                        next_hop = path[1]
                        delay = self.view.path_delay(node, next_hop)
                        self.controller.forward_request_hop(node, next_hop)
                        self.controller.add_event(time+delay, receiver, content, next_hop, flow_id, deadline, rtt_delay+2*delay, REQUEST, None, node, destination)
            # Ndn
            elif node == receiver:
                session = self.consumer_session.get(receiver, None)
                if content % self.n_chunks == 0:
                    # initiate the session and fill the window
                    self.controller.start_session(time, receiver, content, log, flow_id, self.n_chunks)
                    if session != None:
                        print ("Event: @time: " + str(time) + " @receiver: " + str(receiver) + " @node: " + str(node) + " @content: " + str(content) + " @status: " + str(status)) 
                        raise ValueError("Error: There is an on-going session.")
                    self.consumer_session[receiver] = Session(self.view, self.controller, receiver, content, self.n_chunks, self.wndw_size, flow_id)
                    self.flow_start_times[flow_id] = time
                    self.flows.append(flow_id)
                    for incr in range(0, self.wndw_size):
                        servers = self.view.get_min_distance_servers(receiver, content+incr, self.content_servers)
                        server = servers[0]
                        path = self.view.shortest_path(receiver, server)
                        next_hop = path[1]
                        delay = self.view.path_delay(node, next_hop)
                        self.controller.forward_request_hop(node, next_hop)
                        self.controller.add_event(time+(1.0*incr/1000)+delay, receiver, content+incr, next_hop, flow_id, 0.0, 2*delay, REQUEST, None, node)
                else:
                    # on-going session at the receiver
                    deadline = session.get_playback_deadline(time)
                    servers = self.view.get_min_distance_servers(receiver, content, self.content_servers)
                    server = servers[0]
                    path = self.view.shortest_path(node, server)
                    next_hop = path[1]
                    delay = self.view.path_delay(node, next_hop)
                    self.controller.forward_request_hop(node, next_hop)
                    self.controller.add_event(time+delay, receiver, content, next_hop, flow_id, deadline, rtt_delay+2*delay, REQUEST, None, node)

            elif node != receiver and node in self.topology.graph['receivers']:
                if prev_hop is None:
                    raise ValueError("Error in Ndn: prev_hop is None.")
                pit_record = PitRecord(flow_id, receiver, node, prev_hop, time)
                ret = self.pit.insert(node, content, pit_record)
                if ret is False:
                    # no pit aggregation, execute task
                    compSpot = self.view.compSpot(node)
                    services = self.view.services() 
                    serviceTime = services[Ndn.CONTENT_SERVICE].service_time
                    aTask = Task(time, Task.TASK_TYPE_SERVICE, deadline, rtt_delay, node, Ndn.CONTENT_SERVICE, serviceTime, flow_id, receiver, None, None, content)
                    self.controller.get_content(node, content)
                    compSpot.scheduler._taskQueue.append(aTask)
                    compSpot.admit_content_request_task(Ndn.CONTENT_SERVICE, content, time, flow_id, deadline, receiver, rtt_delay, self.controller, self.debug)    

            elif node == source:
                delay = self.view.path_delay(node, prev_hop)
                self.controller.add_event(time+delay, receiver, content, prev_hop, flow_id, deadline, rtt_delay, RESPONSE)
            else:
                raise ValueError("This should not happen in process_event() of Ndn strategy")
        # Ndn
        elif status == RESPONSE:
            if node != receiver:
                # lookup and delete pit state and forward Data
                results = self.pit.lookup(node, content)
                if results is None:
                    # this can happen now that insert_with_hint is used. Aggregation is disabled and multiple responses may be received for the same content.
                    return
                for pit_record in results:
                    prev_hop = pit_record.prev_hop
                    flow_id = pit_record.flow_id 
                    receiver = pit_record.receiver
                    delay = self.view.path_delay(node, prev_hop)
                    self.controller.forward_content_hop(node, prev_hop)
                    self.controller.add_event(time+delay, receiver, content, prev_hop, flow_id, deadline, rtt_delay, RESPONSE)
                self.pit.remove(node, content)
                if self.view.has_cache(node):
                    self.controller.put_content(node, content)
            else:
                session = self.consumer_session[receiver]
                session.deliver(time, content, flow_id)
                self.controller.put_content(receiver, content)
        # Ndn
        elif status == TASK_COMPLETE:
            # route current request back to the receiver
            compSpot = self.view.compSpot(node)
            self.controller.complete_task(task, time)
            results = self.pit.lookup(node, task.content)
            for pit_record in results:
                prev_hop = pit_record.prev_hop
                flow_id = pit_record.flow_id 
                receiver = pit_record.receiver
                delay = self.view.path_delay(node, prev_hop)
                self.controller.forward_content_hop(node, prev_hop)
                self.controller.add_event(time+delay, receiver, content, prev_hop, flow_id, deadline, rtt_delay, RESPONSE)
            self.pit.remove(node, task.content)
            # schedule next task if any 
            newTask = compSpot.scheduler.schedule(time)
            if newTask is not None:
                self.controller.add_event(newTask.completionTime, newTask.receiver, newTask.content, node, newTask.flow_id, newTask.expiry, newTask.rtt_delay, TASK_COMPLETE, newTask)


@register_strategy('NDN_MEASUREMENT')
class Ndn_measurement(Strategy):
    """
    A strategy for p2p video-on-demand content delivery using icn using load-aware name resolution.
    """
    CONTENT_SERVICE = 0
    def __init__(self, view, controller, n_chunks, playback_rate, n_contents, wndw_size = 10, advertisement_period = 10.0, debug=False, **kwargs):
        super(Ndn_measurement, self).__init__(view,controller)
        self.last_replacement = 0
        self.n_chunks = n_chunks
        self.wndw_size = wndw_size
        self.topology = view.topology()
        # last k (i.e.,of a content) requested by a receiver
        self.consumer_session = {v:None for v in self.topology.graph['receivers']}
        self.debug = debug
        Session.RATE = playback_rate
        self.advertisement_period = advertisement_period
        self.last_advertisement = 0.0
        self.pit = Pit(self.topology.graph['routers'] + self.topology.graph['receivers'])
        self.n_contents = n_contents
        self.content_servers = {c:set() for c in range(0, self.n_chunks*self.n_contents)}
        # flows that were terminated due to looping issue
        self.terminated_flows = set()
        self.content_servers_old = {c:set() for c in range(0, self.n_chunks*self.n_contents)}
        self.flow_start_times = {}
        self.flows = []
        self.measurement = Measurement(self.topology.graph['routers'] + self.topology.graph['receivers'])
        random.seed(0)
    
    def forwarding_strategy_load_balance(self, time, content, node, servers, deadline, status):
        """
        Pick a server based on available measurements and deadline from servers.
        """
        measurements = {}
        for server in servers:
            record = self.measurement.lookup(node, server, time, self.advertisement_period)
            if record is None:
                measurements[server] = 0.0
            else:
                measurements[server] = record.rtt
        #print ("Time: " + str(time) + " Deadline: " + str(deadline) + "Status: " + str(status) + " Measurements: " + str(measurements))

        if status == Session.BUFFERING:
            chosen_servers = []
            for server in servers:
                if (measurements[server] + time) < deadline:
                    chosen_servers.append(server)

                if len(chosen_servers) >= 10:
                    break
            #print ("Chosen servers: " + str(chosen_servers))
            if len(chosen_servers) > 0:
               return random.choice(chosen_servers)
            else:
                return self.view.content_source(content)

        elif status == Session.PLAYING:
            chosen_servers = []
            for server in servers:
                if measurements[server] != 0.0 and (measurements[server] + time ) < deadline:
                    chosen_servers.append(server)
            
            if len(chosen_servers) > 0:
               return random.choice(chosen_servers)
            else:
                return self.view.content_source(content)

        else:
            # this should not happen
            return None

    def terminate_flow(self, time, receiver, flow_id):
        """ Force a flow to terminate
        """
        self.terminated_flows.add(flow_id)
        del self.consumer_session[receiver]
        self.controller.add_event(time+(1.0/Session.RATE), receiver, None, None, flow_id, 0, None, SESSION_COMPLETE)
        self.controller.end_session(True, time, flow_id)

    def update_content_locations(self, time):
        """
        This simulates the advertisement of content locations
        """
        for content in range(0, self.n_contents*self.n_chunks):
            self.content_servers_old[content] = self.content_servers[content]

        for content in range(0, self.n_contents*self.n_chunks):
            locations = self.view.content_locations(content, True)
            self.content_servers[content] = locations

        flows_to_remove = []
        for f in self.flows:
            if time - self.flow_start_times[f] > 6*self.n_chunks/Session.RATE:
                print ("Error in NDN_measurement strategy: Flow: " + str(f)  + " has not completed. Terminating flow...")
                #raise ValueError("Error: Incomplete flow.")
                receiver = self.controller.session[f]['receiver']
                self.terminate_flow(time, receiver, f)
                flows_to_remove.append(f)
                    
        for f in flows_to_remove:
            self.flows.remove(f)
            del self.flow_start_times[f]

        self.pit.remove_expired_pits(time)

    # Ndn_measurement
    @inheritdoc(Strategy)
    def process_event(self, time, receiver, content, log, node, flow_id, deadline, rtt_delay, status, task=None, prev_hop=None, destination = None):
        
        #print ("Event: @time: " + str(time) + " @flow_id: " + str(flow_id) + " @receiver: " + str(receiver) + " @node: " + str(node) + " @content: " + str(content) + " @status: " + str(status) + " @prev_hop: " + str(prev_hop) + " @destination: " + str(destination)) 
        if flow_id in self.terminated_flows:
            return

        if (time - self.last_advertisement) > self.advertisement_period:
            # update the content locations information
            print ("Content advertisement at time: " + str(time))
            self.update_content_locations(time)
            self.last_advertisement = time
            
        source = self.view.content_source(content)
        if status == CONSUME:
            session = self.consumer_session[receiver]
            if session is None:
                raise ValueError("Error in NDN: no session found for receiver: " + str(receiver))
            success = session.consume_ndn(time, content, flow_id, self.content_servers)
            if success and (content % self.n_chunks) == (self.n_chunks-1):
                # last segment is consumed, end session.
                del self.consumer_session[receiver]
                self.controller.add_event(time+(1.0/Session.RATE), receiver, None, None, flow_id, 0, None, SESSION_COMPLETE)
                if flow_id in self.flows:
                    self.flows.remove(flow_id)
                if flow_id in self.flow_start_times.keys():
                    del self.flow_start_times[flow_id]
                self.controller.end_session(True, time, flow_id)
        
        # Ndn_measurement
        elif status == REQUEST:
            if node != source and node in self.topology.graph['routers']:
                if self.view.has_cache(node) and self.controller.get_content(node, content):
                    if prev_hop is None:
                        raise ValueError("Error in Ndn_measurement Request: prev_hop is None")
                    delay = self.view.path_delay(node, prev_hop)
                    self.controller.forward_content_hop(node, prev_hop)
                    self.controller.add_event(time+delay, receiver, content, prev_hop, flow_id, deadline, rtt_delay, RESPONSE, None)
                    return
                
                if destination is None:
                    servers = self.view.get_sorted_servers_by_distance(receiver, content, self.content_servers)
                    #print ("Servers: " + str(servers))
                    client_status = self.consumer_session[receiver].state
                    destination = self.forwarding_strategy_load_balance(time, content, node, servers, deadline, client_status)
                    #print ("Server: " + str(destination))
                    pit_record = PitRecord(flow_id, receiver, destination, prev_hop, time)
                    ret = self.pit.insert_with_hint(node, content, pit_record)
                    if ret is None:
                        # there is a problem (looping) with this flow, terminate session
                        self.terminate_flow(time, receiver, flow_id)
                        del self.flow_start_times[flow_id]
                        self.flows.remove(flow_id)
                    elif ret is False:
                        # no pit aggregation
                        path = self.view.shortest_path(node, destination)
                        next_hop = path[1]
                        delay = self.view.path_delay(node, next_hop)
                        self.controller.forward_request_hop(node, next_hop)
                        self.controller.add_event(time+delay, receiver, content, next_hop, flow_id, deadline, rtt_delay+2*delay, REQUEST, None, node, destination)
                else: # request contains a hint
                    pit_record = PitRecord(flow_id, receiver, destination, prev_hop, time)
                    ret = self.pit.insert_with_hint(node, content, pit_record)
                    if ret is None:
                        # there is a problem (looping) with this flow, terminate session
                        self.terminate_flow(time, receiver, flow_id)
                        del self.flow_start_times[flow_id]
                        self.flows.remove(flow_id)
                    elif ret is False:
                        path = self.view.shortest_path(node, destination)
                        next_hop = path[1]
                        delay = self.view.path_delay(node, next_hop)
                        self.controller.forward_request_hop(node, next_hop)
                        self.controller.add_event(time+delay, receiver, content, next_hop, flow_id, deadline, rtt_delay+2*delay, REQUEST, None, node, destination)
                    
            # Ndn_measurement
            elif node == receiver:
                session = self.consumer_session.get(receiver, None)
                if content % self.n_chunks == 0:
                    # initiate the session and fill the window
                    self.controller.start_session(time, receiver, content, log, flow_id, self.n_chunks)
                    if session != None:
                        print ("Event: @time: " + str(time) + " @receiver: " + str(receiver) + " @node: " + str(node) + " @content: " + str(content) + " @status: " + str(status)) 
                        raise ValueError("Error: There is an on-going session.")
                    self.consumer_session[receiver] = Session(self.view, self.controller, receiver, content, self.n_chunks, self.wndw_size, flow_id)
                    self.flow_start_times[flow_id] = time
                    self.flows.append(flow_id)
                    deadline = time + PitRecord.EXPIRATION - 0.5 #session.get_playback_deadline(time)
                    for incr in range(0, self.wndw_size):
                        servers = self.view.get_sorted_servers_by_distance(receiver, content+incr, self.content_servers)
                        server = servers[0]   
                        path = self.view.shortest_path(receiver, server)
                        next_hop = path[1]
                        delay = self.view.path_delay(node, next_hop)
                        self.controller.forward_request_hop(node, next_hop)
                        self.controller.add_event(time+(1.0*incr/1000)+delay, receiver, content+incr, next_hop, flow_id, deadline, 2*delay, REQUEST, None, node)
                else:
                    # on-going session at the receiver
                    servers = self.view.get_sorted_servers_by_distance(receiver, content, self.content_servers)
                    server = servers[0]   
                    deadline = session.get_playback_deadline(time)
                    path = self.view.shortest_path(node, server)
                    next_hop = path[1]
                    delay = self.view.path_delay(node, next_hop)
                    self.controller.forward_request_hop(node, next_hop)
                    self.controller.add_event(time+delay, receiver, content, next_hop, flow_id, deadline, 2*delay, REQUEST, None, node)

            elif node != receiver and node in self.topology.graph['receivers']:
                if prev_hop is None:
                    raise ValueError("Error in Ndn_measurement: prev_hop is None.")
                pit_record = PitRecord(flow_id, receiver, node, prev_hop, time)
                ret = self.pit.insert(node, content, pit_record)
                if ret is False:
                    # no pit aggregation, execute task
                    compSpot = self.view.compSpot(node)
                    services = self.view.services() 
                    serviceTime = services[Ndn_measurement.CONTENT_SERVICE].service_time
                    aTask = Task(time, Task.TASK_TYPE_SERVICE, deadline, rtt_delay, node, Ndn_measurement.CONTENT_SERVICE, serviceTime, flow_id, receiver, None, None, content)
                    compSpot.scheduler._taskQueue.append(aTask)
                    self.controller.get_content(node, content)
                    compSpot.admit_content_request_task(Ndn_measurement.CONTENT_SERVICE, content, time, flow_id, deadline, receiver, rtt_delay, self.controller, self.debug)    

            elif node == source:
                delay = self.view.path_delay(node, prev_hop)
                self.controller.forward_content_hop(node, prev_hop)
                self.controller.add_event(time+delay, receiver, content, prev_hop, flow_id, deadline, rtt_delay, RESPONSE, None)
            else:
                raise ValueError("This should not happen in process_event() of Ndn_measurement strategy")
        # Ndn_measurement
        elif status == RESPONSE:
            if node != receiver:
                # lookup and delete pit state and forward Data
                results = self.pit.lookup(node, content)
                if results is None:
                    # this can happen now that insert_with_hint is used. Aggregation is disabled and multiple responses may be received for the same content.
                    return
                for pit_record in results:
                    prev_hop = pit_record.prev_hop
                    flow_id = pit_record.flow_id 
                    receiver = pit_record.receiver
                    server = pit_record.destination
                    arr_time = pit_record.time
                    rtt = time - arr_time
                    m_record = Measurement_Record(time, rtt)
                    self.measurement.add(node, server, m_record)
                    delay = self.view.path_delay(node, prev_hop)
                    self.controller.forward_content_hop(node, prev_hop)
                    self.controller.add_event(time+delay, receiver, content, prev_hop, flow_id, deadline, rtt_delay, RESPONSE, None)
                self.pit.remove(node, content)
                if self.view.has_cache(node):
                    self.controller.put_content(node, content)
            else:
                session = self.consumer_session[receiver]
                session.deliver(time, content, flow_id)
                self.controller.put_content(receiver, content)
        # Ndn_measurement
        elif status == TASK_COMPLETE:
            # route current request back to the receiver
            compSpot = self.view.compSpot(node)
            self.controller.complete_task(task, time)
            results = self.pit.lookup(node, task.content)
            for pit_record in results:
                prev_hop = pit_record.prev_hop
                flow_id = pit_record.flow_id 
                receiver = pit_record.receiver
                delay = self.view.path_delay(node, prev_hop)
                self.controller.forward_content_hop(node, prev_hop)
                self.controller.add_event(time+delay, receiver, content, prev_hop, flow_id, deadline, rtt_delay, RESPONSE, None)
            self.pit.remove(node, task.content)
            # schedule next task if any 
            newTask = compSpot.scheduler.schedule(time)
            if newTask is not None:
                self.controller.add_event(newTask.completionTime, newTask.receiver, newTask.content, node, newTask.flow_id, newTask.expiry, newTask.rtt_delay, TASK_COMPLETE, newTask)

@register_strategy('IPFS')
class Ipfs(Strategy):
    """
    A strategy for p2p video-on-demand content delivery using icn using load-aware name resolution.
    """
    CONTENT_SERVICE = 0
    def __init__(self, view, controller, n_chunks, playback_rate, n_contents, wndw_size = 10, advertisement_period = 10.0, debug=False, **kwargs):
        super(Ipfs, self).__init__(view,controller)
        self.last_replacement = 0
        self.n_chunks = n_chunks
        self.wndw_size = wndw_size
        self.topology = view.topology()
        # last k (i.e.,of a content) requested by a receiver
        self.consumer_session = {v:None for v in self.topology.graph['receivers']}
        self.debug = debug
        Session.RATE = playback_rate
        self.advertisement_period = advertisement_period
        self.last_advertisement = 0.0
        self.pit = Pit(self.topology.graph['routers'] + self.topology.graph['receivers'])
        self.n_contents = n_contents
        self.content_servers = {c:set() for c in range(0, self.n_chunks*self.n_contents)}
        # flows that were terminated due to looping issue
        self.terminated_flows = set()
        self.content_servers_old = {c:set() for c in range(0, self.n_chunks*self.n_contents)}
        self.flow_start_times = {}
        self.flows = []
        self.measurement = Measurement(self.topology.graph['routers'] + self.topology.graph['receivers'])
        random.seed(0)
    
    def forwarding_strategy_load_balance(self, time, content, node, servers, deadline, status):
        """
        Pick a server based on available measurements and deadline from servers.
        """
        measurements = {}
        for server in servers:
            record = self.measurement.lookup(node, server, time, 4*self.advertisement_period)
            if record is None:
                measurements[server] = 0.0
            else:
                measurements[server] = record.rtt

        #print ("Time: " + str(time) + " Deadline: " + str(deadline) + "Status: " + str(status) + " Measurements: " + str(measurements))
        if status == Session.BUFFERING:
            chosen_servers = []
            for server in servers:
                if (time + measurements[server]) < deadline:
                    chosen_servers.append(server)

                if len(chosen_servers) >= 10:
                    break
            if len(chosen_servers) > 0:
               return random.choice(chosen_servers)
            else:
                return self.view.content_source(content)

        elif status == Session.PLAYING:
            chosen_servers = []
            for server in servers:
                #if measurements[server] != 0.0 and ((time + measurements[server]) < deadline):
                if ((time + measurements[server]) < deadline):
                    chosen_servers.append(server)
            
            if len(chosen_servers) > 0:
               return random.choice(chosen_servers)
            else:
                return self.view.content_source(content)

        else:
            # this should not happen
            return None

    def terminate_flow(self, time, receiver, flow_id):
        """ Force a flow to terminate
        """
        self.terminated_flows.add(flow_id)
        del self.consumer_session[receiver]
        self.controller.add_event(time+(1.0/Session.RATE), receiver, None, None, flow_id, 0, None, SESSION_COMPLETE)
        self.controller.end_session(True, time, flow_id)

    def update_content_locations(self, time):
        """
        This simulates the advertisement of content locations
        """
        for content in range(0, self.n_contents*self.n_chunks):
            self.content_servers_old[content] = self.content_servers[content]

        for content in range(0, self.n_contents*self.n_chunks):
            locations = self.view.content_locations(content, False)
            self.content_servers[content] = locations

        flows_to_remove = []
        for f in self.flows:
            if time - self.flow_start_times[f] > 4*self.n_chunks/Session.RATE:
                print ("Error in IPFS: Flow: " + str(f)  + " has not completed. Terminating flow...")
                #raise ValueError("Error: Incomplete flow.")
                receiver = self.controller.session[f]['receiver']
                self.terminate_flow(time, receiver, f)
                flows_to_remove.append(f)
                    
        for f in flows_to_remove:
            self.flows.remove(f)
            del self.flow_start_times[f]

        self.pit.remove_expired_pits(time)

    # Ipfs
    @inheritdoc(Strategy)
    def process_event(self, time, receiver, content, log, node, flow_id, deadline, rtt_delay, status, task=None, prev_hop=None, destination = None):
        
        #print ("Event: @time: " + str(time) + " @flow_id: " + str(flow_id) + " @receiver: " + str(receiver) + " @node: " + str(node) + " @content: " + str(content) + " @status: " + str(status) + " @prev_hop: " + str(prev_hop) + " @destination: " + str(destination)) 
        if flow_id in self.terminated_flows:
            return

        if (time - self.last_advertisement) > self.advertisement_period:
            # update the content locations information
            print ("Content advertisement at time: " + str(time))
            self.update_content_locations(time)
            self.last_advertisement = time
            
        source = self.view.content_source(content)
        if status == CONSUME:
            session = self.consumer_session[receiver]
            if session is None:
                raise ValueError("Error in NDN: no session found for receiver: " + str(receiver))
            server = None
            if session.next_segment % self.n_chunks != 0:
                servers = self.view.content_locations(content, True, receiver, self.content_servers[session.next_segment])
                servers = list(servers)
                random.shuffle(servers)
                deadline = session.get_playback_deadline(time)
                server = self.forwarding_strategy_load_balance(time, session.next_segment, receiver, servers, deadline, session.state)
            #print("Server:" + str(server))
            success = session.consume_and_request_from_server(time, content, flow_id, server)
            if success and (content % self.n_chunks) == (self.n_chunks-1):
                # last segment is consumed, end session.
                del self.consumer_session[receiver]
                self.controller.add_event(time+(1.0/Session.RATE), receiver, None, None, flow_id, 0, None, SESSION_COMPLETE)
                if flow_id in self.flows:
                    self.flows.remove(flow_id)
                if flow_id in self.flow_start_times.keys():
                    del self.flow_start_times[flow_id]
                self.controller.end_session(True, time, flow_id)
        
        # Ipfs
        elif status == REQUEST:
            if node != source and node in self.topology.graph['routers']:
                if self.view.has_cache(node) and self.controller.get_content(node, content):
                    if prev_hop is None:
                        raise ValueError("Error in Ipfs Request: prev_hop is None")
                    delay = self.view.path_delay(node, prev_hop)
                    self.controller.forward_content_hop(node, prev_hop)
                    self.controller.add_event(time+delay, receiver, content, prev_hop, flow_id, deadline, rtt_delay, RESPONSE, None)
                    return
                
                if destination is None:
                    raise ValueError("Routing hint is not set in a request in Ipfs.")
                else: # request contains a hint
                    pit_record = PitRecord(flow_id, receiver, destination, prev_hop, time)
                    ret = self.pit.insert_with_hint(node, content, pit_record)
                    if ret is None:
                        # there is a problem (looping) with this flow, terminate session
                        self.terminate_flow(time, receiver, flow_id)
                        del self.flow_start_times[flow_id]
                        self.flows.remove(flow_id)
                    elif ret is False:
                        path = self.view.shortest_path(node, destination)
                        next_hop = path[1]
                        delay = self.view.path_delay(node, next_hop)
                        self.controller.forward_request_hop(node, next_hop)
                        self.controller.add_event(time+delay, receiver, content, next_hop, flow_id, deadline, rtt_delay+2*delay, REQUEST, None, node, destination)
                    
            # Ipfs
            elif node == receiver:
                session = self.consumer_session.get(receiver, None)
                server = None
                if content % self.n_chunks == 0:
                    # initiate the session and fill the window
                    self.controller.start_session(time, receiver, content, log, flow_id, self.n_chunks)
                    if session != None:
                        #print ("Event: @time: " + str(time) + " @receiver: " + str(receiver) + " @node: " + str(node) + " @content: " + str(content) + " @status: " + str(status)) 
                        raise ValueError("Error: There is an on-going session.")
                    self.consumer_session[receiver] = Session(self.view, self.controller, receiver, content, self.n_chunks, self.wndw_size, flow_id)
                    self.flow_start_times[flow_id] = time
                    self.flows.append(flow_id)
                    deadline = time + PitRecord.EXPIRATION - 0.5 #session.get_playback_deadline(time)
                    for incr in range(0, self.wndw_size):
                        servers = self.view.content_locations(content+incr, True, receiver, self.content_servers[content+incr])
                        servers = list(servers)
                        random.shuffle(servers)
                        #print ("Servers: " + str(servers))
                        server = self.forwarding_strategy_load_balance(time, content+incr, node, servers, deadline, Session.BUFFERING)
                        #print("Server:" + str(server))
                        path = self.view.shortest_path(receiver, server)
                        next_hop = path[1]
                        delay = self.view.path_delay(node, next_hop)
                        self.controller.forward_request_hop(node, next_hop)
                        self.controller.add_event(time+(1.0*incr/1000)+delay, receiver, content+incr, next_hop, flow_id, deadline, 2*delay, REQUEST, None, node, server)
                        pit_record = PitRecord(flow_id, receiver, server, prev_hop, time)
                        self.pit.insert_with_hint(node, content+incr, pit_record)

                else:
                    # on-going session at the receiver
                    servers = self.view.content_locations(content, True, receiver, self.content_servers[content])
                    #print ("Servers: " + str(servers))
                    server = self.forwarding_strategy_load_balance(time, content, node, servers, deadline, session.state)
                    #print("Server:" + str(server))
                    deadline = session.get_playback_deadline(time)
                    path = self.view.shortest_path(node, server)
                    next_hop = path[1]
                    delay = self.view.path_delay(node, next_hop)
                    self.controller.forward_request_hop(node, next_hop)
                    self.controller.add_event(time+delay, receiver, content, next_hop, flow_id, deadline, 2*delay, REQUEST, None, node, server)
                    pit_record = PitRecord(flow_id, receiver, server, prev_hop, time)
                    self.pit.insert_with_hint(node, content, pit_record)

            elif node != receiver and node in self.topology.graph['receivers']:
                if prev_hop is None:
                    raise ValueError("Error in Ipfs: prev_hop is None.")
                #pit_record = PitRecord(flow_id, receiver, node, prev_hop, time)
                #ret = self.pit.insert(node, content, pit_record)
                #if ret is False:
                    # no pit aggregation, execute task
                compSpot = self.view.compSpot(node)
                services = self.view.services() 
                serviceTime = services[Ipfs.CONTENT_SERVICE].service_time
                aTask = Task(time, Task.TASK_TYPE_SERVICE, deadline, rtt_delay, node, Ipfs.CONTENT_SERVICE, serviceTime, flow_id, receiver, None, None, content, prev_hop)
                compSpot.scheduler._taskQueue.append(aTask)
                self.controller.get_content(node, content)
                compSpot.admit_content_request_task(Ipfs.CONTENT_SERVICE, content, time, flow_id, deadline, receiver, rtt_delay, self.controller, self.debug)    

            elif node == source:
                delay = self.view.path_delay(node, prev_hop)
                self.controller.forward_content_hop(node, prev_hop)
                self.controller.add_event(time+delay, receiver, content, prev_hop, flow_id, deadline, rtt_delay, RESPONSE, None)
            else:
                raise ValueError("This should not happen in process_event() of Ipfs strategy")
        # Ipfs
        elif status == RESPONSE:
            if node != receiver:
                # lookup and delete pit state and forward Data
                results = self.pit.lookup(node, content)
                if results is None:
                    # this can happen now that insert_with_hint is used. Aggregation is disabled and multiple responses may be received for the same content.
                    return
                for pit_record in results:
                    prev_hop = pit_record.prev_hop
                    flow_id = pit_record.flow_id 
                    receiver = pit_record.receiver
                    server = pit_record.destination
                    arr_time = pit_record.time
                    rtt = time - arr_time
                    m_record = Measurement_Record(time, rtt)
                    self.measurement.add(node, server, m_record)
                    delay = self.view.path_delay(node, prev_hop)
                    self.controller.forward_content_hop(node, prev_hop)
                    self.controller.add_event(time+delay, receiver, content, prev_hop, flow_id, deadline, rtt_delay, RESPONSE, None)
                self.pit.remove(node, content)
                if self.view.has_cache(node):
                    self.controller.put_content(node, content)
            else: # response came back to the original sender
                results = self.pit.lookup(node, content)
                if results is None or len(results) == 0:
                    # this can happen now that insert_with_hint is used. Aggregation is disabled and multiple responses may be received for the same content.
                    session = self.consumer_session[receiver]
                    session.deliver(time, content, flow_id)
                    self.controller.put_content(receiver, content)
                    return
                pit_record = results[0]
                rtt = time - pit_record.time
                server = pit_record.destination
                self.pit.remove(node, content)

                m_record = Measurement_Record(time, rtt)
                self.measurement.add(node, server , m_record)
                session = self.consumer_session[receiver]
                session.deliver(time, content, flow_id)
                self.controller.put_content(receiver, content)
        # Ipfs
        elif status == TASK_COMPLETE:
            # route current request back to the receiver
            compSpot = self.view.compSpot(node)
            self.controller.complete_task(task, time)
            prev_hop = task.prev_hop
            flow_id = task.flow_id 
            receiver = task.receiver
            delay = self.view.path_delay(node, prev_hop)
            self.controller.forward_content_hop(node, prev_hop)
            self.controller.add_event(time+delay, receiver, content, prev_hop, flow_id, deadline, rtt_delay, RESPONSE, None)

            # schedule next task if any 
            newTask = compSpot.scheduler.schedule(time)
            if newTask is not None:
                self.controller.add_event(newTask.completionTime, newTask.receiver, newTask.content, node, newTask.flow_id, newTask.expiry, newTask.rtt_delay, TASK_COMPLETE, newTask)

