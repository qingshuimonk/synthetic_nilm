#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Wed Jul 19 17:08:12 2017
This file defines functions for NILM datasets' readers
@author: BohaoHuang
"""

import os
import pandas as pd
from datetime import datetime
import matplotlib.dates as dates
import matplotlib.pyplot as plt


class nilm_reader(object):
    
    def __init__(self, data_dir):
        self.data_dir = data_dir
        self.meters = {}
    
    def load_chan_list(self, app_name, ds_name='UKDALE'):
        """
        Returns corresponding meter numbers given appliance name
        For different channels with same name, it will return a list
        """
        chan_list = []
        if(ds_name=='UKDALE'):
            for line in open(os.path.join(self.data_dir, 'labels.dat')):
                if(line.strip('\n').split(' ')[1] == app_name or 
                   line.strip('\n').split(' ')[1][:-1] == app_name):
                    chan_list.append(int(line.strip('\n').split(' ')[0]))
        return(chan_list)
        
    
    def load_meter(self, app_name, ds_name='UKDALE'):
        """
        Take an appliance name, return a list of meters object
        Each meter object is a dictionary with three attributes, appliance name,
        channel number and data which is a pandas series
        """
        meter = {}
        if(ds_name=='UKDALE'):
            chan_list = self.load_chan_list(app_name, ds_name)
            for chan_num in chan_list:
                file_name = 'channel_%d.dat' % chan_num
                df = pd.read_csv(os.path.join(self.data_dir, file_name), sep=' ')
                df.dropna(axis=0)
                s = pd.Series(df.values[:,1], df.values[:,0])
                meter.update({chan_num:s})
        self.meters.update({app_name:meter})
        return
            
        
    def lookup_meter(self, meter_num):
        """
        Return appliance name for given meter number
        """
        for a_name in self.meters:
            for m_num in self.meters[a_name]:
                if(m_num==meter_num):
                    return(a_name)
            
        
    def meter_stats(self, app_name=None, meter_num=None):
        """
        Provide statistics of given meter, stats include start/end time
        If no app_name and meter_num specified, return all meter stats
        If only app_name is given, return meter stats with same name
        If only meter_num is given, lookup the corresponding meter
        """
        if app_name and meter_num:
            lb_val = self.meters[app_name][meter_num].index[0]
            ub_val = self.meters[app_name][meter_num].index[-1]
            lb = datetime.fromtimestamp(lb_val).strftime('%Y-%m-%d %H:%M:%S')
            ub = datetime.fromtimestamp(ub_val).strftime('%Y-%m-%d %H:%M:%S')
            return({meter_num:[lb, ub]})
        elif app_name:
            stats = {}
            for m_num in self.meters[app_name]:
                stats.update(self.meter_stats(app_name, m_num))
            return(stats)
        elif meter_num:
            a_name = self.lookup_meter(meter_num)
            return(self.meter_stats(a_name, meter_num))
        else:
            stats = {}
            for a_name in self.meters:
                for m_num in self.meters[a_name]:
                    stats.update(self.meter_stats(a_name, m_num))
            return(stats)
        
    def truncate_meter(self, lb, ub, app_name=None, meter_num=None):
        """
        Truncate meter to a given time window
        If no app_name and meter_num specified, truncate all meter stats
        If only app_name is given, truncate meter stats with same name
        If only meter_num is given, lookup the corresponding meter
        """
        if app_name and meter_num:
            self.meters[app_name][meter_num] = self.meters[app_name][meter_num][
                (self.meters[app_name][meter_num].index>=lb) &
                (self.meters[app_name][meter_num].index<=ub)]
        elif app_name:
            for m_num in self.meters[app_name]:
                self.truncate_meter(app_name, m_num)
        elif meter_num:
            a_name = self.lookup_meter(meter_num)
            self.truncate_meter(a_name, meter_num)
        else:
            for a_name in self.meters:
                for m_num in self.meters[a_name]:
                    self.truncate_meter(a_name, m_num)
                    
    def select_range(self, lb, ub, app_name, meter_num):
        """
        Select specific meter data in a given time 
        Unlike truncate_meter, this will NOT affect stored data
        """
        return(self.meters[app_name][meter_num][
               (self.meters[app_name][meter_num].index>=lb) & 
               (self.meters[app_name][meter_num].index<=ub)])
                    
    
    def plot_single_meter(self, ax, app_name, meter_num, lb, ub):
        """
        Meter plot helper function
        """
        meter2plt = self.select_range(lb, ub, app_name, meter_num)
        x_tics = [datetime.fromtimestamp(x).strftime('%Y-%m-%d %H:%M:%S') 
            for x in meter2plt.index]
        ax.plot_date(pd.DatetimeIndex(x_tics).to_pydatetime(), 
                     meter2plt, '-', label='%s %s'%(app_name, meter_num), 
                     alpha=0.5)
        plt.xticks(rotation=45)
        ax.xaxis.set_major_formatter(dates.DateFormatter('%Y-%m-%d'))
        
        
    def plot_meters(self, lb=0, ub=float('Inf'), app_name=None, meter_num=None):
        """
        Plot meter in a given time window
        If no app_name and meter_num specified, plot all meters
        If only app_name is given, plot meter
        If only meter_num is given, lookup the corresponding meter
        """
        fig, ax = plt.subplots()
        if app_name and meter_num:
            self.plot_single_meter(ax, app_name, meter_num, lb, ub)
        elif app_name:
            for m_num in self.meters[app_name]:
                self.plot_single_meter(ax, app_name, m_num, lb, ub)
        elif meter_num:
            a_name = self.lookup_meter(meter_num)
            self.plot_single_meter(ax, a_name, meter_num, lb, ub)
        else:
            for a_name in self.meters:
                for m_num in self.meters[a_name]:
                    self.plot_single_meter(ax, a_name, m_num, lb, ub)
        plt.tight_layout()
        
        
    def read_single_meter(self, app_name, meter_num, window, overlap):
        """
        Read single meter's data with a sliding window, return a generator
        """
        step = window - overlap
        for start in range(0, len(self.meters[app_name][meter_num].values) 
            - window + 1, step):
            chunk = self.meters[app_name][meter_num].values[start: start + window]
            yield chunk
            
    def read_batch(self, chunk, batch_size):
        """
        Push single meter generator in a batch, also return a generator
        """
        batch = []
        for element in chunk:
            batch.append(element)
            if len(batch) == batch_size:
                yield batch
                batch = []
        yield batch