#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Wed Jul 19 21:27:46 2017
This file defines tools for NILM projects
@author: BohaoHuang
"""

import time
from datetime import datetime

def datestring2ts(s, fmt='%d/%m/%Y'):
    """
    Transform a date string into unix timestam
    """
    return time.mktime(datetime.strptime(s, fmt).timetuple())