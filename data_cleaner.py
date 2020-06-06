# -*- coding: utf-8 -*-
"""
Created on Fri Jun  5 14:04:52 2020

@author: chris
"""

import pandas as pd
#pd.set_option("display.max_rows", 5)

#this is an example using the data from texas. any state's data can be
#plugged in to the appropriate place to get a cleaned up version.

texas = pd.read_csv('DISP-texas.csv')
texas.cost = texas.cost.replace(to_replace='\$|\,', value='', regex=True)
texas.cost = pd.to_numeric(texas.cost)

item_costs_by_dept = texas.groupby(['department','name']).cost.agg([sum]).round(2)
number_of_items_by_dept = texas.groupby(['department','name']).name.count()
texas_new = item_costs_by_dept.join(number_of_items_by_dept)

print(texas_new)
texas_new.to_csv('texas.csv')

dept_total_costs = texas.groupby('department').cost.agg([sum])
print(dept_total_costs)
dept_total_costs.to_csv('texas-totals.csv')
