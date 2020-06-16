# -*- coding: utf-8 -*-
"""
Created on Fri Jun  5 14:04:52 2020

@author: chris
"""

import pandas as pd
#pd.set_option("display.max_rows", 5)

input_folder_name = "input_data/"
output_folder_name = "output_data/"
test_data = ['test']
states=['alabama','alaska','arizona','arkansas','california','colorado',
        'connecticut','delaware','florida','georgia','idaho','illinois','indiana',
        'iowa','kansas','kentucky','louisiana','maine','maryland',
        'massachusetts','michigan','minnesota','mississippi','missouri',
        'montana','nebraska','nevada','new_hampshire','new_jersey',
        'new_mexico','new_york','north_carolina','north_dakota','ohio',
        'oklahoma','oregon','pennsylvania','rhode_island','south_carolina',
        'south_dakota','tennessee','texas','utah','vermont','virginia',
        'washington','west_virginia','wisconsin','wyoming']
#hawaii is missing because it was not included in the original dataset

class Purchased_Item:
    def __init__(self, name, amount, cost):
        self.name = name
        self.quantity = amount
        self.total_cost = cost
    def __str__(self):
        return "There are " + str(self.quantity) + " " + self.name + " at a total cost of $" + str(self.total_cost)
    def update_cost(self, cost, amount):
        self.total_cost += (cost * amount)
        return self.total_cost
    def update_quantity(self, amount):
        self.quantity += amount
    def get_name(self):
        return self.name

class Department:
    def __init__(self, name):
        self.name = name
        self.items_to_update = {}    
    def __str__(self):
        return str(self.items_to_update)

current_index = 0
#stores each department and where each individual item is located for a
#particular type of item in that department in the dataset (by index)
departments = {}

#iterates through each index of the dataframe and adds up the amount
#of and total cost of each type of item for each department in the data set
def read_data(item_name, department, cost, quantity):
    global current_index
    global rows_to_delete
    rename_to = 'none'
    current_dept = department[current_index]
    current_item = item_name
    if current_index != 0:
        if current_dept == department[current_index - 1]:
            for item in departments[current_dept].items_to_update:
                if departments[current_dept].items_to_update[item].get_name() == item_name:
                    departments[current_dept].items_to_update[item].update_quantity(quantity[current_index])
                    departments[current_dept].items_to_update[item].update_cost(cost[current_index], quantity[current_index])
                    current_index += 1
                    return rename_to
            departments[current_dept].items_to_update[current_item] = Purchased_Item(current_item, quantity[current_index], 0)
            departments[current_dept].items_to_update[current_item].update_cost(cost[current_index], quantity[current_index])
            current_index += 1
            return current_item
        else:
            new_dept = Department(current_dept)
            departments.update({current_dept : new_dept})
            departments[current_dept].items_to_update[current_item] = Purchased_Item(item_name[current_index], quantity[current_index], 0)
            departments[current_dept].items_to_update[current_item].update_cost(cost[current_index], quantity[current_index])
            current_index += 1
            return current_item
    else:
        new_dept = Department(current_dept)
        departments.update({current_dept: new_dept})
        departments[current_dept].items_to_update[current_item] = Purchased_Item(item_name, quantity[current_index], 0)
        departments[current_dept].items_to_update[current_item].update_cost(cost[current_index], quantity[current_index])
        current_index += 1
        return current_item
        
def update_attribute(series, attribute, department, item):
    global current_index
    if attribute == 'cost':
        if department[current_index] in departments:
            current_dept = departments[department[current_index]]
            if item[current_index] in current_dept.items_to_update:
                current_item = current_dept.items_to_update[item[current_index]]
                current_index += 1
                cost = current_item.total_cost
                return cost
            elif item[current_index]:
                current_index += 1
                return 0
    elif attribute == 'quantity':
        if department[current_index] in departments:
            current_dept = departments[department[current_index]]
            if item[current_index] in current_dept.items_to_update:
                current_item = current_dept.items_to_update[item[current_index]]
                current_index += 1
                return current_item.quantity
            elif item[current_index]:
                current_index += 1
                return 0
            
for state in test_data:
    input_file = pd.read_csv(input_folder_name + '1033-' + state + '.csv', 
                             index_col=False)
    item_quantities = input_file['Quantity']
    
    input_file.cost = input_file.cost.replace(to_replace='\$|\,', value='', regex=True)
    input_file.cost = pd.to_numeric(input_file.cost)
    current_index = 0
    new_data = input_file.item.apply(read_data, args=[input_file.department, 
                                               input_file.cost, item_quantities])  
    current_index = 0
    combined = input_file.join(new_data, lsuffix='_old', rsuffix='_new')
    current_index = 0
    combined.cost = combined.cost.apply(update_attribute, args=['cost', combined.department, combined.item_new])
    print(combined.cost)
    current_index = 0
    item_quantities = combined['Quantity']
    item_quantities = item_quantities.apply(update_attribute, args=['quantity', combined.department, combined.item_new])
    combined.drop_duplicates(subset='item_new', keep=False, inplace=True)
    output_file = pd.DataFrame({'item':combined.item_new, 
                               'quantity':item_quantities,
                               'cost':combined.cost})
    print(output_file.head())
    output_file.to_csv(output_folder_name + state + '.csv')
    #total cost for all equipment for each state's police departments
    dept_total_costs = input_file.groupby('department').cost.agg([sum])
    print(dept_total_costs)
    dept_total_costs.to_csv(output_folder_name + state + '-total.csv')