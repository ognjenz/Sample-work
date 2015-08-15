#!/usr/bin/python
from __future__ import division
import sys

# Create list from Hive query results
lista = []
for line in sys.stdin:
    line = line.strip()
    line = line.split('\t')
    lista.append(line)

# List of combos
combos = list(set([el[5] for el in lista]))
# List of dates
dates = list(set([el[0] for el in lista]))

agg_data = {}

# Create dictionary categories
for date in dates:
    for combo in combos:
        # Form unique key
        unique_key = date+' '+combo

        agg_data[unique_key] = {'$ sa': 0,
                                'rec oid': [],
                                'sa oid': [],
                                'ref oid': [],
                                'cbk oid': []}

# Aggregate data
for row in lista:
    # 0 - date
    # 1 - event
    # 2 - sale_amount
    # 3 - recurring
    # 4 - order_id
    # 5 - combo

    # Form unique key
    unique_key = row[0]+' '+row[5]

    if row[1]=='sale':
        agg_data[unique_key]['$ sa'] += float(row[2])
        if int(row[3])==1:
            agg_data[unique_key]['rec oid'].append(row[4])
        agg_data[unique_key]['sa oid'].append(row[4])
    elif row[1]=='refund':
        agg_data[unique_key]['ref oid'].append(row[4])
    elif row[1]=='chargeback':
        agg_data[unique_key]['cbk oid'].append(row[4])

# Calculate averages
for date in dates:
    for combo in combos:
        # Form unique key
        unique_key = date+' '+combo

        num_sa = len(set(agg_data[unique_key]['sa oid'])) # number of sales
        num_ref = len(set(agg_data[unique_key]['ref oid'])) # number of refunds
        num_cbk = len(set(agg_data[unique_key]['cbk oid'])) # number of chargebacks
        num_rec = len(set(agg_data[unique_key]['rec oid'])) # number of recurring

        # Average sale, refund, chargeback amounts
        if num_sa>0:
            agg_data[unique_key]['avg sa $'] = round(agg_data[unique_key]['$ sa']/num_sa,2)
        else:
            agg_data[unique_key]['avg sa $'] = 0
        if num_rec>0:
            agg_data[unique_key]['recurring take rate'] = round(100*num_rec/num_sa,2)
        else:
            agg_data[unique_key]['recurring take rate'] = 0

        # Assign values to variable names
        avg_sa = agg_data[unique_key]['avg sa $']
        rec_tr = agg_data[unique_key]['recurring take rate']

        # Combine variables to one string
        final_string = '\t'.join([date, combo, str(num_sa), str(num_ref),
                                  str(num_cbk), str(avg_sa), str(rec_tr)])

        # Append processed data to final list
        print final_string