# -*- coding: utf-8 -*-
"""
@author: Ognjen Zelenbabic
email: ognjen.zelenbabic@rovicom.net
Created on Wed Nov 26 10:16:37 2014
"""

from __future__ import division, unicode_literals
from datetime import date
from time import clock
import MySQLdb as mdb
import os, xlsxwriter
os.system('cls')

# -----------------------------------------------------------------------------
# INICIJALIZACIJA PARAMETARA
# -----------------------------------------------------------------------------
# Start measuring time
t0 = clock()

# Connect to internal MMP database
conn = mdb.connect(host='*****',
                     port=3306,
                     user='ognjen',
                     passwd='*****',
                     db='*****')

# Define path to the file
path = 'J:\\Reports\\Other\\Business reports\\'
# Get current date
curr_date = str(date.today())
# Form name for the file
f_name = 'Business report - ' + curr_date + '.xlsx'

# -----------------------------------------------------------------------------
# PRIPREMA PODATAKA
# -----------------------------------------------------------------------------
def top_partners(limit=500000, days=40):
    # Set cursor to db
    cursor = conn.cursor()
    
    # SQL query
    query = """select distinct CompanyName
        from mmp_table
        where `Date` >= date_sub(date(now()), interval %s day)
        group by CompanyName
        having sum(Install) >= %s"""%(str(days),str(limit))
    
    # Execute query
    cursor.execute(query)
    
    # Fetch rows
    rows = cursor.fetchall()
    
    # Convert fetched rows to list
    results = [row[0] for row in rows]
    
    # Return results
    return results

def per_partner(partner_name, days=40):
    # Set cursor to db
    cursor = conn.cursor()
    
    # SQL query - columns have following meaning:
    # 0 - Date
    # 1 - Weekday
    # 2 - Brand
    # 3 - DL start
    # 4 - DL end
    # 5 - DL fail rate
    # 6 - Install fail rate
    # 7 - Install
    # 8 - External install
    # 9 - Sales
    # 10 - Refunds
    # 11 - CBK's
    # 12 - Account Manager Name

    query = """select date(`Date`), dayofweek(date(`Date`)) 'Weekday', 
        Brand, sum(Download) 'DL start', sum(DownloadEnd) 'DL end', 
        round(100*(1-sum(DownloadEnd)/sum(Download)),2) 'DL fail rate',
        round(100*(1-sum(Install)/sum(DownloadEnd)),2) 'Inst fail rate', 
        sum(Install) 'Inst', sum(ExtInstall) 'Ext inst', sum(NetSales) 'Sales',
        sum(Refunds) 'Refunds', sum('\#CB') 'CBK\\'s', AccountManagerName
        from mmp_table
        where `Date` >= date_sub(date(now()), interval %s day)
        and CompanyName = '%s'
        and Brand = 'OptimizerPro'
        and substr(substr(DealName, instr(DealName, ' ')+1), 1,
        instr(substr(DealName, instr(DealName, ' ')+1), ' ')-1) <> '0.000'
        group by date(`Date`)"""%(str(days),str(partner_name))
    
    # Execute query
    cursor.execute(query)
    
    # Fetch rows
    rows = cursor.fetchall()

    # Convert fetched rows to list
    #results = [row for row in rows]
    results = [[str(row[0]), int(row[1]), row[2], int(row[3]), int(row[4]),
                float(row[5]), float(row[6]), int(row[7]), int(row[8]),
                int(row[9]), int(row[10]), int(row[11]), row[12]]
        for row in rows]
    
    # Return results
    return results

def top_countries(partner_name, num=3, days=40):
    # Set cursor to db
    cursor = conn.cursor()
    
    # SQL query
    query = """select Country
        from mmp_table
        where `Date` >= date_sub(date(now()), interval %s day)
        and CompanyName = '%s'
        and substr(substr(DealName, instr(DealName, ' ')+1), 
        1,instr(substr(DealName, instr(DealName, ' ')+1), ' ')-1) <> '0.000'
        and Country <> 'XX'
        group by Country
        order by sum(Install) desc
        limit %s"""%(str(days),partner_name,str(num))
    
    # Execute query
    cursor.execute(query)
    
    # Fetch rows
    rows = cursor.fetchall()
    
    # Convert fetched rows to list
    results = [row[0] for row in rows]
    
    # Return results
    return results

def date_ranges(data):
    # Extract dates and week numbers from data
    dates = [[row[0], row[1]] for row in data]
    
    # Variable to store ranges
    ranges = []
    # Temporary variable
    temp = []
    # Variable to count ranges
    i = 0
    
    # We assume that all dates are sorted
    # Which date is first
    for datum in dates:
        # If this date is monday
        if datum[1]==2:
            # Write first date
            temp = [datum[0]]
        # If the date is sunday and the first day is already written
        elif datum[1]==1 and len(temp)>0:
            # Increment number of ranges
            i += 1
            # Write end of the week
            ranges.append([temp[0],datum[0],i])
            # Clear temporary variable
            temp = []
    
    # Initialize dictionary to store aggregated data
    agg_data = {}
    
    # For each date range 
    for i in range(len(ranges)):
        # Create dictionary categories
        agg_data[ranges[i][2]] = {'date start': ranges[i][0],
                               'date end': ranges[i][1],
                               'DL start': 0,
                               'DL end': 0,
                               'DL fail rate': 0,
                               'Install fail rate': 0,
                               'Total fail rate': 0,
                               'Install': 0,
                               'External install': 0,
                               'Sales': 0,
                               'Refunds': 0,
                               'CBK\'s': 0,
                               '% ref': 0,
                               '% cbk': 0,
                               'CR': 0,
                               'Account Manager': ''}
    
    return agg_data

def check_range(date, ranges):
    # For each range
    for key in ranges.keys():
        # If given date belongs to this range
        if ranges[key]['date start'] <= date <= ranges[key]['date end']:
            # Return the range number
            return key

def aggregate_data(ranges, data):
    # For each entry 
    for row in data:
        # Check to which date range it belongs to
        range_num = check_range(row[0],ranges)
        
        # If current date fits into one of existing ranges
        if range_num:
            # Add DL start
            ranges[range_num]['DL start'] += row[3]
            # Add DL end
            ranges[range_num]['DL end'] += row[4]
            # Calculate DL fail rate
            ranges[range_num]['DL fail rate'] = 1-\
                ranges[range_num]['DL end']/ranges[range_num]['DL start']
            # Add Install
            ranges[range_num]['Install'] += row[7]
            # Calculate install fail rate
            ranges[range_num]['Install fail rate'] = 1-\
                ranges[range_num]['Install']/ranges[range_num]['DL end']
            # Calculate total fail rate
            ranges[range_num]['Total fail rate'] = 1-\
                ranges[range_num]['Install']/ranges[range_num]['DL start']
            # Add External install
            ranges[range_num]['External install'] += row[8]
            # Add Sale
            ranges[range_num]['Sales'] += row[9]
            # Add Refunds
            ranges[range_num]['Refunds'] += row[10]
            # Add CBK's
            ranges[range_num]['CBK\'s'] += row[11]
            # Calculate refund percentage
            ranges[range_num]['% ref'] = \
                ranges[range_num]['Refunds']/ranges[range_num]['Sales']
            # Calculate chargeback percentage
            ranges[range_num]['% cbk'] = \
                ranges[range_num]['CBK\'s']/ranges[range_num]['Sales']
            # Calculate conversion rate
            ranges[range_num]['CR'] = \
                ranges[range_num]['Sales']/ranges[range_num]['Install']
            # If there is no Account Manager name
            if not ranges[range_num]['Account Manager']:
                # Write it
                ranges[range_num]['Account Manager'] = row[12]

    # Return result
    return ranges

def calculate_deltas(i,r1,r2,form1):
    # DL start
    sh.write(5+i,1, '=B%s/B%s-1'%(r1,r2), form1)
    # DL end
    sh.write(5+i,2, '=C%s/C%s-1'%(r1,r2), form1)
    # Inst
    sh.write(5+i,3, '=D%s/D%s-1'%(r1,r2), form1)
    # Ext inst
    sh.write(5+i,4, '=E%s/E%s-1'%(r1,r2), form1)
    # DL fail rate
    sh.write(5+i,5, '=F%s-F%s'%(r1,r2), form1)
    # Inst fail rate
    sh.write(5+i,6, '=G%s-G%s'%(r1,r2), form1)
    # Total fail rate
    sh.write(5+i,7, '=H%s-H%s'%(r1,r2), form1)
    # Sales
    sh.write(5+i,8, '=I%s/I%s-1'%(r1,r2), form1)
    # Refunds
    sh.write(5+i,9, '=J%s/J%s-1'%(r1,r2), form1)
    # CBK's
    sh.write(5+i,10, '=IF(K%s<>0, K%s/K%s-1, "-")'%(r2,r1,r2), form1)
    # % ref
    sh.write(5+i,11, '=L%s-L%s'%(r1,r2), form1)
    # % cbk
    sh.write(5+i,12, '=M%s-M%s'%(r1,r2), form1)
    # Change num format temporarily    
    form1.set_num_format('0.000%')
    # CR
    sh.write(5+i,13, '=N%s-N%s'%(r1,r2), form1)

# -----------------------------------------------------------------------------
# CALL FUNCTIONS
# -----------------------------------------------------------------------------
# Fetch partner names
partners = top_partners()

# For test purposes choos one partner
partner = partners[2]

# Which are the top countries for given partner
countries = top_countries(partner)

# Fetch all data for a given partner name
data = per_partner(partner)

# Calculate date ranges
ranges = date_ranges(data)

# Aggregate data
agg_data = aggregate_data(ranges,data)
        

# Initialize workbook object
wb = xlsxwriter.Workbook(path+f_name)
# Initialize sheet
sh = wb.add_worksheet(partner)

# Define format for table margins
f1 = wb.add_format({'align': 'center',
                    'bold': True,
                    'valign': 'vcenter',
                    'bg_color': '#404040',
                    'font_color': '#FFFFFF',
                    'border': 1,
                    'border_color': '#C0C0C0'})
                    
# Define format for table body - integers
f2 = wb.add_format({'align': 'center',
                    'valign': 'vcenter',
                    'border': 1})
                    
# Define format for table body - percentages
f3 = wb.add_format({'align': 'center',
                    'valign': 'vcenter',
                    'border': 1})
f3.set_num_format('0.00%')

# Define format for table body - CR
f4 = wb.add_format({'align': 'center',
                    'valign': 'vcenter',
                    'border': 1})
f4.set_num_format('0.000%')

# Define format for table body - percentages (Δ from previous week)
f5 = wb.add_format({'align': 'center',
                    'valign': 'vcenter',
                    'border': 1,
                    'bg_color': '#CCE5FF',
                    'bold': True,
                    'num_format': '0.00%'})

# Define format for table body - percentages (Δ from previous week) CR
f6 = wb.add_format({'align': 'center',
                    'valign': 'vcenter',
                    'border': 1,
                    'bg_color': '#CCE5FF',
                    'bold': True,
                    'num_format': '0.000%'})

# Define format for table body - percentages (Δ from average)
f7 = wb.add_format({'align': 'center',
                    'valign': 'vcenter',
                    'border': 1,
                    'bg_color': '#FFFF66',
                    'bold': True,
                    'num_format': '0.00%',
                    'font_size': 14})

# Merge a range of cells
sh.merge_range(0,0,0,13, partner, f1)
sh.merge_range(1,0,1,13, agg_data[1]['Account Manager'], f2)

# Write table header
sh.write(3,0, 'Date range', f1)
sh.write(3,1, 'DL start', f1)
sh.write(3,2, 'DL end', f1)
sh.write(3,3, 'Inst', f1)
sh.write(3,4, 'Ext Inst', f1)
sh.write(3,5, 'DL fail rate', f1)
sh.write(3,6, 'Inst fail rate', f1)
sh.write(3,7, 'Total fail rate', f1)
sh.write(3,8, 'Sales', f1)
sh.write(3,9, 'Refunds', f1)
sh.write(3,10, 'CBK\'s', f1)
sh.write(3,11, '% ref', f1)
sh.write(3,12, '% cbk', f1)
sh.write(3,13, 'CR', f1)

# For each date range
for i in agg_data.keys():
    # Form date range
    date_range = agg_data[i]['date start']+' - '+agg_data[i]['date end']
    # Write aggregated data
    sh.write(3+i,0, date_range, f1)
    sh.write(3+i,1, agg_data[i]['DL start'], f2)
    sh.write(3+i,2, agg_data[i]['DL end'], f2)
    sh.write(3+i,3, agg_data[i]['Install'], f2)
    sh.write(3+i,4, agg_data[i]['External install'], f2)
    # Change num format temporarily
    f2.set_num_format('0.00%')
    sh.write(3+i,5, agg_data[i]['DL fail rate'], f3)
    sh.write(3+i,6, agg_data[i]['Install fail rate'], f3)
    sh.write(3+i,7, agg_data[i]['Total fail rate'], f3)
    sh.write(3+i,8, agg_data[i]['Sales'], f2)
    sh.write(3+i,9, agg_data[i]['Refunds'], f2)
    sh.write(3+i,10, agg_data[i]['CBK\'s'], f2)
    sh.write(3+i,11, agg_data[i]['% ref'], f3)
    sh.write(3+i,12, agg_data[i]['% cbk'], f3)
    sh.write(3+i,13, agg_data[i]['CR'], f4)

# Write margin names for table calculations
sh.write(4+i,0, 'Average', f1)
sh.write(5+i,0, 'Δ from previous week', f1)
sh.write(6+i,0, 'Δ from average', f1)

# ------ Average -------
# DL start
sh.write(4+i,1, '=AVERAGE(B5:B%s)'%(str(i+3)), f2)
# DL end
sh.write(4+i,2, '=AVERAGE(C5:C%s)'%(str(i+3)), f2)
# Inst
sh.write(4+i,3, '=AVERAGE(D5:D%s)'%(str(i+3)), f2)
# Ext inst
sh.write(4+i,4, '=AVERAGE(E5:E%s)'%(str(i+3)), f2)
# DL fail rate
sh.write(4+i,5, '=1-C%s/B%s'%(str(i+5),str(i+5)), f3)
# Inst fail rate
sh.write(4+i,6, '=1-D%s/C%s'%(str(i+5),str(i+5)), f3)
# Total fail rate
sh.write(4+i,7, '=1-D%s/B%s'%(str(i+5),str(i+5)), f3)
# Sales
sh.write(4+i,8, '=AVERAGE(I5:I%s)'%(str(i+3)), f2)
# Refunds
sh.write(4+i,9, '=AVERAGE(J5:J%s)'%(str(i+3)), f2)
# CBK's
sh.write(4+i,10, '=AVERAGE(K5:K%s)'%(str(i+3)), f2)
# % ref
sh.write(4+i,11, '=J%s/I%s'%(str(i+5),str(i+5)), f3)
# % cbk
sh.write(4+i,12, '=K%s/I%s'%(str(i+5),str(i+5)), f3)
# CR
sh.write(4+i,13, '=I%s/D%s'%(str(i+5),str(i+5)), f4)

# ------ Δ from previous week ------
calculate_deltas(i,str(i+4),str(i+3),f5)

# ------ Δ from average ------
calculate_deltas(i+1,str(i+4),str(i+5),f7)

# Close workbook
wb.close()

# Close connection
conn.close()
# Print end
print '>>> Finished -> %.2f'%(clock()-t0)