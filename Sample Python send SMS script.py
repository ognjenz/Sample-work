# -*- coding: utf-8 -*-
"""
Created on Mon May 18 10:26:27 2015

@author: Ognjen Zelenbabic
email: ognjen.zelenbabic@gmail.com
"""

from __future__ import division
import MySQLdb as mdb

from email.MIMEMultipart import MIMEMultipart
from email.MIMEText import MIMEText
import smtplib

# -----------------------------------------------------------------------------
# THIS VERSION COMPARES ONLY YESTERDAY AND DAY BEFORENUMBER OF SALES,
# REFUNDS AND CHARGEBACKS
# -----------------------------------------------------------------------------

# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
# Since S2P does not use translation tables, logic will have to be updated
# after bringing s2p home
# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

# -----------------------------------------------------------------------------
# Initial setup
# -----------------------------------------------------------------------------
# On which phones to send sms
ph_1 = '*****'
ph_2 = '*****'
ph_3 = '*****'

# Combine phones
phone_g1 = ','.join([ph_1,ph_2,ph_3])
phone = '+381649171027' # Ja

# Connect to db - PostSale (master)
db_ps = mdb.connect(host='*****',
                    port=3306,
                    user='ognjen',
                    passwd='*****',
                    db='*****')

def send_sms(phone,text_msg):
    fromaddr ='*****'
    toaddr = '*****'
    msg = MIMEMultipart()
    msg['From'] = fromaddr
    msg['To'] = toaddr
    msg['Subject'] = ''
    
    body = """\nUser: *****
    Password: *****
    Api_ID: *****
    Reply: notifications@rovicom.net    
    To: %s
    Text: %s"""%(phone,text_msg)
    
    msg.attach(MIMEText(body,'plain'))
    server = smtplib.SMTP_SSL('smtp.gmail.com', 465)
    server.login('*****', '*****')
    text=msg.as_string()
    server.sendmail(fromaddr,toaddr,text)
    
def count_events(shift):
    # Set cursor to db
    cursor = db_ps.cursor()
    
    # SQL query
    query = """select date(oi.event_date), substring(order_id, 1, 4) code, 
       substring(cast(uo.tid_cmp as char), 1, 2) brandID, pp.processor, 
       ev.event, count(distinct order_id)
       from user_orders uo left join _processors pp on uo.pay_processor = pp.id,
           order_items oi, _events ev, _skus sk, _statuses st, users us
       where uo.id = oi.user_order_id
       and oi.event_id = ev.id
       and oi.sku_id = sk.id
       and st.status_id = uo.status
       and uo.user_id = us.id
       and oi.event_date >= date_sub(date(now()), interval %i day)
       and oi.event_date < date_sub(date(now()), interval %i day)
       and (ev.event in ('PAY', 'CHB', 'REF', 'sale', 'refund', 'chargeback') or 
           (ev.event = 'Payment' and st.status_name = 'success'))
       and us.email not like '%%@rovicom.net'
       group by date(oi.event_date), substring(order_id, 1, 4), 
           substring(cast(uo.tid_cmp as char), 1, 2), 
           pp.processor, ev.event"""%(shift+1, shift)
    
    # Execute query
    cursor.execute(query)
    
    # Fetch results
    data = [list(row) for row in cursor.fetchall()]
        
    # Close cursor
    cursor.close()        
        
    # Fetch all code names
    names = list(set([el[1] for el in data if el[3]=='safecart']))
    
    # Initialize dict to store data
    grupped = {}
    for name in names:
        grupped[name] = {'sale': 0, 'refund': 0, 'chargeback': 0}
    
    # Group events per code names for different payment processors
    for row in data:
        if row[3] == 'safecart':
            grupped[row[1]][row[4]] += row[5]
        elif row[3] == 'cleverbridge':
            if row[4] == 'PAY':
                grupped[translate_id(row[1])]['sale'] += row[5]
            elif row[4] == 'REF':
                grupped[translate_id(row[1])]['refund'] += row[5]
            elif row[4] == 'CHB':
                grupped[translate_id(row[1])]['chargeback'] += row[5]
        elif row[3] == 'smart2pay':
            grupped[translate_id(row[1])]['sale'] += row[5]

    # Remove unwanted code name
    if 'CALL' in grupped.keys():
        del grupped['CALL']
    
    return grupped

def translate_id(brandID):
    if brandID in [21, 22, 23]:
        return 'SPCT'
    elif brandID == 13:
        return 'PCRL'
    else:
        return 'UPRO'

def form_message(yday, bday):
    # yday - yesterday
    # bday - day before
    # wday - week before
    
    # Which names exist?
    names = yday.keys()
    # Initialize message
    msg = "Per Brand || "
    
    for name in names:
        # X sales (compared to yesterday/compared to week ago)
        msg += name + ": " + str(yday[name]['sale']) + " SA (" 
        try:
            num = int(round(100*(yday[name]['sale']/bday[name]['sale']-1)))
            msg += '%+i%%); '%(num)
        except:
            msg += "-); "

        # X refunds (compared to yesterday/compared to week ago)
        msg += str(yday[name]['refund']) + " RF ("
        try:
            num = int(round(100*(yday[name]['refund']/bday[name]['refund']-1)))
            msg += '%+i%%); '%(num)
        except:
            msg += "-); "

        # X chargebacks (compared to yesterday/compared to week ago)
        msg += str(yday[name]['chargeback']) + " CBK ("
        try:
            num = int(round(100*(yday[name]['chargeback']/bday[name]['chargeback']-1)))
            msg += '%+i%%); '%(num)
        except:
            msg += "-); "

    # Remove trailing chars
    msg = msg.rstrip()
    msg = msg.rstrip(';')

    return msg

if __name__ == '__main__':
    # Fetch data for - yesterday
    yesterday = count_events(0)

    # Fetch data for - day before
    day_before = count_events(1)

    # Create SMS message text
    msg = form_message(yesterday, day_before)
    
    # Send SMS to phone group 1 & 2
    send_sms(phone_g1, msg)
    
    # TESTING    
    #send_sms(phone, msg)
    #print msg