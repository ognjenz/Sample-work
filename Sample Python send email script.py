# -*- coding: utf-8 -*-
"""
Created on Tue May 05 11:55:45 2015

@author: Administrator
"""
from __future__ import division
from email.MIMEMultipart import MIMEMultipart
from email.MIMEText import MIMEText
import MySQLdb as mdb, smtplib

# -----------------------------------------------------------------------------
# Initial setup
# -----------------------------------------------------------------------------
# Setup email user and pwd
gmail_user = '*****'
gmail_pwd = '*****'

# Who is receiving mail
to = ['ognjen.zelenbabic@gmail.com', 'another_email@gmail.com']


# Format names to be used in email header
emails = ', '.join(to)

# Connect to DWH DB
db = mdb.connect(host='*****',
                 port=3306,
                 user='*****',
                 passwd='*****',
                 db='*****')

# Connect to db - DB 2
db_ps = mdb.connect(host='*****',
                    port=3306,
                    user='*****',
                    passwd='*****',
                    db='*****')

# -----------------------------------------------------------------------------
# Functions for Overall SMS
# -----------------------------------------------------------------------------
def install(db,shift):
    # Set cursor to db
    cursor = db.cursor()
    
    # SQL query
    query = """select sum(fact_du400_cnt)
        from facts_view
        where date(`start`) = date_sub(date(now()), interval %s day)"""%(shift)

    # Execute query
    cursor.execute(query)
    
    # Fetch results
    num = cursor.fetchone()[0]
    
    # Close cursor
    cursor.close()
    
    # Return number of installs
    return num

def sale(db,shift):
    # Set cursor to db
    cursor = db.cursor()
    
    # SQL query
    query = """select count(distinct order_id)
        from order_items oi, _events ev, user_orders uo, users us
        where oi.event_date >= date_sub(date(now()), interval %s day)
        and oi.event_date < date_sub(date(now()), interval %s day)
        and oi.event_id = ev.id
        and oi.user_order_id = uo.id
        and uo.user_id = us.id
        and ev.event in ('sale', 'PAY')
        and us.email not like '%%@rovicom.net'"""%(shift,shift-1)

    # Execute query
    cursor.execute(query)
    
    # Fetch results
    num = cursor.fetchone()[0]
    
    # Close cursor
    cursor.close()
    
    # Return number of installs
    return num

def ref_and_cbk(db_ps,shift,cond1,cond2):
    # Set cursor to db
    cursor = db_ps.cursor()
    
    # SQL query
    query = """select count(distinct order_id)
        from order_items oi, _events ev, user_orders uo, users us, events_map em
        where date(oi.event_date) = date_sub(date(now()), interval %s day)
        and oi.event_id = ev.id
        and oi.user_order_id = uo.id
        and uo.user_id = us.id
        and ev.event = em.value
        and us.email not like '%%@rovicom.net'
        %sand em.type = 'refund'
        %sand em.type = 'chargeback'"""%(shift,cond1,cond2)

    # Execute query
    cursor.execute(query)
    
    # Fetch results
    num = cursor.fetchone()[0]
    
    # Close cursor
    cursor.close()
    
    # Return number of installs
    return num

def rebills(db_ps,shift):
    # Set cursor to db
    cursor = db_ps.cursor()
    
    # SQL query
    query = """select count(distinct uo.order_id)
        from order_items oi, _events ev, user_orders uo, users us
        where date(oi.event_date) = date_sub(date(now()), interval %s day)
        and oi.event_id = ev.id
        and oi.user_order_id = uo.id
        and uo.user_id = us.id
        and ev.event = 'rebill'
        and us.email not like '%%@rovicom.net'"""%(shift)
    
    # Execute query
    cursor.execute(query)
    
    # Fetch results
    num = cursor.fetchone()[0]
    
    # Close cursor
    cursor.close()
    
    # Return number of installs
    return num

def sales_amount(db_ps,cond1,cond2):
    # Set cursor to db
    cursor = db_ps.cursor()
    
    # SQL query
    query = """select sum(payout_amount)
        from order_items oi, _events ev, user_orders uo, users us
        where date(oi.event_date) = date_sub(date(now()), interval 1 day)
        and oi.event_id = ev.id
        and oi.user_order_id = uo.id
        %sand ev.event in ('sale', 'PAY')
        %sand ev.event in ('sale', 'PAY', 'rebill')
        and uo.user_id = us.id
        and us.email not like '%%@rovicom.net'"""%(cond1,cond2)

    # Execute query
    cursor.execute(query)
    
    # Fetch results
    num = cursor.fetchone()[0]
    
    # Close cursor
    cursor.close()
    
    # Return number of installs
    return num

# -----------------------------------------------------------------------------
# Get numbers for Overall SMS
# -----------------------------------------------------------------------------
# Get yesterday's number of installs
inst_curr_day = install(db,1)

# Day before
inst_day_before = install(db,2)
# Calculate day ratio
if inst_day_before!=0:
    inst_day_ratio = 100*(inst_curr_day/inst_day_before-1)

# Week before
inst_week_before = install(db,8)
# Calculate week ratio
if inst_week_before!=0:
    inst_week_ratio = 100*(inst_curr_day/inst_week_before-1)

# -----------------------------------------------------------------------------
# Get yesterday's number of sales
sa_curr_day = sale(db_ps,1)

# Day before
sa_day_before = sale(db_ps,2)
# Calculate day ratio
if sa_day_before!=0:
    sa_day_ratio = 100*(sa_curr_day/sa_day_before-1)

# Week before
sa_week_before = sale(db_ps,8)
# Calculate week ratio
if sa_week_before!=0:
    sa_week_ratio = 100*(sa_curr_day/sa_week_before-1)

# -----------------------------------------------------------------------------
# Calculate yesterday's conversion rate
try:
    cr_curr_day = round(100*sa_curr_day/inst_curr_day,2)
except:
    cr_curr_day = 0

# Day before
try:
    cr_day_before = round(100*sa_day_before/inst_day_before,2)
except:
    cr_day_before = 0
    
# Week before
try:
    cr_week_before = round(100*sa_week_before/inst_week_before,2)
except:
    cr_week_before = 0

# -----------------------------------------------------------------------------
# Get yesterday's number of refunds
ref_curr_day = ref_and_cbk(db_ps,1,'','#')
# Week before
ref_week_before = ref_and_cbk(db_ps,8,'','#')
# Calculate week ratio
if ref_week_before!=0:
    ref_week_ratio = 100*(ref_curr_day/ref_week_before-1)

# -----------------------------------------------------------------------------
# Get yesterday's number of chargebacks
cbk_curr_day = ref_and_cbk(db_ps,1,'#','')
# Week before
cbk_week_before = ref_and_cbk(db_ps,8,'#','')
# Calculate week ratio
if cbk_week_before!=0:
    cbk_week_ratio = 100*(cbk_curr_day/cbk_week_before-1)

# -----------------------------------------------------------------------------
# Get yesterday's rebills
rbl_curr_day = rebills(db_ps,1)

# Get yesterday's sale amount with rebills
sa_rbl_curr_day = sales_amount(db_ps,'#','')
# Get yesterday's sale amount without rebills
sa_am_curr_day = sales_amount(db_ps,'','#')

# Add message header
msg = 'Overall || '

# Installs yesterday (day before/week before)
msg += 'Inst: %i (%+i%%/%+i%%); '%(inst_curr_day, inst_day_ratio, inst_week_ratio)

# Sales yesterday (day before/week before)
msg += 'Sa: %i (%+i%%/%+i%%)(%i); '%(sa_curr_day, sa_day_ratio, sa_week_ratio, sa_week_before)

# Conversion rate yesterday (day before/week before)
msg += 'CR: %.2f%% (%.2f%%/%.2f%%); '%(cr_curr_day, cr_day_before, cr_week_before)

# Refunds yesterday (week before)
if ref_week_before !=0:
    msg += 'Rf: %i (%+i%%); '%(ref_curr_day,ref_week_ratio)
else:
    msg += 'Rf: %i (-); '%(ref_curr_day)

# Chargebacks yesterday (week before)
if cbk_week_before!=0:
    msg += 'Cbk: %i (%+i%%); '%(cbk_curr_day,cbk_week_ratio)
else:
    msg += 'Cbk: %i (-); '%(cbk_curr_day)

# Rebills yesterday
msg += 'Rbl: %i; '%(rbl_curr_day)
# Net sales with/without rebills yesterday
msg += '$sa+rbl: $%i; $sa: $%i'%(sa_rbl_curr_day, sa_am_curr_day)

# Form first part of the message
email_msg = [msg]

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
#        elif row[3] == 'smart2pay':
#            grupped[translate_id(row[1])]['sale'] += row[5]

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

# Fetch data for - yesterday
yesterday = count_events(0)

# Fetch data for - day before
day_before = count_events(1)

# Create SMS message text
msg = form_message(yesterday, day_before)

# Add second part of the message
email_msg.append(msg)

def aggregate_events(shift):
    # Set cursor to db
    cursor = db_ps.cursor()
    
    # SQL query
    query = """select pp.processor, em.type, count(distinct order_id), sum(payout_amount)
        from user_orders uo left join _processors pp on uo.pay_processor = pp.id,
        order_items oi, _events ev, _skus sk, _statuses st, events_map em, users us
        where uo.id = oi.user_order_id
        and oi.event_id = ev.id
        and oi.sku_id = sk.id
        and uo.user_id = us.id
        and ev.event = em.value
        and st.status_id = uo.status
        and oi.event_date >= date_sub(date(now()), interval %i day)
        and oi.event_date < date_sub(date(now()), interval %i day)
        and us.email not like '%%@rovicom.net'
        and em.type in ('sale', 'rebill', 'refund', 'chargeback')
        group by pp.processor, em.type"""%(shift+1, shift)

    # Execute query
    cursor.execute(query)
    
    # Fetch results
    data = [list(row) for row in cursor.fetchall()]
        
    # Close cursor
    cursor.close()
    
    # Fetch all shopping cart names
    names = list(set([el[0] for el in data]))

    # Initialize dictionary to store results
    per_cart = {}
    for name in names:
        per_cart[name] = {'# sales': 0,
                          '$ sales': 0,
                          '# rebills': 0,
                          '$ rebills': 0,
                          '# refunds': 0,
                          '# cbks': 0}

    # Aggregate data
    for row in data:
        if row[1] == 'sale':
            per_cart[row[0]]['# sales'] = row[2]
            per_cart[row[0]]['$ sales'] = row[3]
        elif row[1] == 'rebill':
            per_cart[row[0]]['# rebills'] = row[2]
            per_cart[row[0]]['$ rebills'] = row[3]
        elif row[1] == 'refund':
            per_cart[row[0]]['# refunds'] = row[2]
        elif row[1] == 'chargeback':
            per_cart[row[0]]['# cbks'] = row[2]

    # Form message
    msg = "Per Cart || "
    for name in per_cart.keys():
        if name == 'safecart':
            msg += 'SC: SA %i/%i$; '%(per_cart[name]['# sales'], per_cart[name]['$ sales'])
            if per_cart[name]['# rebills'] == 0:
                msg += 'RBL -; '
            else:
                msg += 'RBL %i/%i$; '%(per_cart[name]['# rebills'], per_cart[name]['$ rebills'])
            try:
                ref_percent = int(100*per_cart[name]['# refunds']/per_cart[name]['# sales'])
                if per_cart[name]['# refunds'] == 0:
                    msg += 'RF -; '
                else:
                    msg += 'RF %i/%i%%; '%(per_cart[name]['# refunds'], ref_percent)
            except:
                msg += 'RF -; '
            try:
                cbk_percent = round(100*per_cart[name]['# cbks']/per_cart[name]['# sales'],1)
                if per_cart[name]['# cbks'] == 0:
                    msg += 'CBK -; '
                else:
                    msg += 'CBK %i/%.1f%%; '%(per_cart[name]['# cbks'], cbk_percent)
            except:
                msg += 'CBK -; '
            
        if name == 'cleverbridge':
            msg += 'CLB: SA %i/%i$; '%(per_cart[name]['# sales'], per_cart[name]['$ sales'])
            if per_cart[name]['# rebills'] == 0:
                msg += 'RBL -; '
            else:
                msg += 'RBL %i/%i$; '%(per_cart[name]['# rebills'], per_cart[name]['$ rebills'])
            try:
                ref_percent = int(100*per_cart[name]['# refunds']/per_cart[name]['# sales'])
                if per_cart[name]['# refunds'] == 0:
                    msg += 'RF -; '
                else:
                    msg += 'RF %i/%i%%; '%(per_cart[name]['# refunds'], ref_percent)
            except:
                msg += 'RF -; '
            try:
                cbk_percent = round(100*per_cart[name]['# cbks']/per_cart[name]['# sales'],1)
                if per_cart[name]['# cbks'] == 0:
                    msg += 'CBK -; '
                else:
                    msg += 'CBK %i/%.1f%%; '%(per_cart[name]['# cbks'], cbk_percent)
            except:
                msg += 'CBK -'
            
    return msg

# Fetch and aggregate data and form message
msg = aggregate_events(0)

# Add final part of the email message
email_msg.append(msg)

def send_email(text_msg,reciever):
    text_msg = ',\n\n'.join(text_msg)
    
    fromaddr = gmail_user
    msg = MIMEMultipart()
    msg['From'] = fromaddr
    msg['To'] = reciever
    msg['Subject'] = 'Daily SMSs in email'
    
    body = '\n'+text_msg
    
    msg.attach(MIMEText(body,'plain'))
    server = smtplib.SMTP_SSL('smtp.gmail.com', 465)
    server.login(gmail_user, gmail_pwd)
    text=msg.as_string()
    server.sendmail(fromaddr,reciever,text)

if __name__ == '__main__':
	# Send emails
	send_email(email_msg, emails)
	#print email_msg

	# Test
	#send_email(email_msg, 'ognjen.zelenbabic@rovicom.net')