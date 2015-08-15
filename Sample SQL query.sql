SELECT
     DISTINCT from_unixtime(floor(unix_timestamp(oi.event_date)/900)*900) as main_time,
     CASE WHEN (uo.tid_cmp = '' OR ((uo.tid_cmp / 10000000) < 1)) THEN NULL ELSE uo.tid_cmp END AS tracker_id,
     CASE WHEN uo.tid_cou = '' THEN NULL ELSE uo.tid_cou END AS tracker_country,
     CASE WHEN uo.tid_dst = '' THEN NULL ELSE uo.tid_dst END AS tracker_distribution,
     CASE WHEN us.country = '' THEN NULL ELSE us.country END AS ip_geo,
     em.type event,
     pr.processor,
     CASE WHEN uo.combo = '' THEN NULL ELSE uo.combo END AS main_combo,
     CASE WHEN uo.customer_lang = '' THEN NULL ELSE uo.customer_lang END AS main_customer_lang,
     CASE WHEN pm.type = '' THEN NULL ELSE pm.type END AS main_pay_method,
     CASE WHEN uo.affiliate = '' THEN NULL ELSE uo.affiliate END AS main_affiliate,
     CASE WHEN oi.currency = '' THEN NULL ELSE oi.currency END AS main_currency,
     IF(oi.currency IN ("", "USD"), SUM(oi.sale_amount), ROUND(SUM(ra.rate * oi.sale_amount), 4)) AS sale_amount,
     IF(oi.currency IN ("", "USD"), SUM(oi.tax_amount), ROUND(SUM(ra.rate * oi.tax_amount), 4)) AS tax_amount,
     IF(oi.currency IN ("", "USD"), SUM(oi.payout_amount), SUM(oi.payout_amount)) AS payout_amount,
     COUNT(DISTINCT order_id) num
     
    FROM postsale_redesign.order_items oi
    LEFT JOIN postsale_redesign._events ev ON oi.event_id = ev.id
    LEFT JOIN postsale_redesign.user_orders uo ON oi.user_order_id = uo.id
    LEFT JOIN postsale_redesign.users us ON uo.user_id = us.id
    LEFT JOIN postsale_redesign._processors pr ON uo.pay_processor = pr.id
    LEFT JOIN postsale_redesign.payment_methods pm ON uo.pay_method = pm.value and pm.processor = uo.pay_processor
    LEFT JOIN postsale_redesign.events_map em ON ev.event = em.value and em.processor = uo.pay_processor
    LEFT JOIN postsale_redesign.rates ra ON oi.currency = ra.currency_from AND DATE(oi.event_date) = ra.pars_date
    WHERE
     oi.event_date >= "2015-06-30 00:00:00" AND
     oi.event_date < "2015-07-01 00:00:00" AND
     us.email NOT LIKE "%@adsology.com" AND
     us.email NOT LIKE "%@cleverbridge.com" AND
     us.email NOT LIKE "%@rovicom.net" AND
     us.email NOT LIKE "%@revenuewire.com"
    GROUP BY main_time, tracker_id, tracker_country, tracker_distribution, ip_geo, em.type, pr.processor, main_combo, 
        main_customer_lang, main_pay_method, main_affiliate, main_currency