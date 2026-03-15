-- =========================================================================
-- ADVANCED SQL QUERIES
-- =========================================================================

-- ---------------------------------------------------------
-- 1. Top 10 symbols by open interest (OI) change across exchanges
-- ---------------------------------------------------------
WITH DailyOI AS (
        SELECT 
            symbol,
            exchange,
            -- Use strptime to parse the '09-AUG-2019' format, then cast to DATE
            strptime(timestamp, '%d-%b-%Y')::DATE as trade_date,
            SUM(open_int) as total_oi,
            LAG(SUM(open_int)) OVER (
                PARTITION BY symbol, exchange 
                ORDER BY strptime(timestamp, '%d-%b-%Y')::DATE
            ) as prev_oi
        FROM raw_trades
        GROUP BY symbol, exchange, strptime(timestamp, '%d-%b-%Y')::DATE
    )
    SELECT 
        symbol,
        exchange,
        MAX(ABS(total_oi - prev_oi)) as max_daily_oi_change
    FROM DailyOI
    WHERE prev_oi IS NOT NULL
    GROUP BY symbol, exchange
    ORDER BY max_daily_oi_change DESC
    LIMIT 10;

/* Sample output
	SYMBOL	exchange	max_daily_oi_change
0	IDEA	NSE	349608000.0
1	YESBANK	NSE	101435400.0
2	SBIN	NSE	74850000.0
3	RELIANCE	NSE	59653000.0
4	INFY	NSE	59565600.0
5	INFY	BSE	58531200.0
6	IDFCFIRSTB	NSE	57960000.0
7	BHEL	NSE	54615000.0
8	GOLD	MCX	54432000.0
9	GMRINFRA	NSE	53505000.0
*/

-- ---------------------------------------------------------
-- 2. Volatility analysis: 7-day rolling std dev of close prices for NIFTY options
-- ---------------------------------------------------------
SELECT 
        strptime(timestamp, '%d-%b-%Y')::DATE as trade_date,
        strike_pr,
        option_typ,
        close,
        STDDEV(close) OVER (
            PARTITION BY strike_pr, option_typ 
            ORDER BY strptime(timestamp, '%d-%b-%Y')::DATE 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) as rolling_7d_stddev
    FROM raw_trades
    WHERE symbol = 'NIFTY' AND instrument LIKE '%OPT%'
    ORDER BY strike_pr, option_typ, strptime(timestamp, '%d-%b-%Y')::DATE
    LIMIT 10;

/* Sample Output
trade_date	STRIKE_PR	OPTION_TYP	CLOSE	rolling_7d_stddev
0	2019-08-01	4600.0	CE	3955.65	NaN
1	2019-08-01	4600.0	CE	3683.20	136.225833
2	2019-08-01	4600.0	CE	3820.25	95.742258
3	2019-08-02	4600.0	CE	3683.20	121.844089
4	2019-08-02	4600.0	CE	3955.65	113.876260
5	2019-08-02	4600.0	CE	3820.25	111.228267
6	2019-08-05	4600.0	CE	3683.20	122.610773
7	2019-08-05	4600.0	CE	3955.65	122.610773
8	2019-08-05	4600.0	CE	3820.25	103.133156
9	2019-08-06	4600.0	CE	3955.65	122.523446
*/

-- ---------------------------------------------------------
-- 3. Cross-exchange comparison: Avg settle_pr for gold futures (MCX) vs. equity index futures (NSE)
-- ---------------------------------------------------------

SELECT 
        strptime(timestamp, '%d-%b-%Y')::DATE as trade_date,
        AVG(CASE WHEN exchange = 'MCX' AND symbol = 'GOLD' THEN close END) as avg_gold_mcx_settle,
        AVG(CASE WHEN exchange = 'NSE' AND instrument LIKE '%FUT%' THEN close END) as avg_eq_index_nse_settle
    FROM raw_trades
    GROUP BY strptime(timestamp, '%d-%b-%Y')::DATE
    HAVING AVG(CASE WHEN exchange = 'MCX' AND symbol = 'GOLD' THEN close END) IS NOT NULL
    ORDER BY strptime(timestamp, '%d-%b-%Y')::DATE
    LIMIT 10;

/* Sample Output
rade_date	avg_gold_mcx_settle	avg_eq_index_nse_settle
0	2019-08-01	93.890000	1882.931979
1	2019-08-02	59.476923	1889.637500
2	2019-08-05	162.294444	1873.796138
3	2019-08-06	153.360000	1897.701046
4	2019-08-07	106.486538	1877.593021
5	2019-08-08	84.073214	1897.370937
6	2019-08-09	90.104545	1914.375521
7	2019-08-13	100.768182	1877.774271
8	2019-08-14	104.192857	1890.743854
9	2019-08-16	146.039583	1896.716701
*/

-- ---------------------------------------------------------
-- 4. Option chain summary: Grouped by expiry_dt and strike_pr, calculating implied volume
-- ---------------------------------------------------------
SELECT 
        strptime(expiry_dt, '%d-%b-%Y')::DATE as expiry_date,
        strike_pr,
        SUM(CASE WHEN option_typ = 'CE' THEN CONTRACTS ELSE 0 END) as ce_implied_volume,
        SUM(CASE WHEN option_typ = 'PE' THEN CONTRACTS ELSE 0 END) as pe_implied_volume,
        SUM(CONTRACTS) as total_implied_volume
    FROM raw_trades
    WHERE option_typ IN ('CE', 'PE')
    GROUP BY strptime(expiry_dt, '%d-%b-%Y')::DATE, strike_pr
    ORDER BY strptime(expiry_dt, '%d-%b-%Y')::DATE, strike_pr
    LIMIT 10;

/* Sample Output
expiry_date	STRIKE_PR	ce_implied_volume	pe_implied_volume	total_implied_volume
0	2019-08-01	9600.0	0.0	10.0	10.0
1	2019-08-01	9650.0	0.0	0.0	0.0
2	2019-08-01	9700.0	0.0	63.0	63.0
3	2019-08-01	9750.0	0.0	0.0	0.0
4	2019-08-01	9800.0	0.0	16.0	16.0
5	2019-08-01	9850.0	0.0	0.0	0.0
6	2019-08-01	9900.0	0.0	5.0	5.0
7	2019-08-01	9950.0	0.0	41.0	41.0
8	2019-08-01	10000.0	19.0	1394.0	1413.0
9	2019-08-01	10050.0	0.0	285.0	285.0
*/

-- ---------------------------------------------------------
-- 5. Performance-optimized query for max volume in last 30 days
-- ---------------------------------------------------------

WITH RankedVolume AS (
        SELECT 
            symbol,
            CONTRACTS as volume_contracts,
            strptime(timestamp, '%d-%b-%Y')::DATE as trade_date,
            RANK() OVER (PARTITION BY symbol ORDER BY CONTRACTS DESC) as vol_rank
        FROM raw_trades
        WHERE strptime(timestamp, '%d-%b-%Y')::DATE >= (SELECT MAX(strptime(timestamp, '%d-%b-%Y')::DATE) - INTERVAL 30 DAY FROM raw_trades)
    )
    SELECT 
        symbol,
        volume_contracts as max_volume,
        trade_date
    FROM RankedVolume
    WHERE vol_rank = 1
    ORDER BY max_volume DESC
    LIMIT 10;

/* Sample Output
	SYMBOL	max_volume	trade_date
0	BANKNIFTY	4564524	2019-11-14
1	NIFTY	1274605	2019-11-07
2	YESBANK	215823	2019-10-31
3	INFY	84779	2019-10-22
4	SBIN	83986	2019-10-25
5	BHARTIARTL	66343	2019-10-24
6	TATAMOTORS	64754	2019-10-29
7	IBULHSGFIN	62762	2019-10-17
8	RELIANCE	62144	2019-10-29
9	ICICIBANK	54549	2019-11-08
*/
