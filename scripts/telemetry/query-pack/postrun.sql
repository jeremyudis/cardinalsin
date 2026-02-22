minute_rollup|SELECT date_trunc('minute', timestamp) AS minute, metric_name, avg(value) AS avg_value FROM metrics WHERE timestamp > now() - interval '60 minutes' GROUP BY minute, metric_name ORDER BY minute DESC LIMIT 1000
peak_values|SELECT metric_name, max(value) AS max_value FROM metrics WHERE timestamp > now() - interval '24 hours' GROUP BY metric_name ORDER BY max_value DESC LIMIT 50
signal_presence|SELECT metric_name, count(*) AS samples FROM metrics WHERE timestamp > now() - interval '24 hours' GROUP BY metric_name ORDER BY samples DESC LIMIT 100
