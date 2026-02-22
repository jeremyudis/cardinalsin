metric_recent_avg|SELECT metric_name, avg(value) AS avg_value FROM metrics WHERE timestamp > now() - interval '5 minutes' GROUP BY metric_name ORDER BY avg_value DESC LIMIT 25
metric_recent_p95|SELECT metric_name, percentile_cont(0.95) WITHIN GROUP (ORDER BY value) AS p95_value FROM metrics WHERE timestamp > now() - interval '5 minutes' GROUP BY metric_name ORDER BY p95_value DESC LIMIT 25
recent_error_like_signals|SELECT timestamp, metric_name, value FROM metrics WHERE timestamp > now() - interval '5 minutes' AND metric_name LIKE '%error%' ORDER BY timestamp DESC LIMIT 200
