-- Top selectors by pool_missing.
SELECT selector, target, token_pair, dex_kind, fee_tier,
       SUM(pool_missing_count) AS pool_missing,
       SUM(partial_pool_missing_count) AS partial_pool_missing,
       SUM(pool_found_count) AS pool_found
FROM selector_pool_performance_rollups
GROUP BY selector, target, token_pair, dex_kind, fee_tier
ORDER BY pool_missing DESC
LIMIT 50;

-- Top selectors by pool_found.
SELECT selector, target, token_pair, pool, dex_kind, fee_tier,
       SUM(pool_found_count) AS pool_found,
       SUM(partial_pool_found_count) AS partial_pool_found,
       SUM(payload_built_count) AS payload_built
FROM selector_pool_performance_rollups
GROUP BY selector, target, token_pair, pool, dex_kind, fee_tier
ORDER BY pool_found DESC
LIMIT 50;

-- Top selectors by shadow_ev_positive.
SELECT selector, target, token_pair, pool, dex_kind, fee_tier,
       SUM(shadow_ev_positive_count) AS shadow_ev_positive,
       SUM(partial_shadow_ev_positive_count) AS partial_shadow_ev_positive,
       SUM(expected_profit_sum) / NULLIF(SUM(samples), 0) AS avg_expected_profit
FROM selector_pool_performance_rollups
GROUP BY selector, target, token_pair, pool, dex_kind, fee_tier
ORDER BY shadow_ev_positive DESC
LIMIT 50;

-- Conversion: partial -> pool_found -> shadow_ev_positive -> replay_candidate.
SELECT selector, target,
       SUM(partial_entered_payload_builder_count) AS partial_entered_payload_builder,
       SUM(partial_pool_discovery_attempted_count) AS partial_pool_discovery_attempted,
       SUM(partial_pool_found_count) AS partial_pool_found,
       SUM(partial_shadow_ev_positive_count) AS partial_shadow_ev_positive,
       SUM(partial_replay_candidate_created_count) AS partial_replay_candidate_created,
       ROUND(100.0 * SUM(partial_pool_found_count) / NULLIF(SUM(partial_pool_discovery_attempted_count), 0), 2) AS pool_found_pct,
       ROUND(100.0 * SUM(partial_shadow_ev_positive_count) / NULLIF(SUM(partial_pool_found_count), 0), 2) AS ev_positive_pct,
       ROUND(100.0 * SUM(partial_replay_candidate_created_count) / NULLIF(SUM(partial_shadow_ev_positive_count), 0), 2) AS replay_created_pct
FROM selector_pool_performance_rollups
GROUP BY selector, target
ORDER BY partial_pool_discovery_attempted DESC
LIMIT 50;

-- Selectors with much pool_missing and no pool_found.
SELECT selector, target, token_pair, dex_kind, fee_tier,
       SUM(pool_missing_count) AS pool_missing,
       SUM(partial_pool_discovery_attempted_count) AS partial_attempted
FROM selector_pool_performance_rollups
GROUP BY selector, target, token_pair, dex_kind, fee_tier
HAVING SUM(pool_missing_count) >= 10 AND SUM(pool_found_count) = 0
ORDER BY pool_missing DESC
LIMIT 50;

-- Selectors with pool_found but EV always negative.
SELECT selector, target, token_pair, pool, dex_kind, fee_tier,
       SUM(pool_found_count) AS pool_found,
       SUM(shadow_ev_negative_count) AS shadow_ev_negative,
       SUM(shadow_ev_positive_count) AS shadow_ev_positive,
       SUM(expected_profit_sum) / NULLIF(SUM(samples), 0) AS avg_expected_profit
FROM selector_pool_performance_rollups
GROUP BY selector, target, token_pair, pool, dex_kind, fee_tier
HAVING SUM(pool_found_count) > 0 AND SUM(shadow_ev_positive_count) = 0
ORDER BY shadow_ev_negative DESC, pool_found DESC
LIMIT 50;
