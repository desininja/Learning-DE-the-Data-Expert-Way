-- What is the average number of web events of a session from a user on Tech Creator?



-- This query answers: "What is the average number of web events of a session from a user on Tech Creator?"
-- It calculates the average of the 'num_hits' column, which represents the number of events in each session.
-- The query is filtered to only include hosts ending in '.techcreator.io'.

SELECT
    AVG(num_hits) AS average_events_per_session
FROM
    sessionized_aggregated_events
WHERE
    host LIKE '%.techcreator.io';


-- Compare results between different hosts (zachwilson.techcreator.io, zachwilson.tech, lulu.techcreator.io)


-- This query helps to: "Compare results between different hosts"
-- For each specified host, it calculates:
--   - average_events_per_session: The average number of events within a single session.
--   - total_sessions: The total number of distinct user sessions.
--   - total_hits: The overall total number of events.
-- This allows for a comprehensive comparison of user activity across these different websites.

SELECT
    host,
    AVG(num_hits) AS average_events_per_session,
    COUNT(*) AS total_sessions,
    SUM(num_hits) AS total_hits
FROM
    sessionized_aggregated_events
WHERE
    host IN ('zachwilson.techcreator.io', 'zachwilson.tech', 'lulu.techcreator.io')
GROUP BY
    host
ORDER BY
    host;
