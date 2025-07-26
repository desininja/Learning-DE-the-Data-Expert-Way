# Flink Web Traffic Sessionization

This project uses an Apache Flink job to process a stream of web traffic events. It groups events into user sessions based on a 5-minute inactivity gap and stores the aggregated results in a PostgreSQL database. SQL queries are then used to analyze this session data.

-----

## ⚙️ Setup and Execution

Follow these steps to set up the database, run the Flink job, and execute the analysis.

### 1\. Database Schema

The Flink job requires a sink table in your PostgreSQL database. Create it using the following SQL command:

```sql
CREATE TABLE sessionized_aggregated_events (
    event_hour TIMESTAMP(3),
    ip VARCHAR,
    host VARCHAR,
    num_hits BIGINT
);
```

### 2\. File Placement

Place the Flink job script `homework_job.py` inside the `/src/job/` directory within the project structure.

### 3\. Makefile Configuration

Add the following command to your `Makefile` to create a target for running the new job:

```makefile
homework_job:
    docker compose exec jobmanager ./bin/flink run \
        -py /opt/src/job/homework_job.py \
        --pyFiles /opt/src \
        -d
```

### 4\. Running the Job

Execute the following commands from your terminal to build the environment and run the jobs.

```bash
# Stop and remove existing containers, if any
make down

# Build and start the new Docker Compose environment
make up

# Start the initial 'process_events' job (if required)
make job

# Run the sessionization homework job
make homework_job
```

-----

## 📊 SQL Analysis and Results

The following queries were run against the `sessionized_aggregated_events` table to analyze user behavior.

### Average Session Events on Tech Creator

This query calculates the average number of web events per session for all hosts under the `techcreator.io` domain.

**Query:**

```sql
SELECT
    AVG(num_hits) AS average_events_per_session
FROM
    sessionized_aggregated_events
WHERE
    host LIKE '%.techcreator.io';
```

**Result:**
The average user session on Tech Creator websites consists of approximately **2.45** web events.

| average\_events\_per\_session |
| :------------------------- |
| 2.4545454545454545         |

### Host-Specific Engagement Comparison

This query compares user engagement across `zachwilson.techcreator.io`, `zachwilson.tech`, and `lulu.techcreator.io`.

**Query:**

```sql
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
```

**Result:**
The analysis shows a single session for `zachwilson.techcreator.io` with 2 total hits. Data for the other hosts was not present in the results.

| host                      | average\_events\_per\_session | total\_sessions | total\_hits |
| :------------------------ | :------------------------- | :------------- | :--------- |
| zachwilson.techcreator.io | 2.0000000000000000         | 1              | 2          |