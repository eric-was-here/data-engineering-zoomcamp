# Week 1 Homework Answers

## Question 1
Answer: `--iidfile string`

### Commands
```bash
docker build --help | grep "Write the image ID to the file"
```

## Question 2
Answer: 3 (pip, setuptools, wheel)

### Commands
```bash
docker run -it \
    --entrypoint=bash \
    python:3.9
```
In Docker container:
```bash
pip list
```

## Question 3

Answer: 20530

### Query

```sql
SELECT count(*)
FROM green_taxi_trips
WHERE cast(lpep_pickup_datetime AS date) = '2019-01-15'
AND cast(lpep_dropoff_datetime AS date) = '2019-01-15'
```

## Question 4

Answer: "2019-01-15"

### Query

```sql
SELECT
	cast(lpep_pickup_datetime AS date) AS pickup_date,
	max(cast(trip_distance AS float)) AS max_distance
FROM green_taxi_trips
GROUP BY pickup_date
ORDER BY max_distance DESC
LIMIT 1
```

## Question 5

Answer: 2: 1282 ; 3: 254

### Query

```sql
SELECT
	SUM(two_passengers) AS two_passengers_total,
	SUM(three_passengers) AS three_passengers_total
FROM (
	SELECT
		CASE WHEN cast(passenger_count AS int) = 2
		THEN 1
		END AS two_passengers,
		CASE WHEN cast(passenger_count AS int) = 3
		THEN 1
		END AS three_passengers
	FROM green_taxi_trips
	WHERE cast(lpep_pickup_datetime AS date) = '2019-01-01'
) AS counted
```

## Question 6

Answer: Long Island City/Queens Plaza

### Query

```sql
SELECT
	MAX(CAST(tip_amount AS float)) as max_tip,
	zdo."Zone" AS dropoff
FROM green_taxi_trips AS gtt
LEFT JOIN zones AS zpu
ON cast(gtt."PULocationID" AS bigint) = zpu."LocationID"
LEFT JOIN zones AS zdo
ON cast(gtt."DOLocationID" AS bigint) = zdo."LocationID"
WHERE zpu."Zone" = 'Astoria'
GROUP BY dropoff
ORDER BY max_tip DESC
LIMIT 1
```
