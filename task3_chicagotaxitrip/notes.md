# Task 3 – Chicago Taxi Trip Graph

**Objective:**  
Model and analyze city-wide mobility patterns using graph-based methods.

**📊 Dataset:**

**Name:** Chicago Taxi Trips  
**Source:** Provided via ECS765P module  
**File:** `chicago taxi trips.csv`

**Key Columns:**
- `Trip Start Timestamp`, `Trip End Timestamp`
- `Pickup Community Area`, `Dropoff Community Area`
- `Trip Miles`, `Fare`, `Tips`, `Trip Total`
- `Pickup Centroid Latitude/Longitude`
- `Dropoff Centroid Latitude/Longitude`

**🛠️ Key Steps:**

- Created graph using GraphFrames
- Applied PageRank and BFS for shortest paths
- Normalized edge weights
- Filtered connected nodes and analyzed urban flow

**🧰 Tools Used:**

- PySpark
- GraphFrames
- Matplotlib


