# Task 1 – Twitter Geo-Spatial Analysis

Objective:  
Analyze geo-tagged tweets to identify regional and time-based tweet patterns.

**Dataset:**

**Name:** U.S. Twitter Data  
**Source:** Provided via ECS765P module  
**Format:** CSV (`twitter.csv`)  
**Key Columns:**
- `longitude`: Tweet longitude
- `latitude`: Tweet latitude
- `timestamp`: Tweet creation time (Unix format)
- `timezone`: Timezone offset

**🛠️ Key Steps:**  
- Loaded tweet dataset with PySpark.
- Extracted hour and weekday from timestamp.
- Categorized time of day: Morning, Afternoon, Evening, Night.
- Visualized tweet density by region and hour.

**🧰 Tools Used:**  
- PySpark
- Matplotlib
- Pandas (for plotting)


