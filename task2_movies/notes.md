# Task 2 – MovieLens Ratings Analysis

**Objective:**  
Explore user behavior, movie popularity, and rating patterns across time.

---

**Dataset:**

**Name:** MovieLens 20M Dataset  
**Source:** Provided via ECS765P module  
**Files Used:**
- `movies.csv`: Movie ID, title, genres
- `ratings.csv`: User ID, movie ID, rating, timestamp

**Key Columns:**
- `movieId`, `title`, `genres`
- `userId`, `rating`, `timestamp`

**Preprocessing:**
- Joined movie and ratings data
- Converted timestamps to date
- Extracted year and month of ratings
- Categorized rating levels (Low, Medium, High)

---

## 🛠️ Key Steps

- Analyzed genre frequency and rating distribution
- Grouped ratings by time and genre
- Visualized with pie and bar charts

---

## 🧰 Tools Used

- PySpark
- SQL functions (groupBy, join)
- Matplotlib

