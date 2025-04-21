# Task 4 – HDFS Log Streaming

**Objective:**  
Build a real-time streaming pipeline to monitor HDFS system logs.

**📊 Dataset:**

**Name:** HDFS Log Events  
**Source:** Provided via ECS765P module  
**Format:** Structured logs (streamed input)  
**Fields:**
- `Timestamp`
- `Log Level` (`INFO`, `WARN`, `ERROR`)
- `Component`
- `Block ID`
- `Source IP:Port`, `Destination IP:Port`
- `Block Size`
- `Message`

**Preprocessing:**
- Parsed log fields using Spark schema
- Applied watermarking and windowing
- Filtered log levels and component names

** 🛠️ Key Steps:**

- Created structured streaming pipeline
- Used time windows with watermarking
- Analyzed DataNode activity
- Aggregated by host and message type


** 🧰 Tools Used:**

- PySpark (Structured Streaming)
- Spark SQL
- Console sink (for debugging)
