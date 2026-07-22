# Real-Time Twitter Sentiment Analysis
### Scala · Apache Spark Structured Streaming · Apache Kafka

```
[Tweet Producer (Python)]
        │
        ▼  (messages)
[Apache Kafka]  ←── twitter-topic
        │
        ▼  (readStream)
[Spark Structured Streaming]
        │
        ▼  (keyword sentiment UDF)
[Console Output]
  Tweet: I love AI  →  Positive ✅
```

---

## 1. Folder Structure

```
twitter-sentiment/
├── producer/
│   └── tweet_producer.py          ← Fake tweet generator → Kafka
├── spark-streaming/
│   ├── build.sbt                  ← SBT dependencies (Spark + Kafka connector)
│   ├── project/
│   │   └── plugins.sbt            ← sbt-assembly plugin
│   └── src/main/scala/sentiment/
│       └── SentimentAnalysis.scala ← Spark Streaming + sentiment logic
├── scripts/
│   └── kafka_setup.sh             ← One-shot Kafka startup helper
└── README.md
```

---

## 2. Setup Instructions (Ubuntu / WSL)

### 2.1 Install Java 11

```bash
sudo apt update
sudo apt install -y openjdk-11-jdk

# Verify
java -version
# expected: openjdk version "11.x.x"
```

### 2.2 Install Scala & SBT

```bash
# Install Scala
sudo apt install -y scala

# Install SBT (Scala Build Tool)
echo "deb https://repo.scala-sbt.org/scalasbt/debian all main" \
  | sudo tee /etc/apt/sources.list.d/sbt.list
curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x2EE0EA64E40A89B84B2DF73499E82A75642AC823" \
  | sudo apt-key add -
sudo apt update
sudo apt install -y sbt

# Verify
sbt --version
```

### 2.3 Install Apache Spark 3.3.2

```bash
cd ~
wget https://archive.apache.org/dist/spark/spark-3.3.2/spark-3.3.2-bin-hadoop3.tgz
tar -xzf spark-3.3.2-bin-hadoop3.tgz
mv spark-3.3.2-bin-hadoop3 ~/spark
rm spark-3.3.2-bin-hadoop3.tgz
```

### 2.4 Install Apache Kafka 3.5.0

```bash
cd ~
wget https://archive.apache.org/dist/kafka/3.5.0/kafka_2.12-3.5.0.tgz
tar -xzf kafka_2.12-3.5.0.tgz
mv kafka_2.12-3.5.0 ~/kafka
rm kafka_2.12-3.5.0.tgz
```

### 2.5 Set Environment Variables

Add these lines to your `~/.bashrc` (or `~/.zshrc`):

```bash
# Java
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

# Spark
export SPARK_HOME=$HOME/spark
export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin

# Kafka
export KAFKA_HOME=$HOME/kafka
export PATH=$PATH:$KAFKA_HOME/bin
```

Apply the changes:

```bash
source ~/.bashrc
```

### 2.6 Install Python Kafka library

```bash
pip install kafka-python
# or: pip3 install kafka-python
```

---

## 3. Build the Spark JAR

```bash
cd ~/twitter-sentiment/spark-streaming
sbt assembly
```

This downloads dependencies and produces:
```
target/scala-2.12/TwitterSentimentAnalysis-assembly-1.0.jar
```

> First build takes 3-5 minutes (downloading from Maven). Subsequent builds are fast.

---

## 4. How to Run (3 Terminals)

### Terminal 1 — Start Kafka

```bash
cd ~/twitter-sentiment
bash scripts/kafka_setup.sh
```

This starts Zookeeper → waits → starts Kafka broker → creates `twitter-topic`.

**Or run manually step-by-step:**

```bash
# Start Zookeeper
$KAFKA_HOME/bin/zookeeper-server-start.sh $KAFKA_HOME/config/zookeeper.properties

# [New terminal] Start Kafka broker
$KAFKA_HOME/bin/kafka-server-start.sh $KAFKA_HOME/config/server.properties

# [New terminal] Create topic
$KAFKA_HOME/bin/kafka-topics.sh \
  --create \
  --topic twitter-topic \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1 \
  --if-not-exists

# Verify topic exists
$KAFKA_HOME/bin/kafka-topics.sh --list --bootstrap-server localhost:9092
```

### Terminal 2 — Run Tweet Producer

```bash
cd ~/twitter-sentiment
python3 producer/tweet_producer.py
```

You'll see:
```
============================================================
  Fake Tweet Producer → Kafka
  Topic : twitter-topic
  Broker: localhost:9092
============================================================
[✓] Connected to Kafka at localhost:9092
[   1] Sent → I love AI and machine learning so much!
[   2] Sent → This laptop is terrible, worst purchase I ever made
[   3] Sent → Just read an interesting article about space
...
```

### Terminal 3 — Run Spark Streaming Job

```bash
cd ~/twitter-sentiment/spark-streaming

spark-submit \
  --master local[*] \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2 \
  --class sentiment.SentimentAnalysis \
  target/scala-2.12/TwitterSentimentAnalysis-assembly-1.0.jar
```

---

## 5. Sample Console Output

```
╔══════════════════════════════════════════════════════╗
║   Real-Time Twitter Sentiment Analysis               ║
║   Kafka → Spark Structured Streaming → Console       ║
╚══════════════════════════════════════════════════════╝

[INFO] Listening to Kafka topic 'twitter-topic' ... (Ctrl+C to stop)

-------------------------------------------
Batch: 1
-------------------------------------------
+------------------------------------------------------------------+
|result                                                            |
+------------------------------------------------------------------+
|Tweet : I love AI and machine learning so much!  →  Positive ✅  |
|Tweet : This is an awesome day feeling great    →  Positive ✅   |
+------------------------------------------------------------------+

-------------------------------------------
Batch: 2
-------------------------------------------
+------------------------------------------------------------------+
|result                                                            |
+------------------------------------------------------------------+
|Tweet : This laptop is terrible worst purchase  →  Negative ❌   |
|Tweet : I hate traffic jams so much this is     →  Negative ❌   |
+------------------------------------------------------------------+

-------------------------------------------
Batch: 3
-------------------------------------------
+------------------------------------------------------------------+
|result                                                            |
+------------------------------------------------------------------+
|Tweet : Just read an interesting article space  →  Neutral  ➖   |
|Tweet : Spark is fantastic for big data         →  Positive ✅   |
+------------------------------------------------------------------+
```

---

## 6. Sentiment Logic Explained

Located in `SentimentAnalysis.scala`:

```scala
val positiveWords = Set("good", "happy", "love", "great", "awesome",
                        "excellent", "amazing", "fantastic", "wonderful", "best")
val negativeWords = Set("bad", "sad", "hate", "worst", "terrible",
                        "horrible", "awful", "disgusting", "poor", "ugly")

val classifyTweet = udf((text: String) => {
  val words      = text.toLowerCase.replaceAll("[^a-z\\s]", "").split("\\s+").toSet
  val posMatches = words.intersect(positiveWords).size
  val negMatches = words.intersect(negativeWords).size
  if (posMatches > negMatches)      "Positive ✅"
  else if (negMatches > posMatches) "Negative ❌"
  else                              "Neutral  ➖"
})
```

Steps:
1. Lowercase the tweet and strip punctuation
2. Split into individual words
3. Count matches against positive and negative word sets
4. Whichever count is higher wins; tie → Neutral

---

## 7. Troubleshooting

| Problem | Fix |
|---|---|
| `No brokers available` | Kafka isn't running. Run `scripts/kafka_setup.sh` first. |
| `ClassNotFoundException` | Re-run `sbt assembly` inside `spark-streaming/` |
| `Port 9092 already in use` | `sudo kill -9 $(lsof -ti:9092)` |
| `Port 2181 already in use` | `sudo kill -9 $(lsof -ti:2181)` |
| Spark logs too noisy | Already set to WARN. Run `export SPARK_LOG_LEVEL=WARN` |
| SBT download slow | First run downloads ~500MB. Wait or use a faster mirror. |

---

## 8. BONUS: The Big Picture

### Where is Kafka used?
Kafka acts as the **message broker / data bus**. The producer writes fake tweets to the `twitter-topic` partition. Kafka durably stores each message and lets Spark read them in order, at its own pace. This **decouples** the producer from the consumer — you can restart Spark without losing messages.

### Where is Spark used?
Spark Structured Streaming reads from Kafka as a **continuous stream of micro-batches** (every 5 seconds here). It applies a distributed UDF (classifyTweet) to each row across worker threads in parallel. In a real cluster this work would fan out across dozens of machines processing millions of tweets.

### What makes this "Big Data"?
Three properties — known as the **3 Vs**:

- **Volume** — Kafka can handle millions of messages/second; Spark can process them in parallel across a cluster of any size.
- **Velocity** — Data arrives and is processed in real-time (seconds, not hours).
- **Variety** — Raw, unstructured text is cleaned and classified on the fly with no manual ETL step.

The same code that runs on your laptop in `local[*]` mode would run on a 1,000-node cluster unchanged — that's the power of Spark's abstraction.

---

## 9. Stopping Everything

```bash
# Stop Spark job
Ctrl+C   (in Terminal 3)

# Stop Producer
Ctrl+C   (in Terminal 2)

# Stop Kafka
$KAFKA_HOME/bin/kafka-server-stop.sh

# Stop Zookeeper
$KAFKA_HOME/bin/zookeeper-server-stop.sh
```
