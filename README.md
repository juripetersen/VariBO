# Training Queries

- [JOB Training (From lero-on-PostgreSQL)](https://github.com/AlibabaIncubator/Lero-on-PostgreSQL/blob/c22591ea962763d3eac11b56fc231a636ca337b6/lero/reproduce/training_query/job.txt)
- TPC-H Queries generated with [DataFarm](https://github.com/agora-ecosystem/data-farm) [here](./src/Queries/tpch/)

# Datasets

## IMDB Dataset (JOB-light)
The CSV files used in the paper, which are from May 2013, can be found [here](http://event.cwi.nl/da/job/imdb.tgz)

- Download the csv files
- Create a PostgreSQL database with name job
- Run the .sql scripts in [/src/Queries/imdb/scripts](./src/Queries/imdb/scripts/)
- JOB light queries 1-70 are located in [/src/Queries/imdb/](./src/Queries/imdb/)

## TPC-H Dataset
The CSV files used in the paper can be found [here](https://www.tpc.org/TPC_Documents_Current_Versions/download_programs/tools-download-request6.asp?bm_type=TPC-H&bm_vers=3.0.1&mode=CURRENT-ONLY).
The test queries run in our experiments are 900-999.

# Usage

## Training a model

**Parameters**

--model bvae, cost
<br>
--retrain 'path/to/data', (default) ' '
<br>
--epochs 10, (default) 100
<br>
--trials 15, (default) 25
<br>
--model-path 'path/to/store/model'
<br>
--parameters 'path/to/store/hyperparameters'

Example use:

```bash
python main.py --model bvae --model-path="src/Models/bvae.onnx" --parameters="./src/HyperaparameterLogs/BVAE.json" --trials 50 --epochs 250
```

## Running the LSBO loop

**Parameters**

--model bvae
<br>
--trainset 'path/to/data', (default) ' '
<br>
--epochs 10, (default) 100 (only needed when retraining enabled)
<br>
--model-path 'path/to/store/model'
<br>
--parameters 'path/to/store/hyperparameters'
<br>
--query 'path/to/query' or query number for TPC-H, default 1
<br>
--stats 'path/to/store/statistics', default ''
<br>
--time 10, default 0 - time in minutes
<br>
--steps 1000, default 0 - max. candidates to be tried during exploration
<br>
--improvement 90, default 0 - min. improvement threshhold to stop
exploration
<br>
--acqf (random|ts|ei), default 'ts' - acquisition function to optimize
<br>
--zdim 128 - required latent space size of model
<br>
--exec 'path/to/wayang-submit' - path to wayang executable

Example use:


```bash
python init_lsbo.py --model bvae --model-path="src/Models/bvae.onnx" --parameters="./src/HyperaparameterLogs/BVAE.json" --query="./src/Queries/imdb/1.sql" --steps 1000 --zdim 128
```


## Running benchmarks in Apache Wayang
The scripts used for running any of the experiments can be found on [GitHub](https://github.com/Mikkel-MJ/incubator-wayang-thesis/tree/main/bin/runners/benchmarks):
- [TPC-H](https://github.com/Mikkel-MJ/incubator-wayang-thesis/blob/main/bin/runners/benchmarks/generatables-runner.sh)
- [JOB-light](https://github.com/Mikkel-MJ/incubator-wayang-thesis/blob/main/bin/runners/benchmarks/imdb-runner.sh)
- [LSBO exploration](https://github.com/Mikkel-MJ/incubator-wayang-thesis/blob/main/bin/runners/benchmarks/lsbo-runner.sh)
- [Training data generation](https://github.com/Mikkel-MJ/incubator-wayang-thesis/blob/main/bin/runners/benchmarks/imdb-encode-runner.sh)

These scripts have to be modified in order to run on your setup, as they
are fit for our experiments on the SDU Ucloud HPC.

Apache Wayang can either be installed and run as described on the
[official website](https://wayang.apache.org/docs/guide/installation) or
in a [docker cluster](https://github.com/juripetersen/wayang_docker/blob/main/docker-compose.yml), simulating both Apache Spark and Apache Flink
clusters and a PostgreSQL database.

**Configuration**
Three main classes in Apache Wayang are used to execute our experiments:
- [JOBenchmark.java](https://github.com/Mikkel-MJ/incubator-wayang-thesis/blob/main/wayang-plugins/wayang-ml/src/main/java/org/apache/wayang/ml/benchmarks/JOBenchmark.java)
- [GeneratableBenchmarks.java](https://github.com/Mikkel-MJ/incubator-wayang-thesis/blob/main/wayang-plugins/wayang-ml/src/main/java/org/apache/wayang/ml/benchmarks/GeneratableBenchmarks.java)
- [LSBORunner.java](https://github.com/Mikkel-MJ/incubator-wayang-thesis/blob/main/wayang-plugins/wayang-ml/src/main/java/org/apache/wayang/ml/benchmarks/LSBORunner.java)

All of these hold configurations for Apache Spark and Apache Flink
clusters and also the PostgreSQL database. These configurations can be
modified to fit your needs in the respective files:

```java
public static String psqlUser = "ucloud";
    public static String psqlPassword = "ucloud";

    public static void main(String[] args) {
        List<Plugin> plugins = JavaConversions.seqAsJavaList(Parameters.loadPlugins(args[0]));
        Configuration config = new Configuration();

        final String calciteModel = "{\n" +
                "    \"version\": \"1.0\",\n" +
                "    \"defaultSchema\": \"wayang\",\n" +
                "    \"schemas\": [\n" +
                "        {\n" +
                "            \"name\": \"postgres\",\n" +
                "            \"type\": \"custom\",\n" +
                "            \"factory\": \"org.apache.wayang.api.sql.calcite.jdbc.JdbcSchema$Factory\",\n" +
                "            \"operand\": {\n" +
                "                \"jdbcDriver\": \"org.postgresql.Driver\",\n" +
                "                \"jdbcUrl\": \"jdbc:postgresql://job:5432/job\",\n" +
                "                \"jdbcUser\": \"" + psqlUser + "\",\n" +
                "                \"jdbcPassword\": \"" + psqlPassword + "\"\n" +
                "            }\n" +
                "        }\n" +
                "    ]\n" +
                "}";

        config.load(ReflectionUtils.loadResource("wayang-api-python-defaults.properties"));
        config.setProperty("org.apache.calcite.sql.parser.parserTracing", "true");
        config.setProperty("wayang.calcite.model", calciteModel);
        config.setProperty("wayang.postgres.jdbc.url", "jdbc:postgresql://job:5432/job");
        config.setProperty("wayang.postgres.jdbc.user", psqlUser);
        config.setProperty("wayang.postgres.jdbc.password", psqlPassword);

        config.setProperty("spark.master", "spark://spark-cluster:7077");
        config.setProperty("spark.app.name", "JOB Query");
        config.setProperty("spark.rpc.message.maxSize", "2047");
        config.setProperty("spark.executor.memory", "42g");
        config.setProperty("spark.executor.cores", "8");
        config.setProperty("spark.executor.instances", "2");
        config.setProperty("spark.default.parallelism", "8");
        config.setProperty("spark.driver.maxResultSize", "16g");
        config.setProperty("spark.dynamicAllocation.enabled", "true");
        config.setProperty("wayang.flink.mode.run", "distribution");
        config.setProperty("wayang.flink.parallelism", "1");
        config.setProperty("wayang.flink.master", "flink-cluster");
        config.setProperty("wayang.flink.port", "7071");
        config.setProperty("wayang.flink.rest.client.max-content-length", "200MiB");
        config.setProperty("wayang.flink.collect.path", "file:///work/lsbo-paper/data/flink-data");
        config.setProperty("wayang.ml.experience.enabled", "false");
    }
```

---
# Acknowledgements
Thanks to Ryan Marcus for the implementation of ["Tree Convolution"](https://github.com/RyanMarcus/TreeConvolution) that was used in the training script.


