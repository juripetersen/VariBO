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

**Parameters**

--model vae (default), pairwise, cost
<br>
--retrain 'path/to/data', (default) ' '
<br>
--name 'name-of-themodel', (default) ' '
<br>
--lr '[1e-6, 1e-3]', (default) '[1e-6, 0.1]'
<br>
--epochs 10, (default) 100
<br>
--trials 15, (default) 25

Example use model.py --model <model_name> --retrain <retrain>
--model-path <model_path>

```bash
python main.py --model pairwise --retrain src/Data/pairwise-encodings.txt --model-path src/Data/vae.onnx --name model-name
```

---
Thanks to Ryan Marcus for the implementation of ["Tree Convolution"](https://github.com/RyanMarcus/TreeConvolution) that was used in the training script.


