# Data Engineering Projects

Batch, streaming and lakehouse pipelines built with Python, SQL, Spark and open-source tools. Each project has its own README with an architecture diagram, how to run it, and tests.

| Project | What it does | Stack | Status |
| --- | --- | --- | --- |
| [Directory Sync and Reconciliation](data-engineering/directory-sync-reconciliation) | Nightly pipeline that keeps an HR system and an identity directory in agreement and reports every mismatched account | Python, PostgreSQL, SQL, Docker, Kubernetes CronJob | Tested on a 30-day synthetic run with 6,000 employees: 99.58–99.93% daily accuracy |
| [Real-Time API Monitoring](data-engineering/api-monitoring-stream%202) | Streaming pipeline that flags latency spikes, error bursts and regional outages from microservice logs | Kafka (Redpanda), Spark Structured Streaming, PostgreSQL, Grafana | Simulated run: 3 of 3 injected incidents detected within 60 s |
| [Mine Safety Lakehouse](data-engineering/mine-safety-lakehouse) | Weekly bronze/silver/gold pipeline on Databricks for U.S. MSHA mine safety data, with incremental loads and quality checks | PySpark, Delta Lake, Databricks, Unity Catalog | Tested on synthetic batches; real-data run pending |
| [Hospital Quality Warehouse](data-engineering/hospital-quality-warehouse) | Star-schema warehouse tracking changes in CMS hospital star ratings across releases | Airflow, dbt Core, PostgreSQL | 16 dbt tests passing on synthetic releases; real-data run pending |
| [Skin Lesion Diffusion Augmentation](data-engineering/skin-lesion-diffusion) | Tests whether Stable Diffusion images help a classifier recognize rare skin lesion types (learning project, not a diagnostic tool) | PyTorch, Hugging Face Diffusers, LoRA | Experiment set up; results pending |

## About me

I'm Flora Phiri, a data and AI engineer completing an M.S. in Information Systems Technology at George Washington University (December 2026). Before that I spent over two years as a Systems Integration Specialist at Zimbabwe Platinum Mines, building ETL and reconciliation pipelines across 100+ Azure subscriptions.

[LinkedIn](https://www.linkedin.com/in/flora-phiri/) · [More projects](https://github.com/FloraP277)
