# ELT Pipeline with Airflow, MinIO, DuckDB and Streamlit

This project demonstrates an end-to-end ELT pipeline using the following tools:

- **Apache Airflow** – for orchestration
- **MinIO** – as an object storage (S3-compatible)
- **DuckDB** – as a fast local analytical database
- **Streamlit** – for interactive dashboards and data exploration
- **Python** – for writing scripts that extract, load and transform data

---

## 🔁 Project Workflow

```mermaid
graph TD
    A[CoinCap API] --> B[Airflow DAG: extract & save to MinIO]
    B --> C[Airflow DAG: load to DuckDB]
    C --> D[Streamlit Dashboard: visualize data]
