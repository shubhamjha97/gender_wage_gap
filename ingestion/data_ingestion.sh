set -euo pipefail
hdfs dfs -mkdir bdad_project_data
hdfs dfs -mkdir bdad_project_data/output
hdfs dfs -put ./data/* bdad_project_data/
