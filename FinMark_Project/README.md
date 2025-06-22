Project Documentation: FinMark Data Analytics Pipeline - Milestone 2

What was set up and why
   
Project Structure: I organized the project into dedicated folders (data, scripts, notebooks, output, config). This was done to maintain a clean and professional structure, separating raw data from processed data, and exploratory code (notebooks) from reusable code (scripts).

Dependency Management (requirements.txt): I created a requirements.txt file listing all necessary Python libraries (like Pandas, Prophet, and PyYAML). This ensures that the project environment is reproducible and can be easily set up by anyone with a single command (pip install -r requirements.txt).

Configuration File (config.yaml): I created a YAML configuration file to store file paths and model parameters. This was done to separate the configuration from the code, making the scripts more flexible and easier to maintain without having to edit the code itself.

Functional Module (run_forecasting.py): I developed a key feature from my proposal—sales forecasting—as a standalone Python script. This transforms the logic from the exploratory notebook into a reusable and automatable module, which is the foundational step for building an operational data pipeline with a tool like Airflow.

Challenges Encountered

Initial Data Structure: The initial raw data (marketing_summary.csv) was in a wide-format, where related regional sales data was spread horizontally across many columns (from col_6 to col_50). This format is difficult to analyze and contains many null values. I've normalized the data first, transforming it into a tidy dataset. After that, I identified the repeating pattern of three columns (Region, Regional Sales, Product ID). I programmatically iterated through these column groups, "stacking" them vertically into single, consistent columns: region, regional_sales, and product_id. During this process, any rows that were entirely empty were dropped, resulting in the clean, analysis-ready marketing_summary_cleaned.csv file that was used for the subsequent EDA and modeling.

Data Limitations for Segmentation: My Milestone 1 proposal included RFM-based customer segmentation. However, during the EDA, I discovered the dataset was aggregated and did not contain individual customer IDs, making RFM analysis impossible. I addressed this by pivoting to a region-based segmentation model as a proof-of-concept to demonstrate the methodology, while documenting that customer-level data would be required for the full feature.

Library Warnings: When running the script, Prophet produced a warning: Importing plotly failed. I identified this not as an error but as an informational message indicating an optional dependency for interactive plots was missing. Since my script's purpose was to save static .png files, this did not affect the successful outcome

What Worked and What Needs Refinement
   
What Worked:
The run_forecasting.py script executed successfully from the command line, proving the functional module is working as intended.
The script correctly read the configurations, processed the data, and generated the expected output files (sales_forecast.csv, sales_forecast_plot.png) in the output/ directory.
The Prophet model successfully identified a weekly seasonality in the sales data, providing a valuable and actionable insight.

What Needs Refinement (Next Steps):
Data Ingestion: The current script reads from a static CSV file. In a full pipeline, this would be the first step to automate, replacing the local file with a data source like a database or an API.
Model Evaluation: The region-based segmentation model yielded a 50% accuracy, which correctly indicates that the available features are not predictive. This highlights the need to ingest richer, more granular data to improve model performance, as planned in the proposed architecture.
Error Handling: The script currently follows a "happy path." A production-ready version would need more robust error handling (e.g., try-except blocks) and logging to manage potential issues during automated runs.
