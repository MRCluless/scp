import streamlit as st
import boto3
import pandas as pd
import time
from dotenv import load_dotenv

load_dotenv()

REGION = "us-east-1"
ATHENA_S3_OUTPUT = "s3://25140990-scp-f1-data/athena_results/" 

def run_athena_query(query):
    """Executes a SQL query in Athena using boto3 and returns a Pandas DataFrame."""
    client = boto3.client('athena', region_name=REGION)

    response = client.start_query_execution(
        QueryString=query,
        ResultConfiguration={'OutputLocation': ATHENA_S3_OUTPUT}
    )
    query_id = response['QueryExecutionId']
    
    with st.spinner("Querying AWS Athena..."):
        while True:
            status = client.get_query_execution(QueryExecutionId=query_id)
            state = status['QueryExecution']['Status']['State']
            
            if state == 'SUCCEEDED':
                break
            elif state in ['FAILED', 'CANCELLED']:
                st.error(f"Athena Query Failed: {status['QueryExecution']['Status']['StateChangeReason']}")
                return pd.DataFrame()
            time.sleep(1)
        
    results = client.get_query_results(QueryExecutionId=query_id)
    
    columns = [col['Label'] for col in results['ResultSet']['ResultSetMetadata']['ColumnInfo']]
    rows = []
    
    for row in results['ResultSet']['Rows'][1:]: 
        rows.append([data.get('VarCharValue', '') for data in row['Data']])
        
    df = pd.DataFrame(rows, columns=columns)
    
    numeric_cols = ['Live_Speed', 'Historical_Avg_Speed', 'Speed_Delta']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
            
    return df

st.set_page_config(page_title="F1 Live Telemetry", layout="wide")
st.title("🏎️ F1 Real-Time Analytics Dashboard")
st.markdown("Live Flink speed layer metrics merged with historical Spark baselines.")

f1_query = """
    SELECT 
        s.Track,
        s.Driver,
        s.AvgSpeed AS Live_Speed,
        b.historical_avg_speed AS Historical_Avg_Speed,
        (s.AvgSpeed - b.historical_avg_speed) AS Speed_Delta
    FROM 
        speed_database.speed_layer AS s
    JOIN 
        batch_database.batch_output AS b
    ON 
        s.Track = b.Track 
        AND s.Driver = b.Driver
    LIMIT 20;
"""

if st.button("Refresh Telemetry"):
    st.rerun()

df = run_athena_query(f1_query)

if not df.empty:
    st.subheader("Unified Telemetry Feed")
    st.dataframe(df, use_container_width=True)
    
    st.divider()
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Live vs Historical Speed")
        chart_data = df.set_index('Driver')[['Live_Speed', 'Historical_Avg_Speed']]
        st.bar_chart(chart_data)
        
    with col2:
        st.subheader("Speed Delta (Performance Gap)")
        delta_data = df.set_index('Driver')[['Speed_Delta']]
        st.bar_chart(delta_data, color="#ff4b4b")
else:
    st.warning("No data returned. Ensure your Flink producer is running and writing to S3.")