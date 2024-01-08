import pandas as pd
import subprocess
import time
import json
import dotenv
import streamlit as st

dotenv.load_dotenv("/Users/oreotamish/Desktop/Tamish/bazaar-lambda/.env")
aws_role = "arn:aws:iam::x:role/redshift-cluster-role"

data = pd.DataFrame(columns=["Mongo Column", "Redshift Column"], data=[["", ""]] * 1)

####################################################### STREAMLIT
db_name = st.text_input("Database Name", placeholder="Database")
collection_name = st.text_input("Collection Name", placeholder="Collection")
####################################################################


def main(column_dict):
    start_time = time.time()
    go_code_path = "../server/cmd/main.go"
    column_dict = json.dumps(column_dict)
    command = "go run {} {} {} {}".format(
        go_code_path, db_name, collection_name, column_dict
    )
    st.write(command)

    p1 = subprocess.Popen(
        command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )

    p1.wait()

    st.write("Data written!")
    st.write("Serialized!")

    with open("/Users/oreotamish/Desktop/Coding/table-org/server/cmd/FINAL.json") as f:
        data = json.load(f)

    df = pd.DataFrame(data)
    df.to_parquet(
        "./parquet/FINAL.parquet",
        index=False,
    )

    end_time = time.time()
    elapsed_time_minutes = (end_time - start_time) / 60
    st.write("Time elapsed in minutes: ", elapsed_time_minutes)
    st.write(len(col_data))


####################################################### STREAMLIT
col_data = st.data_editor(data, num_rows="dynamic", width=1000)

st.write("OR")
value = st.text_area(
    'Input JSON { "Mongo Column Name" : "Redshift Column Name"} :', height=175
)
if st.button("Done"):
    if value:
        column_dict = json.loads(value)
        st.write(column_dict)
        main(column_dict)
    else:
        df = pd.DataFrame(col_data)
        column_dict = {df.iloc[i, 0]: df.iloc[i, 1] for i in range(len(df))}
        st.write(column_dict)
        main(column_dict)
