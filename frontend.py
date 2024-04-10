import streamlit as st 
from server import Server 
import json
import requests
import base64

def main():
    st.title("Download Top Records")
    num_records = st.number_input("Enter the number of records to download:", min_value=1, value=1000000)

    response = requests.get(f"http://localhost:5000/records?{num_records}")

    # submit button
    if st.button("Submit"): 
        if response.status_code == 200:
            # Get the records from the response
            records = response.json()
            print('fetched records from server')

            # Convert records to a JSON string
            json_data = str(records)

            # Encode the JSON string as base64
            b64_json = base64.b64encode(json_data.encode()).decode()

            # Create a download link for the JSON data
            download_link = f'<a href="data:file/json;base64,{b64_json}" download="records.json">Download JSON File</a>'
            st.markdown(download_link, unsafe_allow_html=True)
        else:
            st.error(f"Error fetching records: {response.status_code} - {response.text}")
if __name__ == '__main__':
    main()