import streamlit as st
import pandas as pd
import os

def process_file(file):
    """Example processing function: converts all text in a CSV file to uppercase."""
    df = pd.read_csv(file)
    df_processed = df.applymap(lambda x: x.upper() if isinstance(x, str) else x)
    return df_processed

def main():
    st.title("Local File Processor")
    
    uploaded_file = st.file_uploader("Upload a CSV file", type=["csv"])
    
    if uploaded_file is not None:
        st.write("### Original File Preview:")
        df = pd.read_csv(uploaded_file)
        st.dataframe(df)
        
        # Process file
        df_processed = process_file(uploaded_file)
        st.write("### Processed File Preview:")
        st.dataframe(df_processed)
        
        # Save processed file
        processed_filename = "processed_file.csv"
        df_processed.to_csv(processed_filename, index=False)
        
        # Provide download link
        with open(processed_filename, "rb") as file:
            st.download_button(label="Download Processed File",
                               data=file,
                               file_name=processed_filename,
                               mime="text/csv")
        
        # Cleanup saved file
        os.remove(processed_filename)

if __name__ == "__main__":
    main()
