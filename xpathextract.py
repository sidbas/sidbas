import streamlit as st
from st_aggrid import AgGrid, GridOptionsBuilder
import pandas as pd
import xml.etree.ElementTree as ET
import io
import matplotlib.pyplot as plt

# Helper to parse XSD and return DataFrame
def parse_xsd_to_df(xsd_file):
    tree = ET.parse(xsd_file)
    root = tree.getroot()

    data = []
    for element in root.iter():
        data.append({"Tag": element.tag, "Attributes": element.attrib})

    df = pd.DataFrame(data)
    return df

# Initialize session state for file tracking
if "uploaded_filename" not in st.session_state:
    st.session_state.uploaded_filename = None
if "original_df" not in st.session_state:
    st.session_state.original_df = None

# File uploader
uploaded_file = st.file_uploader("Upload XSD File", type=["xsd"])

if uploaded_file:
    # Check if it's a new file
    if uploaded_file.name != st.session_state.uploaded_filename:
        st.session_state.uploaded_filename = uploaded_file.name
        st.session_state.original_df = parse_xsd_to_df(uploaded_file)

        # Generate graph only for new file
        st.success("Graph generated for new file!")
        fig, ax = plt.subplots()
        st.session_state.original_df["Tag"].value_counts().plot(kind="bar", ax=ax)
        st.pyplot(fig)
    else:
        st.info("Same file uploaded again, graph not regenerated.")

# Display AG Grid only if data is available
if st.session_state.original_df is not None:
    st.subheader("Data Table (filterable)")
    gb = GridOptionsBuilder.from_dataframe(st.session_state.original_df)
    gb.configure_default_column(filter=True, sortable=True, resizable=True)
    grid_options = gb.build()

    grid_response = AgGrid(
        st.session_state.original_df,
        gridOptions=grid_options,
        enable_enterprise_modules=True,
        update_mode="MODEL_CHANGED",  # AGGrid filter interaction doesn't trigger new computation
        height=300,
        fit_columns_on_grid_load=True
    )