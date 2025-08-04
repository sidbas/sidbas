import streamlit as st
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode
import pandas as pd

# 🔹 Sample DataFrame — replace this with your Oracle query result
df_raw = pd.DataFrame({
    "Map_Id": ["00001", "00002", "00003"],
    "Preview": ["If ServiceId = 'I'", "If ServiceId = 'O'", "Lookup on Tbl2"],
    "Pseudocode": [
        "WHEN Db1.Tbl1.ServiceId IN ('I')\nTHEN Db1.Tbl1.ExecDate",
        "WHEN Db1.Tbl1.ServiceId IN ('O')\nTHEN Db1.Tbl1.InstrDate",
        "JOIN Db1.Tbl2 ON keys\nFILTER Clr_Type = 'X'"
    ]
})
df_raw["Map_Id"] = df_raw["Map_Id"].astype(str)

# 🔹 Configure AgGrid
gb = GridOptionsBuilder.from_dataframe(df_raw[["Map_Id", "Preview"]])
gb.configure_column("Preview", wrapText=True, autoHeight=True)
gb.configure_selection(selection_mode="single", use_checkbox=True)
grid_options = gb.build()

# 🔹 Display Grid
response = AgGrid(
    df_raw[["Map_Id", "Preview"]],
    gridOptions=grid_options,
    update_mode=GridUpdateMode.SELECTION_CHANGED,
    fit_columns_on_grid_load=True,
    height=300
)

selected = response["selected_rows"]

# 🔹 Show Modal Popup if a row is selected
if selected and isinstance(selected[0], dict):
    selected_row = selected[0]
    selected_map_id = selected_row["Map_Id"]

    # Match full row from original df
    full_row = df_raw[df_raw["Map_Id"] == selected_map_id].iloc[0]
    full_code = full_row["Pseudocode"]

    # 📌 Optional: Only show modal when a button is clicked
    if st.button(f"📎 View Full Pseudocode for {selected_map_id}"):
        with st.modal(f"📄 Pseudocode - Map ID {selected_map_id}", key=f"popup_{selected_map_id}"):
            st.markdown("#### Functional Pseudocode")
            st.code(full_code, language="text")
