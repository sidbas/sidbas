import streamlit as st
import os

# File path (Change this to your actual file)
file_path = r"C:\path\to\your\file.txt"

# Check if file exists
if not os.path.exists(file_path):
    with open(file_path, "w") as f:
        f.write("This is a test file.")

# Notepad++ path (Change if Notepad++ is installed elsewhere)
notepadpp_path = r"C:\Program Files\Notepad++\notepad++.exe"

# Create a command to open file in Notepad++
open_command = f'"{notepadpp_path}" "{file_path}"'

# Streamlit UI
st.title("Open File in Notepad++")

# Button to open file
if st.button("Open in Notepad++"):
    os.system(open_command)
    st.success(f"Opening {file_path} in Notepad++")

# Show clickable link to file
st.markdown(f"[Open {file_path}](file:///{file_path})")