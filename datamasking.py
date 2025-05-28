import pandas as pd
import re
import random
import string

# --------------------------
# CONFIGURATION
# --------------------------

input_file = 'data.xlsx'            # Input Excel file
output_file = 'masked_data.xlsx'    # Output Excel file
column_to_mask = 'Notes'            # Column name to mask in all sheets
use_seed = False                    # Set to True for reproducible random results
random_seed = 42                    # Optional: seed for reproducibility

# --------------------------
# RANDOM STRING GENERATOR
# --------------------------

def random_string(length):
    characters = string.ascii_letters + string.digits  # You can customize this
    return ''.join(random.choices(characters, k=length))

# --------------------------
# MASKING FUNCTIONS
# --------------------------

def mask_every_star_enclosed_word(text):
    return re.sub(r'\*[^*]+\*', lambda m: '*' + random_string(len(m.group(0)) - 2) + '*', text)

def mask_trailing_star_words(text):
    return re.sub(r'(?<=\*)[^\*]+\*', lambda m: random_string(len(m.group(0)) - 1) + '*', text)

def mask_all_starred_words(text):
    if pd.isna(text):
        return text
    text = str(text)
    text = mask_every_star_enclosed_word(text)
    text = mask_trailing_star_words(text)
    return text

# --------------------------
# PROCESS EXCEL FILE
# --------------------------

def process_excel(input_file, output_file, column_to_mask):
    if use_seed:
        random.seed(random_seed)

    # Load all sheets
    all_sheets = pd.read_excel(input_file, sheet_name=None)
    masked_sheets = {}

    for sheet_name, df in all_sheets.items():
        if column_to_mask in df.columns:
            df[column_to_mask] = df[column_to_mask].apply(mask_all_starred_words)
        masked_sheets[sheet_name] = df

    # Save to new Excel file
    with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
        for sheet_name, df in masked_sheets.items():
            df.to_excel(writer, sheet_name=sheet_name, index=False)

    print(f"\n✅ Masked Excel saved as: {output_file}\n")

# --------------------------
# MAIN
# --------------------------

if __name__ == '__main__':
    process_excel(input_file, output_file, column_to_mask)