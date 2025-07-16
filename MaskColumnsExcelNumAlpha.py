import pandas as pd
import re
import random
import string
import hashlib

# --- Utility Functions ---

def seed_from_value(val):
    val_str = str(val).strip()
    return int(hashlib.sha256(val_str.encode('utf-8')).hexdigest(), 16) % (10 ** 8)

def random_char_for_type(char_type, seed):
    random.seed(seed)
    if char_type == 'digit':
        return random.choice(string.digits)
    elif char_type == 'upper':
        return random.choice(string.ascii_uppercase)
    elif char_type == 'lower':
        return random.choice(string.ascii_lowercase)
    else:
        return char_type  # Should never happen

def mask_alphanumeric(val_str, seed_base):
    masked = []
    for i, ch in enumerate(val_str):
        seed = seed_base + i
        if ch.isdigit():
            masked.append(random_char_for_type('digit', seed))
        elif ch.isupper():
            masked.append(random_char_for_type('upper', seed))
        elif ch.islower():
            masked.append(random_char_for_type('lower', seed))
        else:
            masked.append(ch)  # Preserve special chars like - or _
    return ''.join(masked)

# --- Main Masking Logic ---

def mask_value(val):
    if pd.isnull(val):
        return val

    val_str = str(val).strip()
    seed = seed_from_value(val_str)

    # Handle numeric types
    if isinstance(val, (int, float)):
        if isinstance(val, float) and not val.is_integer():
            return mask_alphanumeric(str(val), seed)
        else:
            return mask_alphanumeric(str(int(val)), seed)

    return mask_alphanumeric(val_str, seed)


def mask_columns_across_sheets(file_path, columns_to_mask, output_file):
    all_sheets = pd.read_excel(file_path, sheet_name=None)
    updated_sheets = {}

    for sheet_name, df in all_sheets.items():
        print(f"\n📄 Processing sheet: {sheet_name}")
        df.columns = df.columns.str.strip()

        for col in columns_to_mask:
            if col in df.columns:
                print(f"🔐 Masking column: {col}")
                df[col] = df[col].apply(mask_value)
            else:
                print(f"⚠️ Column '{col}' not found in sheet '{sheet_name}'")

        updated_sheets[sheet_name] = df

    with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
        for sheet_name, df in updated_sheets.items():
            df.to_excel(writer, sheet_name=sheet_name, index=False)

    print(f"\n✅ Deterministic alphanumeric masking complete. Output saved to: {output_file}")
    

