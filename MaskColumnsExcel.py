import pandas as pd
import re
import random
import string
import hashlib

# --- Helpers ---

def seed_from_value(val):
    """Generate a deterministic seed from the input value (string-safe)."""
    val_str = str(val).strip()
    return int(hashlib.sha256(val_str.encode('utf-8')).hexdigest(), 16) % (10 ** 8)

def random_digits(length, seed):
    """Generate random digits with a given seed and length."""
    random.seed(seed)
    return ''.join(random.choice(string.digits) for _ in range(length))

def mask_digits_only(s, seed_base):
    """Mask only digits in the string, using seeded randomness."""
    result = []
    i = 0
    for ch in s:
        if ch.isdigit():
            seed = seed_base + i
            result.append(random_digits(1, seed))
            i += 1
        else:
            result.append(ch)
    return ''.join(result)

def is_numeric_value(val):
    return isinstance(val, (int, float))

def is_numeric_string(val):
    val = str(val).strip()
    return val.replace('.', '', 1).replace('-', '', 1).isdigit()
    
    
    def mask_value(val):
    if pd.isnull(val):
        return val

    val_str = str(val).strip()
    seed = seed_from_value(val_str)

    # Handle numeric types directly
    if is_numeric_value(val):
        if isinstance(val, float) and not val.is_integer():
            return mask_digits_only(str(val), seed)
        else:
            return mask_digits_only(str(int(val)), seed)

    # If it's a string that looks numeric
    if is_numeric_string(val_str):
        return mask_digits_only(val_str, seed)

    # Mixed alphanumeric: mask only digit groups inside the string
    def repl(m):
        return random_digits(len(m.group()), seed + m.start())

    return re.sub(r'\d+', repl, val_str)
    
    
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

    print(f"\n✅ Deterministic masking complete. Output saved to: {output_file}")
    
    


    