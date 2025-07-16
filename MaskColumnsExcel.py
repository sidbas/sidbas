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


columns_to_mask = ["PhoneNumber", "SSN", "Email", "CardNumber", "UserID", "TaxID"]

mask_columns_across_sheets(
    file_path="input_data.xlsx",
    columns_to_mask=columns_to_mask,
    output_file="masked_output.xlsx"
)


#
Perfect — if you want to encrypt instead of mask, we can use AES encryption to secure the data reversibly (unlike masking). That means:
	•	🔐 You can later decrypt the values (with the same key)
	•	💡 Useful for secure testing, reversible transformations, or audit trails
# pip install cryptography

from cryptography.hazmat.primitives.ciphers.aead import AESGCM
import os
import base64

# Generate a 256-bit key securely (do this once and save safely)
# Use os.urandom(32) and store securely in env/file
ENCRYPTION_KEY = b"your-32-byte-long-key.............."  # Must be 32 bytes

def encrypt_value(value, key=ENCRYPTION_KEY):
    if pd.isnull(value):
        return value
    val_str = str(value).encode('utf-8')
    nonce = os.urandom(12)  # AESGCM requires 96-bit nonce
    aesgcm = AESGCM(key)
    encrypted = aesgcm.encrypt(nonce, val_str, None)
    combined = nonce + encrypted
    return base64.b64encode(combined).decode('utf-8')

def decrypt_value(encoded_value, key=ENCRYPTION_KEY):
    if pd.isnull(encoded_value):
        return encoded_value
    decoded = base64.b64decode(encoded_value)
    nonce, ciphertext = decoded[:12], decoded[12:]
    aesgcm = AESGCM(key)
    decrypted = aesgcm.decrypt(nonce, ciphertext, None)
    return decrypted.decode('utf-8')
    

custom_column_rules = {
    "PhoneNumber": mask_keep_last_4_digits,
    "SSN": mask_ssn,
    "Email": mask_email,
    "TaxID": lambda v: mask_regex_format(v, r'\d{2}-\d{7}', lambda m: 'XX-XXXXXXX'),
    "CardNumber": lambda v: mask_regex_format(v, r'(\d{4})-(\d{4})-(\d{4})-(\d{4})', lambda m: f"{'****'}-{'****'}-{'****'}-{m.group(4)}"),
    
    # 🔐 Encrypt these:
    "SecretNote": lambda v: encrypt_value(v),
    "AccessKey": lambda v: encrypt_value(v),
}


def mask_value(val, column_name=None):
    if pd.isnull(val):
        return val

    # Use custom column rule if defined
    if column_name in custom_column_rules:
        return custom_column_rules[column_name](val)

    # Fallback: generic deterministic masking
    val_str = str(val).strip()
    seed = seed_from_value(val_str)
    return mask_alphanumeric(val_str, seed)


def decrypt_column(df, column_name):
    df[column_name] = df[column_name].apply(decrypt_value)


columns_to_mask_or_encrypt = [
    "PhoneNumber", "SSN", "Email", "CardNumber", "UserID", "TaxID", "AccessKey", "SecretNote"
]

mask_columns_across_sheets(
    file_path="input.xlsx",
    columns_to_mask=columns_to_mask_or_encrypt,
    output_file="output_encrypted.xlsx"
)



    


    