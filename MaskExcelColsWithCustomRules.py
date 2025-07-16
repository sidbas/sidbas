def mask_keep_last_4_digits(val):
    val_str = str(val).strip()
    if len(val_str) <= 4:
        return val_str
    masked_part = ''.join('X' for _ in range(len(val_str) - 4))
    return masked_part + val_str[-4:]

def mask_ssn(val):
    """Mask SSN like XXX-XX-1234"""
    val_str = str(val).strip()
    return re.sub(r'\d{3}-\d{2}-(\d{4})', r'XXX-XX-\1', val_str)

def mask_email(val):
    """Mask email username part"""
    val_str = str(val).strip()
    parts = val_str.split('@')
    if len(parts) != 2:
        return val_str  # Not a valid email
    username, domain = parts
    masked_username = ''.join(random.choice(string.ascii_letters) for _ in range(len(username)))
    return f"{masked_username}@{domain}"
    

custom_column_rules = {
    "PhoneNumber": mask_keep_last_4_digits,
    "SSN": mask_ssn,
    "Email": mask_email,
    # Add more as needed
}

def mask_value(val, column_name=None):
    if pd.isnull(val):
        return val

    # Check if column has a custom rule
    if column_name in custom_column_rules:
        return custom_column_rules[column_name](val)

    # Fallback to deterministic alphanumeric masking
    val_str = str(val).strip()
    seed = seed_from_value(val_str)

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
                df[col] = df[col].apply(lambda val: mask_value(val, col))
            else:
                print(f"⚠️ Column '{col}' not found in sheet '{sheet_name}'")

        updated_sheets[sheet_name] = df

    with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
        for sheet_name, df in updated_sheets.items():
            df.to_excel(writer, sheet_name=sheet_name, index=False)

    print(f"\n✅ Masking complete with custom rules. Output saved to: {output_file}")




