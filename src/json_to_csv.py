import os
import glob
import json
import pandas as pd


def extract_text_fields(invoice_json: dict) -> dict:
    """
    For FATURA-style JSON: each key typically maps to a dict containing 'text' and 'bbox'.
    We keep only 'text' (blank if missing). For non-dict values, we stringify them.
    """
    flat = {}
    for key, value in invoice_json.items():
        if isinstance(value, dict):
            flat[key] = value.get("text", "")
        else:
            flat[key] = str(value)
    return flat


def template_folder_to_csv(
    template_folder: str,
    output_csv_path: str,
    pattern: str = "*.json",
    include_source_filename: bool = True
) -> None:
    """
    Reads all JSON files in a single template folder and writes one CSV.
    One row per JSON file (A1).
    """
    json_paths = sorted(glob.glob(os.path.join(template_folder, pattern)))
    if not json_paths:
        print(f"[SKIP] No JSON files found in: {template_folder}")
        return

    rows = []
    for path in json_paths:
        with open(path, "r", encoding="utf-8") as f:
            invoice = json.load(f)

        # A1 expectation: top-level must be a dict
        if not isinstance(invoice, dict):
            raise ValueError(f"Expected top-level dict (A1), got {type(invoice)} in: {path}")

        row = extract_text_fields(invoice)

        if include_source_filename:
            row["_source_file"] = os.path.basename(path)

        rows.append(row)

    df = pd.DataFrame(rows)

    # Optional: stable column ordering
    df = df.reindex(sorted(df.columns), axis=1)

    os.makedirs(os.path.dirname(output_csv_path), exist_ok=True)
    df.to_csv(output_csv_path, index=False, encoding="utf-8")
    print(f"[OK] {os.path.basename(template_folder)} → {len(df)} rows × {len(df.columns)} cols → {output_csv_path}")


def json_templates_to_csv(
    base_folder: str,
    output_folder: str,
    template_glob: str = "template*",
    json_pattern: str = "*.json",
    include_source_filename: bool = True
) -> None:
    """
    Auto-detects template subfolders under base_folder (e.g., template1, template2, ...)
    and writes one CSV per template folder into output_folder.
    """
    os.makedirs(output_folder, exist_ok=True)

    template_dirs = sorted(
        d for d in glob.glob(os.path.join(base_folder, template_glob))
        if os.path.isdir(d)
    )

    if not template_dirs:
        raise FileNotFoundError(
            f"No template folders matching '{template_glob}' found under: {base_folder}"
        )

    for template_path in template_dirs:
        template_name = os.path.basename(template_path)
        output_csv_path = os.path.join(output_folder, f"{template_name}.csv")

        template_folder_to_csv(
            template_folder=template_path,
            output_csv_path=output_csv_path,
            pattern=json_pattern,
            include_source_filename=include_source_filename
        )


# ---------------
# Example usage:
# ---------------
if __name__ == "__main__":
    json_templates_to_csv(
        base_folder="../data/json_files",     # contains template1, template2, ...
        output_folder="../data/csv_files",   # will contain template1.csv, template2.csv, ...
        template_glob="template*",              # change if your folder naming differs
        json_pattern="*.json",
        include_source_filename=True
    )