import json

data = []
fields = [
    "full_name", "occupation", "headline", "country_full_name", "city", "state",
    "experiences", "education", "languages", "certifications", "connections", "skills", "industry"
]

invalid_lines = []

def fix_missing_comma(line, error_message):
    try:
        position = int(error_message.split("char")[-1].strip(")").strip())
        fixed_line = line[:position] + ',' + line[position:]
        return fixed_line
    except Exception as e:
        print(f"Failed to fix missing comma: {e}")
        return None

def fix_missing_quotes(line):
    try:
        if line.count('"') % 2 != 0:
            line += '"'
        return line
    except Exception as e:
        print(f"Failed to fix quotes: {e}")
        return None

try:
    with open('linked_data.json', 'r', encoding='utf-8') as json_file:
        try:
            data = json.load(json_file)
        except json.JSONDecodeError:
            print("Error reading existing data; starting with an empty list.")
            data = []
except FileNotFoundError:
    print("No existing file found; starting fresh.")
    data = []

with open('singapore_profiles.txt', 'r', encoding='utf-8') as file:
    for i, line in enumerate(file, start=1):
        try:
            res = json.loads(line.strip().replace("'", '"'))  # Attempt to parse JSON
            filtered_data = {key: res[key] for key in fields if key in res}
            if filtered_data:
                data.append(filtered_data)
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON on line {i}: {e}")
            fixed_line = fix_missing_comma(line.strip(), str(e))
            if not fixed_line:  # Try fixing quotes if comma fix fails
                fixed_line = fix_missing_quotes(line.strip())
            if fixed_line:
                try:
                    res = json.loads(fixed_line)
                    filtered_data = {key: res[key] for key in fields if key in res}
                    if filtered_data:
                        data.append(filtered_data)
                except json.JSONDecodeError as retry_error:
                    print(f"Retry failed for line {i}: {retry_error}")
                    invalid_lines.append((i, line.strip()))
            else:
                invalid_lines.append((i, line.strip()))
        except Exception as e:
            print(f"Unexpected error on line {i}: {e}")
            invalid_lines.append((i, line.strip()))

with open('linked_data.json', 'w', encoding='utf-8') as json_file:
    json.dump(data, json_file, ensure_ascii=False, indent=4)

with open('invalid_lines.txt', 'w', encoding='utf-8') as invalid_file:
    for line in invalid_lines:
        invalid_file.write(f"Line {line[0]}: {line[1]}\n")

print(f"Processing complete. Total valid records: {len(data)}")
print(f"Invalid lines logged to 'invalid_lines.txt'.")
