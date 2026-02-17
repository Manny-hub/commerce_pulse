import json 

def read_jsonl(file_path):
    data = []
    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            # line still has the \n at the end, but json.loads handles it
            if line.strip(): 
                data.append(json.loads(line))
    return data