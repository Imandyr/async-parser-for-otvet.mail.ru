# how to read jsonl file
import json


# read file as list of lines
jsonl_file = open("all_mail_parse.jsonl", "r", encoding="utf-8")
json_list = list(jsonl_file)

# decode each lines as json and convert to dict
for json_line in json_list:
    result = json.loads(json_line)
    result = dict(result)
    print(result)
